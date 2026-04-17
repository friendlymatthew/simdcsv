use std::sync::Arc;

use crate::classify::{
    self, HIGH_NIBBLES, LOW_NIBBLES, NEWLINE, QUOTES, build_eq_bitset, classify_four_lanes,
};
use crate::monoid::ClassifyResult;
use crate::simd::{u8x16, u8x16x4};

/// A partition streams bytes and produces newline bitsets
/// for both possible initial quote states
///
/// To keep memory bounded, only bitsets for the first `window` bytes are
/// retained. The rest of the data is streamed through just for parity counting.
///
/// This way, memory is O(window).
#[derive(Debug)]
pub struct PartitionClassifier {
    newlines_outside: Vec<u64>,
    newlines_inside: Vec<u64>,
    quote_parity: bool,
    window: usize,
    bytes_classified: usize,
    remainder: Vec<u8>,
}

impl PartitionClassifier {
    pub fn new(window: usize) -> Self {
        let num_words = window / 64 + 1;

        Self {
            newlines_outside: Vec::with_capacity(num_words),
            newlines_inside: Vec::with_capacity(num_words),
            quote_parity: false,
            window,
            bytes_classified: 0,
            remainder: Vec::new(),
        }
    }

    pub fn ingest(&mut self, bytes: &[u8]) {
        // if we have leftover bytes from last ingest, do it in this cycle
        let offset = if self.remainder.is_empty() {
            0
        } else {
            let needed = 64 - self.remainder.len();
            if bytes.len() < needed {
                self.remainder.extend_from_slice(bytes);
                return;
            }

            self.remainder.extend_from_slice(&bytes[..needed]);
            let block: [u8; 64] = self.remainder.clone().try_into().unwrap();
            self.remainder.clear();
            self.classify_block(&block);

            needed
        };

        let remaining = &bytes[offset..];
        let complete = remaining.len() / 64 * 64;
        for chunk in remaining[..complete].chunks_exact(64) {
            self.classify_block(chunk.try_into().expect("unreachable"));
        }

        if complete < remaining.len() {
            self.remainder.extend_from_slice(&remaining[complete..]);
        }
    }

    pub fn finish(mut self) -> ClassifyResult {
        if !self.remainder.is_empty() {
            let mut padded = [0u8; 64];
            padded[..self.remainder.len()].copy_from_slice(&self.remainder);
            self.classify_block(&padded);
        }

        ClassifyResult {
            newlines_outside: self.newlines_outside,
            newlines_inside: self.newlines_inside,
            quote_parity: self.quote_parity,
        }
    }

    fn classify_block(&mut self, block: &[u8; 64]) {
        if self.bytes_classified < self.window {
            self.classify_block_full(block);
        } else {
            self.count_quotes_fast(block);
        }

        self.bytes_classified += 64;
    }

    #[inline(always)]
    fn classify_block_full(&mut self, block: &[u8; 64]) {
        let newline_bc = u8x16::broadcast(NEWLINE);
        let quote_bc = u8x16::broadcast(QUOTES);
        let table = u8x16x4::from_slice_unchecked(&classify::BYTE_TABLE);
        let bitmask1 = u8x16::broadcast(0x55);
        let bitmask2 = u8x16::broadcast(0x33);

        let (classified) = classify_four_lanes(block, table);

        let raw_newline = build_eq_bitset(classified, newline_bc, bitmask1, bitmask2);
        let quote = build_eq_bitset(classified, quote_bc, bitmask1, bitmask2);

        let inside = unsafe { std::arch::aarch64::vmull_p64(quote, 0xFFFFFFFFFFFFFFFF_u64) } as u64;

        let (outside_mask, inside_mask) = if self.quote_parity {
            (inside, !inside)
        } else {
            (!inside, inside)
        };

        self.newlines_outside.push(raw_newline & outside_mask);
        self.newlines_inside.push(raw_newline & inside_mask);
        self.quote_parity ^= (quote.count_ones() & 1) != 0;
    }

    #[inline(always)]
    fn count_quotes_fast(&mut self, block: &[u8; 64]) {
        let q = u8x16::broadcast(b'"');
        let parity = u8x16::from_slice_unchecked(&block[..16]).eq(q).count_ones()
            ^ u8x16::from_slice_unchecked(&block[16..32])
                .eq(q)
                .count_ones()
            ^ u8x16::from_slice_unchecked(&block[32..48])
                .eq(q)
                .count_ones()
            ^ u8x16::from_slice_unchecked(&block[48..]).eq(q).count_ones();

        self.quote_parity ^= (parity & 1) != 0;
    }
}

#[derive(Debug)]
pub struct RecordAlignedRange {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, Copy)]
enum RecordStart {
    Unknown,
    NoNewline,
    Found(usize),
}

impl RecordStart {
    const fn is_unknown(self) -> bool {
        matches!(self, Self::Unknown)
    }
}

// note: partition `i` can be resolved as soon as partition `0..=i+1` submitted their classify results
#[derive(Debug)]
pub struct PartitionResolver {
    partition_boundaries: Arc<[usize]>,
    num_partitions: usize,
    partition_results: Vec<Option<ClassifyResult>>,
    prefix_parities: Vec<Option<bool>>,
    record_starts: Vec<RecordStart>,
    already_computed: Vec<bool>,
}

impl PartitionResolver {
    pub fn new(boundaries: Arc<[usize]>) -> Self {
        let num_partitions = boundaries.len() - 1;

        let mut prefix_parities = vec![None; num_partitions + 1];
        prefix_parities[0] = Some(false);

        let mut record_starts = vec![RecordStart::Unknown; num_partitions];
        record_starts[0] = RecordStart::Found(0);

        Self {
            partition_boundaries: boundaries,
            num_partitions,
            partition_results: (0..num_partitions).map(|_| None).collect(),
            prefix_parities,
            record_starts,
            already_computed: vec![false; num_partitions],
        }
    }

    pub fn get_partition_range_by_id(&self, partition_id: usize) -> (usize, usize) {
        (
            self.partition_boundaries[partition_id],
            self.partition_boundaries[partition_id + 1],
        )
    }

    pub fn submit(
        &mut self,
        partition_id: usize,
        result: ClassifyResult,
    ) -> Vec<(usize, RecordAlignedRange)> {
        self.partition_results[partition_id] = Some(result);

        if partition_id > 0
            && self.record_starts[partition_id].is_unknown()
            && let Some(carry) = self.prefix_parities[partition_id]
        {
            let result = self.partition_results[partition_id]
                .as_ref()
                .expect("above");

            let newlines = if carry {
                &result.newlines_inside
            } else {
                &result.newlines_outside
            };

            let start_offset = self.partition_boundaries[partition_id];
            self.record_starts[partition_id] = find_first_set_bit(newlines)
                .map_or(RecordStart::NoNewline, |pos| {
                    RecordStart::Found(start_offset + pos + 1)
                });
        }

        let propagated_up_to = self.propagate_from(partition_id);

        let lo = partition_id.saturating_sub(1);
        (lo..=propagated_up_to)
            .filter_map(|j| {
                self.try_compute_record_aligned_range_by_id(j)
                    .map(|split| (j, split))
            })
            .collect()
    }

    // my reasonin ghere is submitting one partition can unlock the parity for partitions _after_ it
    // might be a premature optimization but, there's no 100% sequential consistent ordering guarantee
    fn propagate_from(&mut self, from_partition_id: usize) -> usize {
        let mut last = from_partition_id;

        for partition_id in from_partition_id..self.num_partitions {
            let next_partition_id = partition_id + 1;

            if self.prefix_parities[next_partition_id].is_none() {
                match (
                    self.prefix_parities[partition_id],
                    self.partition_results[partition_id].as_ref(),
                ) {
                    (Some(prev), Some(result)) => {
                        self.prefix_parities[next_partition_id] = Some(prev ^ result.quote_parity);
                    }
                    _ => break,
                }
            }

            // Compute record start for partition next_i (if not already done)
            if next_partition_id < self.num_partitions
                && self.record_starts[next_partition_id].is_unknown()
                && let Some(carry) = self.prefix_parities[next_partition_id]
                && let Some(result) = self.partition_results[next_partition_id].as_ref()
            {
                let newlines = if carry {
                    &result.newlines_inside
                } else {
                    &result.newlines_outside
                };

                let partition_start = self.partition_boundaries[next_partition_id];

                self.record_starts[next_partition_id] = find_first_set_bit(newlines)
                    .map_or(RecordStart::NoNewline, |pos| {
                        RecordStart::Found(partition_start + pos + 1)
                    });
            }

            last = next_partition_id.min(self.num_partitions - 1);
        }

        last
    }

    fn try_compute_record_aligned_range_by_id(
        &mut self,
        partition_id: usize,
    ) -> Option<RecordAlignedRange> {
        if self.already_computed[partition_id] {
            return None;
        }

        let start = match self.record_starts[partition_id] {
            RecordStart::Found(s) => s,
            RecordStart::NoNewline => {
                self.already_computed[partition_id] = true;
                return None;
            }
            RecordStart::Unknown => return None,
        };

        let end = if partition_id == self.num_partitions - 1 {
            *self.partition_boundaries.last().unwrap()
        } else {
            let RecordStart::Found(end) = self.record_starts[partition_id + 1] else {
                return None;
            };

            end
        };

        self.already_computed[partition_id] = true;

        Some(RecordAlignedRange { start, end })
    }
}

pub fn find_first_set_bit(bitsets: &[u64]) -> Option<usize> {
    bitsets
        .iter()
        .enumerate()
        .find_map(|(i, &bits)| (bits != 0).then_some(i * 64 + bits.trailing_zeros() as usize))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monoid::Monoid;

    const TEST_WINDOW: usize = 1024;

    #[test]
    fn test_find_first_set_bit() {
        assert_eq!(find_first_set_bit(&[0, 0, 0]), None);
        assert_eq!(find_first_set_bit(&[0b1000, 0, 0]), Some(3));
        assert_eq!(find_first_set_bit(&[0, 0b10, 0]), Some(65));
    }

    fn classify(data: &[u8]) -> ClassifyResult {
        let mut s = PartitionClassifier::new(TEST_WINDOW);
        s.ingest(data);
        s.finish()
    }

    fn resolve_in_order(chunks: &[&[u8; 64]]) -> Vec<RecordAlignedRange> {
        let n = chunks.len();
        let boundaries: Arc<[usize]> = (0..=n).map(|i| i * 64).collect::<Vec<_>>().into();

        let mut resolver = PartitionResolver::new(boundaries);
        let mut splits = Vec::new();

        for (i, chunk) in chunks.iter().enumerate() {
            splits.extend(resolver.submit(i, classify(*chunk)));
        }

        splits.sort_by_key(|(idx, _)| *idx);
        splits.into_iter().map(|(_, s)| s).collect()
    }

    fn resolve_in_reverse(chunks: &[&[u8; 64]]) -> Vec<RecordAlignedRange> {
        let n = chunks.len();
        let boundaries: Arc<[usize]> = (0..=n).map(|i| i * 64).collect::<Vec<_>>().into();

        let mut resolver = PartitionResolver::new(boundaries);
        let mut splits = Vec::new();

        for i in (0..n).rev() {
            splits.extend(resolver.submit(i, classify(chunks[i])));
        }
        splits.sort_by_key(|(idx, _)| *idx);

        splits.into_iter().map(|(_, s)| s).collect()
    }

    #[test]
    fn two_partitions_no_quotes() {
        let mut c0 = [b'x'; 64];
        c0[10] = b'\n';
        c0[30] = b'\n';

        let mut c1 = [b'x'; 64];
        c1[3] = b'\n';
        c1[20] = b'\n';

        insta::assert_debug_snapshot!(resolve_in_order(&[&c0, &c1]), @r"
        [
            RecordAlignedRange {
                start: 0,
                end: 68,
            },
            RecordAlignedRange {
                start: 68,
                end: 128,
            },
        ]
        ");
    }

    #[test]
    fn two_partitions_quoted_newline() {
        let mut c0 = [b'x'; 64];
        c0[0] = b'a';
        c0[1] = b',';
        c0[2] = b'"';

        let mut c1 = [b'x'; 64];
        c1[0] = b'\n';
        c1[1] = b'z';
        c1[2] = b'"';
        c1[3] = b'\n';
        c1[10] = b'\n';

        insta::assert_debug_snapshot!(resolve_in_order(&[&c0, &c1]), @r"
        [
            RecordAlignedRange {
                start: 0,
                end: 68,
            },
            RecordAlignedRange {
                start: 68,
                end: 128,
            },
        ]
        ");
    }

    #[test]
    fn two_partitions_even_parity() {
        let mut c0 = [b'x'; 64];
        c0[5] = b'"';
        c0[10] = b'"';

        let mut c1 = [b'x'; 64];
        c1[0] = b'\n';
        c1[5] = b'\n';

        insta::assert_debug_snapshot!(resolve_in_order(&[&c0, &c1]), @r"
        [
            RecordAlignedRange {
                start: 0,
                end: 65,
            },
            RecordAlignedRange {
                start: 65,
                end: 128,
            },
        ]
        ");
    }

    #[test]
    fn single_partition() {
        let mut c0 = [b'x'; 64];
        c0[10] = b'\n';

        let splits = resolve_in_order(&[&c0]);
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].start, 0);
        assert_eq!(splits[0].end, 64);
    }

    #[test]
    fn three_partitions() {
        let mut c0 = [b'x'; 64];
        c0[20] = b'\n';

        let mut c1 = [b'x'; 64];
        c1[5] = b'\n';
        c1[40] = b'\n';

        let mut c2 = [b'x'; 64];
        c2[10] = b'\n';

        let splits = resolve_in_order(&[&c0, &c1, &c2]);

        assert_eq!(splits.len(), 3);

        assert_eq!(splits[0].start, 0);
        assert_eq!(splits[0].end, 70); // first newline in c1 at 64+5, +1a

        assert_eq!(splits[1].start, 70);
        assert_eq!(splits[1].end, 139); // first newline in c2 at 128+10, +1

        assert_eq!(splits[2].start, 139);
        assert_eq!(splits[2].end, 192);
    }

    #[test]
    fn four_partitions() {
        let mut c0 = [b'x'; 64];
        c0[10] = b'\n';

        let mut c1 = [b'x'; 64];
        c1[2] = b'\n';

        let mut c2 = [b'x'; 64];
        c2[7] = b'\n';

        let mut c3 = [b'x'; 64];
        c3[0] = b'\n';

        let splits = resolve_in_order(&[&c0, &c1, &c2, &c3]);

        assert_eq!(splits.len(), 4);

        assert_eq!(splits[0].end, splits[1].start);
        assert_eq!(splits[1].end, splits[2].start);
        assert_eq!(splits[2].end, splits[3].start);
        assert_eq!(splits[3].end, 256);
    }

    #[test]
    fn reverse_order_matches_forward() {
        let mut c0 = [b'x'; 64];
        c0[10] = b'\n';

        let mut c1 = [b'x'; 64];
        c1[5] = b'\n';

        let mut c2 = [b'x'; 64];
        c2[8] = b'\n';

        let mut c3 = [b'x'; 64];
        c3[3] = b'\n';

        let forward = resolve_in_order(&[&c0, &c1, &c2, &c3]);
        let reverse = resolve_in_reverse(&[&c0, &c1, &c2, &c3]);

        assert_eq!(forward.len(), reverse.len());

        for (f, r) in forward.iter().zip(&reverse) {
            assert_eq!(f.start, r.start);
            assert_eq!(f.end, r.end);
        }
    }

    #[test]
    fn interleaved_submission_order() {
        let mut c0 = [b'x'; 64];
        c0[10] = b'\n';

        let mut c1 = [b'x'; 64];
        c1[5] = b'\n';

        let mut c2 = [b'x'; 64];
        c2[8] = b'\n';

        let mut c3 = [b'x'; 64];
        c3[3] = b'\n';

        let chunks = [&c0, &c1, &c2, &c3];
        let boundaries: Arc<[usize]> = vec![0, 64, 128, 192, 256].into();

        let mut resolver = PartitionResolver::new(boundaries);
        let mut splits = Vec::new();

        for i in [2, 0, 3, 1] {
            splits.extend(resolver.submit(i, classify(chunks[i])));
        }

        splits.sort_by_key(|(idx, _)| *idx);

        let splits = splits.into_iter().map(|(_, s)| s).collect::<Vec<_>>();

        let expected = resolve_in_order(&chunks);
        assert_eq!(splits.len(), expected.len());

        for (a, b) in splits.iter().zip(&expected) {
            assert_eq!(a.start, b.start);
            assert_eq!(a.end, b.end);
        }
    }

    #[test]
    fn three_partitions_with_odd_parity() {
        let mut c0 = [b'x'; 64];
        c0[5] = b'"';
        c0[20] = b'\n';

        let mut c1 = [b'x'; 64];
        c1[0] = b'\n';
        c1[3] = b'"';
        c1[5] = b'\n';
        c1[10] = b'\n';

        let mut c2 = [b'x'; 64];
        c2[2] = b'\n';

        let splits = resolve_in_order(&[&c0, &c1, &c2]);

        assert_eq!(splits.len(), 3);
        assert_eq!(splits[1].start, 70);
    }

    #[test]
    fn ranges_are_contiguous() {
        let mut c0 = [b'x'; 64];
        c0[15] = b'\n';

        let mut c1 = [b'x'; 64];
        c1[3] = b'\n';

        let mut c2 = [b'x'; 64];
        c2[7] = b'\n';

        let mut c3 = [b'x'; 64];
        c3[1] = b'\n';

        let mut c4 = [b'x'; 64];
        c4[9] = b'\n';

        let splits = resolve_in_order(&[&c0, &c1, &c2, &c3, &c4]);

        assert_eq!(splits.len(), 5);
        assert_eq!(splits[0].start, 0);
        assert_eq!(splits[4].end, 320);

        for i in 0..4 {
            assert_eq!(splits[i].end, splits[i + 1].start);
        }
    }

    #[test]
    fn test_windowed_chunk_parity_still_correct() {
        let mut data = vec![b'x'; 256];
        data[0] = b'"';
        // quotes are outside the window
        data[200] = b'"';
        data[201] = b'"';

        let mut chunk = PartitionClassifier::new(64);
        chunk.ingest(&data);
        let result = chunk.finish();

        assert!(result.quote_parity);
        assert_eq!(result.newlines_outside.len(), 1);
    }

    #[test]
    fn test_classify_result_identity() {
        let mut c = [b'x'; 64];
        c[0] = b'"';
        c[5] = b'\n';
        let m = classify(&c);
        let e = ClassifyResult::identity();

        assert_eq!(e.combine(&m), m);
        assert_eq!(m.combine(&e), m);
    }

    #[test]
    fn test_classify_result_associativity() {
        let mut c0 = [b'x'; 64];
        c0[0] = b'"';
        c0[10] = b'\n';

        let mut c1 = [b'x'; 64];
        c1[5] = b'"';
        c1[15] = b'"';
        c1[3] = b'\n';

        let mut c2 = [b'x'; 64];
        c2[0] = b'"';
        c2[1] = b'"';
        c2[2] = b'"';
        c2[7] = b'\n';

        let a = classify(&c0);
        let b = classify(&c1);
        let c = classify(&c2);

        assert_eq!(a.combine(&b).combine(&c), a.combine(&b.combine(&c)));
    }
}
