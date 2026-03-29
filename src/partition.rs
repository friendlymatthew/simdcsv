use std::sync::Arc;

use crate::classify::{self, HIGH_NIBBLES, LOW_NIBBLES, NEWLINE, QUOTES};
use crate::monoid::{ClassifyResult, QuoteParity, prefix_scan};
use crate::simd::u8x16;

/// A partition streams bytes and produces newline bitsets
/// for both possible initial quote states
///
/// To keep memory bounded, only bitsets for the first `window` bytes are
/// retained. The rest of the data is streamed through just for parity counting.
///
/// This way, memory is O(window).
#[derive(Debug)]
pub struct SubPartition {
    newlines_outside: Vec<u64>,
    newlines_inside: Vec<u64>,
    quote_parity: bool,
    window: usize,
    bytes_classified: usize,
    remainder: Vec<u8>,
}

impl SubPartition {
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
        let low_nib = u8x16::from_slice_unchecked(&LOW_NIBBLES);
        let high_nib = u8x16::from_slice_unchecked(&HIGH_NIBBLES);
        let newline_bc = u8x16::broadcast(NEWLINE);
        let quote_bc = u8x16::broadcast(QUOTES);

        let (c0, c1, c2, c3) = classify::classify_four_lanes(block, high_nib, low_nib);

        let quote = classify::build_eq_bitset(c0, c1, c2, c3, quote_bc);
        let raw_newline = classify::build_eq_bitset(c0, c1, c2, c3, newline_bc);

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
pub struct ResolvedSplit {
    pub start: usize,
    pub end: usize,
}

pub struct PartitionResolver {
    boundaries: Arc<[usize]>,
    results: Vec<Option<ClassifyResult>>,
}

impl PartitionResolver {
    pub fn new(boundaries: Arc<[usize]>) -> Self {
        let num_partitions = boundaries.len() - 1;
        Self {
            boundaries,
            results: (0..num_partitions).map(|_| None).collect(),
        }
    }

    pub fn partition_range(&self, idx: usize) -> (usize, usize) {
        (self.boundaries[idx], self.boundaries[idx + 1])
    }

    pub fn submit(&mut self, partition_idx: usize, result: ClassifyResult) {
        self.results[partition_idx] = Some(result);
    }

    pub fn resolve(&self) -> Vec<ResolvedSplit> {
        let parities = self
            .results
            .iter()
            .map(|r| QuoteParity(r.as_ref().expect("partition not submitted").quote_parity));

        let mut candidates = Vec::with_capacity(self.results.len() + 1);
        candidates.push((Some(0), false));

        for (i, carry) in prefix_scan(parities).enumerate().skip(1) {
            let chunk_start = self.boundaries[i];
            let result = self.results[i].as_ref().expect("results not found");

            let newlines = if carry.0 {
                &result.newlines_inside
            } else {
                &result.newlines_outside
            };

            let split = find_first_set_bit(newlines).map(|pos| chunk_start + pos + 1);
            candidates.push((split, carry.0));
        }

        let file_end = *self.boundaries.last().unwrap();

        let mut splits = Vec::with_capacity(candidates.len());
        let mut prev = None;

        for &(curr, carry) in &candidates {
            let Some(start) = curr else {
                continue;
            };

            if let Some((prev_start, _)) = prev {
                splits.push(ResolvedSplit {
                    start: prev_start,
                    end: start,
                });
            }

            prev = Some((start, carry));
        }

        if let Some((start, _)) = prev {
            splits.push(ResolvedSplit {
                start,
                end: file_end,
            });
        }

        splits
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

    fn split_two_chunks(chunk0: &[u8], chunk1: &[u8]) -> Vec<ResolvedSplit> {
        assert_eq!(chunk0.len(), 64);
        assert_eq!(chunk1.len(), 64);

        let boundaries = vec![0, 64, 128].into();

        let mut s0 = SubPartition::new(TEST_WINDOW);
        s0.ingest(chunk0);

        let r0 = s0.finish();

        let mut s1 = SubPartition::new(TEST_WINDOW);
        s1.ingest(chunk1);
        let r1 = s1.finish();

        let mut resolver = PartitionResolver::new(boundaries);
        resolver.submit(0, r0);
        resolver.submit(1, r1);

        resolver.resolve()
    }

    #[test]
    fn test_simple_split_no_quotes() {
        let mut chunk0 = [b'x'; 64];
        chunk0[10] = b'\n';
        chunk0[30] = b'\n';

        let mut chunk1 = [b'x'; 64];
        chunk1[3] = b'\n';
        chunk1[20] = b'\n';

        insta::assert_debug_snapshot!(split_two_chunks(&chunk0, &chunk1), @r"
        [
            ResolvedSplit {
                start: 0,
                end: 68,
            },
            ResolvedSplit {
                start: 68,
                end: 128,
            },
        ]
        ");
    }

    #[test]
    fn test_split_with_quoted_newline() {
        let mut chunk0 = [b'x'; 64];
        chunk0[0] = b'a';
        chunk0[1] = b',';
        chunk0[2] = b'"';

        let mut chunk1 = [b'x'; 64];
        chunk1[0] = b'\n';
        chunk1[1] = b'z';
        chunk1[2] = b'"';
        chunk1[3] = b'\n';
        chunk1[10] = b'\n';

        insta::assert_debug_snapshot!(split_two_chunks(&chunk0, &chunk1), @r"
        [
            ResolvedSplit {
                start: 0,
                end: 68,
            },
            ResolvedSplit {
                start: 68,
                end: 128,
            },
        ]
        ");
    }

    #[test]
    fn test_split_even_parity_no_shift() {
        let mut chunk0 = [b'x'; 64];
        chunk0[5] = b'"';
        chunk0[10] = b'"';

        let mut chunk1 = [b'x'; 64];
        chunk1[0] = b'\n';
        chunk1[5] = b'\n';

        insta::assert_debug_snapshot!(split_two_chunks(&chunk0, &chunk1), @r"
        [
            ResolvedSplit {
                start: 0,
                end: 65,
            },
            ResolvedSplit {
                start: 65,
                end: 128,
            },
        ]
        ");
    }

    #[test]
    fn test_windowed_chunk_parity_still_correct() {
        let mut data = vec![b'x'; 256];
        data[0] = b'"';
        // quotes are outside the window
        data[200] = b'"';
        data[201] = b'"';

        let mut chunk = SubPartition::new(64);
        chunk.ingest(&data);
        let result = chunk.finish();

        assert!(result.quote_parity);
        assert_eq!(result.newlines_outside.len(), 1);
    }

    fn scan_chunk(chunk: &[u8; 64]) -> ClassifyResult {
        let mut s = SubPartition::new(TEST_WINDOW);
        s.ingest(chunk);
        s.finish()
    }

    #[test]
    fn test_classify_result_identity() {
        let mut c = [b'x'; 64];
        c[0] = b'"';
        c[5] = b'\n';
        let m = scan_chunk(&c);
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

        let a = scan_chunk(&c0);
        let b = scan_chunk(&c1);
        let c = scan_chunk(&c2);

        assert_eq!(a.combine(&b).combine(&c), a.combine(&b.combine(&c)));
    }
}
