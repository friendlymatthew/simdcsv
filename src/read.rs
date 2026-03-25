use crate::classify::{COMMA, NEWLINE, QUOTES, classify};
use crate::u8x16;
use std::arch::aarch64::vmull_p64;
use std::ops::Range;

pub type FieldRef = Range<usize>;
pub type RowRef = Vec<FieldRef>;

pub fn read(data: &mut Vec<u8>) -> Vec<RowRef> {
    let original_len = data.len();
    data.resize(data.len().next_multiple_of(64), 0);

    // phase 1: classify structual characters
    let vectors = classify(data);
    let capacity = vectors.len() / 4;

    // phase 2: build u64 bitsets
    let comma_broadcast = u8x16::broadcast(COMMA);
    let new_line_broadcast = u8x16::broadcast(NEWLINE);
    let quotation_broadcast = u8x16::broadcast(QUOTES);

    let mut comma_bitsets = Vec::with_capacity(capacity);
    let mut new_line_bitsets = Vec::with_capacity(capacity);
    let mut quotation_bitsets = Vec::with_capacity(capacity);

    vectors.chunks_exact(4).for_each(|chunk| {
        comma_bitsets.push(build_u64(chunk, comma_broadcast));
        new_line_bitsets.push(build_u64(chunk, new_line_broadcast));
        quotation_bitsets.push(build_u64(chunk, quotation_broadcast));
    });

    // phase 3: quote masking
    let mut carry = false;
    for i in 0..quotation_bitsets.len() {
        let bitset = quotation_bitsets[i];

        let inside_quotes = unsafe { vmull_p64(bitset, 0xFFFFFFFFFFFFFFFF_u64) } as u64;
        let outside = if carry { inside_quotes } else { !inside_quotes };

        carry ^= (bitset.count_ones() & 1) != 0;

        comma_bitsets[i] &= outside;
        new_line_bitsets[i] &= outside;
    }

    let comma_count = comma_bitsets.iter().map(|b| b.count_ones() as usize).sum();
    let newline_count = new_line_bitsets
        .iter()
        .map(|b| b.count_ones() as usize)
        .sum();

    // phase 4: extract positions
    let mut comma_positions = Vec::with_capacity(comma_count);
    let mut newline_positions = Vec::with_capacity(newline_count);

    for (i, (&commas, &newlines)) in comma_bitsets.iter().zip(&new_line_bitsets).enumerate() {
        let base = i * 64;
        extract_positions(commas, base, &mut comma_positions);
        extract_positions(newlines, base, &mut newline_positions);
    }

    // phase 5: build rows
    let fields_per_row = comma_positions
        .iter()
        .take_while(|&&p| newline_positions.first().is_none_or(|&nl| p < nl))
        .count()
        + 1;

    let mut rows = Vec::with_capacity(newline_count + 1);
    let mut current_row = Vec::with_capacity(fields_per_row);
    let mut start = 0;
    let mut ci = 0;
    let mut ni = 0;

    while ni < newline_positions.len() {
        for _ in 0..fields_per_row - 1 {
            let end = comma_positions[ci] as usize;
            current_row.push(Range { start, end });
            start = end + 1;
            ci += 1;
        }

        let pos = newline_positions[ni] as usize;
        current_row.push(Range { start, end: pos });

        if data[pos] == b'\r' && data.get(pos + 1) == Some(&b'\n') {
            start = pos + 2;
            if newline_positions.get(ni + 1).copied() == Some(newline_positions[ni] + 1) {
                ni += 1;
            }
        } else {
            start = pos + 1;
        }

        rows.push(std::mem::take(&mut current_row));
        current_row = Vec::with_capacity(fields_per_row);
        ni += 1;
    }

    // flush the last row if the input doesn't end with a newline
    while ci < comma_positions.len() {
        let end = comma_positions[ci] as usize;
        current_row.push(Range { start, end });
        start = end + 1;
        ci += 1;
    }
    if start < original_len {
        current_row.push(Range {
            start,
            end: original_len,
        });
    }

    if !current_row.is_empty() {
        rows.push(current_row);
    }

    rows
}

#[inline]
fn build_u64(chunks: &[u8x16], broadcast: u8x16) -> u64 {
    let a = chunks[0].eq(broadcast).bitset() as u64;
    let b = chunks[1].eq(broadcast).bitset() as u64;
    let c = chunks[2].eq(broadcast).bitset() as u64;
    let d = chunks[3].eq(broadcast).bitset() as u64;
    a | (b << 16) | (c << 32) | (d << 48)
}

#[inline]
fn extract_positions(mut bitmask: u64, base: usize, out: &mut Vec<u32>) {
    while bitmask != 0 {
        let pos = bitmask.trailing_zeros();
        out.push((base + pos as usize) as u32);
        bitmask &= bitmask - 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_rows(test: &[u8], expected: Vec<Vec<String>>) {
        let mut data = test.to_vec();
        let rows = read(&mut data)
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|range| String::from_utf8(test[range].to_vec()).expect("valid"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert_eq!(rows, expected);
    }

    #[test]
    fn read_basic() {
        let data = b"aaa,bbb,ccc";

        parse_rows(
            data,
            vec![vec![
                "aaa".to_string(),
                "bbb".to_string(),
                "ccc".to_string(),
            ]],
        );
    }

    #[test]
    fn read_basic2() {
        let data = b"aaa,\"bbb\",ccc";

        parse_rows(
            data,
            vec![vec![
                "aaa".to_string(),
                "\"bbb\"".to_string(),
                "ccc".to_string(),
            ]],
        );
    }

    #[test]
    fn read_nested() {
        let data = b"\"aaa,howdy\",\"b\"\"bb\",\"ccc\"";

        parse_rows(
            data,
            vec![vec![
                "\"aaa,howdy\"".to_string(),
                "\"b\"\"bb\"".to_string(),
                "\"ccc\"".to_string(),
            ]],
        );
    }

    #[test]
    fn read_newline_field() {
        let data = b"\"aaa,ho\nwdy\",\"b\"\"bb\",\"ccc\"";

        parse_rows(
            data,
            vec![vec![
                "\"aaa,ho\nwdy\"".to_string(),
                "\"b\"\"bb\"".to_string(),
                "\"ccc\"".to_string(),
            ]],
        );
    }

    #[test]
    fn read_crlf_field() {
        let data = b"\"aaa,ho\r\nwdy\",\"b\"\"bb\",\"ccc\"";

        parse_rows(
            data,
            vec![vec![
                "\"aaa,ho\r\nwdy\"".to_string(),
                "\"b\"\"bb\"".to_string(),
                "\"ccc\"".to_string(),
            ]],
        );
    }

    #[test]
    fn read_with_hyphen() {
        parse_rows(
            b"a,b\n1,-1\n",
            vec![vec!["a".into(), "b".into()], vec!["1".into(), "-1".into()]],
        );
    }
}
