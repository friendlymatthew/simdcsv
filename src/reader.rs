use crate::classifier::CsvClassifier;
use crate::classifier::{COMMA_CLASS, NEW_LINE_CLASS, QUOTATION_CLASS};
use crate::u8x16::u8x16;
use std::ops::Range;

pub type FieldRef = Range<usize>;
pub type RowRef = Vec<FieldRef>;

/// [`CsvReader`] holds 3 bits per character in the data set.
/// To understand csv, you only need to know whether a byte is a quotation, comma, new line delimiter, or something else.
#[derive(Debug)]
pub struct CsvReader {
    quotation_bitsets: Vec<u64>,
    comma_bitsets: Vec<u64>,
    new_line_bitsets: Vec<u64>,
}

impl CsvReader {
    pub fn new(data: &[u8]) -> Self {
        // todo: can you store non-utf8 encoded characters in csv?

        let vectors = CsvClassifier::new(data).classify();
        let capacity = vectors.len() / 4 + (vectors.len() % 4 != 0) as usize;

        let comma_broadcast = u8x16::broadcast(COMMA_CLASS);
        let new_line_broadcast = u8x16::broadcast(NEW_LINE_CLASS);
        let quotation_broadcast = u8x16::broadcast(QUOTATION_CLASS);

        let mut comma_bitsets = Vec::with_capacity(capacity);
        let mut new_line_bitsets = Vec::with_capacity(capacity);
        let mut quotation_bitsets = Vec::with_capacity(capacity);

        vectors.chunks(4).for_each(|chunk| {
            comma_bitsets.push(build_u64(chunk, comma_broadcast));
            new_line_bitsets.push(build_u64(chunk, new_line_broadcast));
            quotation_bitsets.push(build_u64(chunk, quotation_broadcast));
        });

        Self {
            comma_bitsets,
            new_line_bitsets,
            quotation_bitsets,
        }
    }
    pub fn read(&mut self) -> Vec<RowRef> {
        let mut rows = Vec::new();
        let mut current_row = Vec::new();

        let mut start = 0;
        let mut end = 0;

        for i in 0..self.quotation_bitsets.len() {
            let valid_quotations = remove_escaped_quotations(self.quotation_bitsets[i]);
            let outside_quotations = !mark_inside_quotations(valid_quotations);

            let mut valid_commas = self.comma_bitsets[i] & outside_quotations;
            let mut valid_new_line = self.new_line_bitsets[i] & outside_quotations;

            // no structual characters exist in this bitset,
            // so we can just advance the end cursor
            if valid_commas == 0 && valid_new_line == 0 {
                end += 64;
                continue;
            }

            loop {
                let first_comma = valid_commas.leading_zeros() as usize;
                let first_new_line = valid_new_line.leading_zeros() as usize;

                let bits_traveled = first_comma.min(first_new_line);

                // there aren't any more structual characters to consider
                // so we just advance the end cursor to the next bitset
                if bits_traveled == 64 {
                    end = (i + 1) * 64;
                    break;
                }

                end += bits_traveled;

                if start < end {
                    current_row.push(Range { start, end });

                    if first_new_line < first_comma {
                        rows.push(current_row.clone());
                        current_row.clear();
                    }
                }

                // consume the structual character
                end += 1;

                valid_commas <<= bits_traveled + 1;
                valid_new_line <<= bits_traveled + 1;

                start = end;
            }
        }

        rows
    }
}

const fn remove_escaped_quotations(q: u64) -> u64 {
    let escaped = q & (q << 1);
    let escaped = escaped | (escaped >> 1);

    q & !escaped
}

/// `mark_inside_quotations` does a parallel xor to mark all bits inbetween a quote pair.
/// Note because of how xor works, the closing quote will be marked as 0. This is fine since
/// we use this to mask commas and new_line in between quote pairs.
#[inline]
const fn mark_inside_quotations(mut x: u64) -> u64 {
    x ^= x << 1;
    x ^= x << 2;
    x ^= x << 4;
    x ^= x << 8;
    x ^= x << 16;
    x ^= x << 32;

    x << 1
}

// todo, find a quicker way to do this
#[inline]
fn build_u64(chunks: &[u8x16], broadcast: u8x16) -> u64 {
    let mut packed: u64 = 0;
    for (i, &chunk) in chunks.iter().enumerate() {
        let word = chunk.eq(broadcast).bitset() as u64;
        packed |= word << (48 - i * 16);
    }
    packed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_rows(test: &[u8], expected: Vec<Vec<String>>) {
        let mut reader = CsvReader::new(test);
        let rows = reader
            .read()
            .iter()
            .map(|row| {
                row.iter()
                    .map(|range| String::from_utf8(test[range.clone()].to_vec()).unwrap())
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
    fn read_taxi_zone_lookup_header() {
        let data = b"\"LocationID\",\"Borough\",\"Zone\",\"service_zone\"\n";

        parse_rows(
            data,
            vec![vec![
                r#""LocationID""#.to_string(),
                r#""Borough""#.to_string(),
                r#""Zone""#.to_string(),
                r#""service_zone""#.to_string(),
            ]],
        );
    }

    #[test]
    fn test_taxi_zone_should_split_two_rows() {
        let data = b"\"LocationID\",\"Borough\",\"Zone\",\"service_zone\"\r\n1,\"EWR\"";

        parse_rows(
            data,
            vec![
                vec![
                    r#""LocationID""#.to_string(),
                    r#""Borough""#.to_string(),
                    r#""Zone""#.to_string(),
                    r#""service_zone""#.to_string(),
                ],
                vec!["1".to_string(), r#""EWR""#.to_string()],
            ],
        );
    }

    //     #[test]
    //     fn read_taxi_zone_lookup() {
    //         let data = r#"
    // "LocationID","Borough","Zone","service_zone"
    // 1,"EWR","Newark Airport","EWR"
    // 2,"Queens","Jamaica Bay","Boro Zone"
    // 3,"Bronx","Allerton/Pelham Gardens","Boro Zone""#;

    //         let mut csv = Vec::new();

    //         for row in CsvReader::new(data.as_bytes()).read() {
    //             let fields = row
    //                 .fields()
    //                 .iter()
    //                 .map(|field_range| {
    //                     String::from_utf8(data[field_range.clone()].as_bytes().to_vec()).unwrap()
    //                 })
    //                 .collect::<Vec<_>>();

    //             csv.push(fields);
    //         }

    //         println!("Statistics\ntotal rows: {}", csv.len());

    //         for (i, row) in csv.iter().enumerate() {
    //             println!("row {}\t{}\n", i, row.join("\t"));
    //         }
    //     }

    #[test]
    fn test_mark_inside_quotations() {
        let res = mark_inside_quotations(0b10001000);
        assert_eq!(res, 0b11110000);

        let res2 = mark_inside_quotations(0b1001_1001);
        assert_eq!(res2, 0b1110_1110);

        let res4 = mark_inside_quotations(0b1100_1001);
        assert_eq!(res4, 0b1000_1110);
    }
}
