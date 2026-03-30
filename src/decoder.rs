use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};

use crate::arrow::build_column;
use crate::classify::classify_and_mask;

#[derive(Debug)]
pub struct Decoder {
    schema: SchemaRef,
    batch_size: usize,

    col_data: Vec<Vec<u8>>,
    col_offsets: Vec<Vec<usize>>,
    num_columns: usize,
    num_rows: usize,

    header_skipped: bool,

    cached_buf: Vec<u8>,
    cached_comma_bitsets: Vec<u64>,
    cached_newline_bitsets: Vec<u64>,
    comma_current: u64,
    comma_idx: usize,
    newline_current: u64,
    newline_idx: usize,
    cached_start: usize,
    cached_bytes_consumed: usize,
}

impl Decoder {
    pub fn new(schema: SchemaRef, batch_size: usize, has_header: bool) -> Self {
        let num_columns = schema.fields().len();
        Self {
            schema,
            batch_size,
            col_data: vec![Vec::new(); num_columns],
            col_offsets: (0..num_columns).map(|_| vec![0]).collect(),
            num_columns,
            num_rows: 0,
            header_skipped: !has_header,
            cached_buf: Vec::new(),
            cached_comma_bitsets: Vec::new(),
            cached_newline_bitsets: Vec::new(),
            comma_current: 0,
            comma_idx: 0,
            newline_current: 0,
            newline_idx: 0,
            cached_start: 0,
            cached_bytes_consumed: 0,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError> {
        if self.capacity() == 0 {
            return Ok(0);
        }

        if !self.cached_buf.is_empty() {
            return self.extract_from_cache();
        }

        if buf.is_empty() {
            return Ok(0);
        }

        self.cached_buf.clear();
        self.cached_buf.extend_from_slice(buf);
        let original_len = self.cached_buf.len();
        self.cached_buf.resize(original_len.next_multiple_of(64), 0);

        let result = classify_and_mask(&self.cached_buf);

        self.cached_buf.truncate(original_len);
        self.cached_comma_bitsets = result.comma_bitsets;
        self.cached_newline_bitsets = result.newline_bitsets;
        self.comma_current = self.cached_comma_bitsets.first().copied().unwrap_or(0);
        self.comma_idx = 0;
        self.newline_current = self.cached_newline_bitsets.first().copied().unwrap_or(0);
        self.newline_idx = 0;
        self.cached_start = 0;
        self.cached_bytes_consumed = 0;

        if !self.header_skipped {
            if let Some(nl_pos) = self.next_newline() {
                self.skip_crlf(nl_pos);
                self.advance_commas_past(self.cached_start);
                self.cached_bytes_consumed = self.cached_start;
                self.header_skipped = true;
            } else {
                self.clear_cache();
                return Ok(0);
            }
        }

        self.extract_from_cache()
    }

    fn extract_from_cache(&mut self) -> Result<usize, ArrowError> {
        let rows_to_read = self.capacity();
        let mut rows_read = 0;
        let cols = self.num_columns;
        let mut exhausted = false;

        while rows_read < rows_to_read {
            let nl_pos = match self.next_newline() {
                Some(pos) => pos,
                None => {
                    exhausted = true;
                    break;
                }
            };

            for col in 0..cols.saturating_sub(1) {
                if let Some(end) = self.next_comma() {
                    let start = self.cached_start;
                    self.push_field(col, start, end);
                    self.cached_start = end + 1;
                }
            }

            let start = self.cached_start;
            self.push_field(cols - 1, start, nl_pos);
            self.skip_crlf(nl_pos);

            self.num_rows += 1;
            rows_read += 1;
            self.cached_bytes_consumed = self.cached_start;
        }

        if exhausted {
            let consumed = self.cached_bytes_consumed;
            self.clear_cache();
            Ok(consumed)
        } else {
            Ok(0)
        }
    }

    fn skip_crlf(&mut self, nl_pos: usize) {
        if self.cached_buf[nl_pos] == b'\r' && self.cached_buf.get(nl_pos + 1) == Some(&b'\n') {
            self.cached_start = nl_pos + 2;
            let lf_pos = nl_pos + 1;
            let word = lf_pos / 64;
            let bit = lf_pos % 64;
            let mask = !(1u64 << bit);
            self.cached_newline_bitsets[word] &= mask;
            let same = (word == self.newline_idx) as u64;
            self.newline_current &= mask | !same.wrapping_neg();
        } else {
            self.cached_start = nl_pos + 1;
        }
    }

    fn advance_commas_past(&mut self, pos: usize) {
        let word = pos / 64;
        let bit = pos % 64;
        self.comma_idx = word;
        self.comma_current = self.cached_comma_bitsets.get(word).copied().unwrap_or(0)
            & !((1u64 << bit).wrapping_sub(1));
    }

    #[inline(always)]
    fn next_comma(&mut self) -> Option<usize> {
        next_bit(
            &mut self.comma_current,
            &mut self.comma_idx,
            &self.cached_comma_bitsets,
        )
    }

    #[inline(always)]
    fn next_newline(&mut self) -> Option<usize> {
        next_bit(
            &mut self.newline_current,
            &mut self.newline_idx,
            &self.cached_newline_bitsets,
        )
    }

    fn clear_cache(&mut self) {
        self.cached_buf.clear();
        self.cached_comma_bitsets.clear();
        self.cached_newline_bitsets.clear();
        self.comma_current = 0;
        self.comma_idx = 0;
        self.newline_current = 0;
        self.newline_idx = 0;
        self.cached_start = 0;
        self.cached_bytes_consumed = 0;
    }

    pub fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        if self.num_rows == 0 {
            return Ok(None);
        }

        let num_rows = self.num_rows;

        let columns = (0..self.num_columns)
            .map(|col| {
                let data = std::mem::take(&mut self.col_data[col]);
                let offsets = std::mem::take(&mut self.col_offsets[col]);

                build_column(self.schema.field(col), data, offsets, col, num_rows)
            })
            .collect::<Result<Vec<_>, ArrowError>>()?;

        for offsets in &mut self.col_offsets {
            offsets.push(0);
        }

        self.num_rows = 0;

        RecordBatch::try_new(self.schema.clone(), columns).map(Some)
    }

    pub const fn capacity(&self) -> usize {
        self.batch_size - self.num_rows
    }

    fn push_field(&mut self, col: usize, start: usize, end: usize) {
        let raw = &self.cached_buf[start..end];
        let col_data = &mut self.col_data[col];
        if raw.len() >= 2 && raw[0] == b'"' && raw[raw.len() - 1] == b'"' {
            let inner = &self.cached_buf[start + 1..end - 1];
            let mut i = 0;
            while i < inner.len() {
                if inner[i] == b'"' && inner.get(i + 1) == Some(&b'"') {
                    col_data.push(b'"');
                    i += 2;
                } else {
                    col_data.push(inner[i]);
                    i += 1;
                }
            }
        } else {
            col_data.extend_from_slice(raw);
        }
        self.col_offsets[col].push(self.col_data[col].len());
    }
}

#[inline(always)]
fn next_bit(current: &mut u64, idx: &mut usize, bitsets: &[u64]) -> Option<usize> {
    while *current == 0 {
        *idx += 1;

        if *idx >= bitsets.len() {
            return None;
        }

        *current = unsafe { *bitsets.get_unchecked(*idx) };
    }

    let pos = current.trailing_zeros() as usize;
    *current &= *current - 1;

    Some(*idx * 64 + pos)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn utf8_schema(names: &[&str]) -> SchemaRef {
        Arc::new(Schema::new(
            names
                .iter()
                .map(|n| Field::new(*n, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn basic_decode_flush() {
        let schema = utf8_schema(&["a", "b", "c"]);
        let mut decoder = Decoder::new(schema, 1024, false);

        let input = b"aaa,bbb,ccc\n111,222,333\n";
        let consumed = decoder.decode(input).unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(decoder.capacity(), 1024 - 2);

        let batch = decoder.flush().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        let col0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col0.value(0), "aaa");
        assert_eq!(col0.value(1), "111");

        let col2 = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col2.value(0), "ccc");
        assert_eq!(col2.value(1), "333");
    }

    #[test]
    fn with_header() {
        let schema = utf8_schema(&["name", "value"]);
        let mut decoder = Decoder::new(schema, 1024, true);

        let input = b"name,value\nalice,30\nbob,25\n";
        let consumed = decoder.decode(input).unwrap();
        assert_eq!(consumed, input.len());

        let batch = decoder.flush().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let col0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col0.value(0), "alice");
        assert_eq!(col0.value(1), "bob");
    }

    #[test]
    fn quote_unescaping() {
        let schema = utf8_schema(&["a", "b", "c"]);
        let mut decoder = Decoder::new(schema, 1024, false);

        let input = b"\"hello, world\",\"say \"\"hi\"\"\",plain\n";
        decoder.decode(input).unwrap();

        let batch = decoder.flush().unwrap().unwrap();

        let col0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let col1 = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let col2 = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(col0.value(0), "hello, world");
        assert_eq!(col1.value(0), "say \"hi\"");
        assert_eq!(col2.value(0), "plain");
    }

    #[test]
    fn batch_size_chunking() {
        let schema = utf8_schema(&["x"]);
        let mut decoder = Decoder::new(schema, 2, false);

        let input = b"a\nb\nc\nd\n";

        let consumed1 = decoder.decode(input).unwrap();
        let batch1 = decoder.flush().unwrap().unwrap();
        assert_eq!(batch1.num_rows(), 2);

        // continue decoding — cache still has remaining rows
        decoder.decode(&input[consumed1..]).unwrap();
        let batch2 = decoder.flush().unwrap().unwrap();
        assert_eq!(batch2.num_rows(), 2);
    }

    #[test]
    fn typed_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
        ]));
        let mut decoder = Decoder::new(schema, 1024, false);

        let input = b"alice,30,95.5\nbob,25,87.3\n";
        decoder.decode(input).unwrap();

        let batch = decoder.flush().unwrap().unwrap();

        let ages = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let scores = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(ages.value(0), 30);
        assert_eq!(ages.value(1), 25);
        assert!((scores.value(0) - 95.5).abs() < f64::EPSILON);
        assert!((scores.value(1) - 87.3).abs() < f64::EPSILON);
    }

    #[test]
    fn flush_empty_returns_none() {
        let schema = utf8_schema(&["a"]);
        let mut decoder = Decoder::new(schema, 1024, false);

        assert!(decoder.flush().unwrap().is_none());
    }
}
