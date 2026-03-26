use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder,
    Int64Builder, StringBuilder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{ArrowError, DataType, SchemaRef};

use crate::classify::{COMMA, NEWLINE, QUOTES, classify};
use crate::u8x16;

#[derive(Debug)]
pub struct Decoder {
    schema: SchemaRef,
    batch_size: usize,

    field_data: Vec<u8>,
    field_offsets: Vec<usize>,
    num_columns: usize,
    num_rows: usize,

    header_skipped: bool,
}

impl Decoder {
    pub(crate) fn new(schema: SchemaRef, batch_size: usize, has_header: bool) -> Self {
        let num_columns = schema.fields().len();
        Self {
            schema,
            batch_size,
            field_data: Vec::new(),
            field_offsets: vec![0],
            num_columns,
            num_rows: 0,
            header_skipped: !has_header,
        }
    }

    pub fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError> {
        if buf.is_empty() || self.capacity() == 0 {
            return Ok(0);
        }

        let original_len = buf.len();

        // pad for SIMD (64-byte alignment)
        let mut padded = buf.to_vec();
        padded.resize(padded.len().next_multiple_of(64), 0);

        // phase 1: classify
        let vectors = classify(&padded);

        // phase 2: bitsets
        let comma_bc = u8x16::broadcast(COMMA);
        let newline_bc = u8x16::broadcast(NEWLINE);
        let quote_bc = u8x16::broadcast(QUOTES);

        let cap = vectors.len() / 4;
        let mut comma_bitsets = Vec::with_capacity(cap);
        let mut newline_bitsets = Vec::with_capacity(cap);
        let mut quote_bitsets = Vec::with_capacity(cap);

        vectors.chunks_exact(4).for_each(|chunk| {
            comma_bitsets.push(build_u64(chunk, comma_bc));
            newline_bitsets.push(build_u64(chunk, newline_bc));
            quote_bitsets.push(build_u64(chunk, quote_bc));
        });

        // phase 3: quote mask
        let mut carry = false;
        for i in 0..quote_bitsets.len() {
            let bitset = quote_bitsets[i];
            let inside =
                unsafe { std::arch::aarch64::vmull_p64(bitset, 0xFFFFFFFFFFFFFFFF_u64) } as u64;
            let outside = if carry { inside } else { !inside };
            carry ^= (bitset.count_ones() & 1) != 0;

            comma_bitsets[i] &= outside;
            newline_bitsets[i] &= outside;
        }

        // phase 4: extract positions
        let comma_count = comma_bitsets.iter().map(|b| b.count_ones() as usize).sum();
        let newline_count: usize = newline_bitsets
            .iter()
            .map(|b| b.count_ones() as usize)
            .sum();

        let mut comma_pos = Vec::with_capacity(comma_count);
        let mut newline_pos = Vec::with_capacity(newline_count);

        for (i, (&c, &n)) in comma_bitsets.iter().zip(&newline_bitsets).enumerate() {
            let base = i * 64;
            extract_positions(c, base, &mut comma_pos);
            extract_positions(n, base, &mut newline_pos);
        }

        // phase 5: extract fields into field_data/field_offsets
        let mut start = 0usize;
        let mut ci = 0;
        let mut ni = 0;
        let mut bytes_consumed = 0;
        let rows_to_read = self.capacity();
        let mut rows_read = 0;

        // skip header row
        if !self.header_skipped {
            if let Some(&nl) = newline_pos.first() {
                let pos = nl as usize;
                if pos < original_len {
                    if buf[pos] == b'\r' && buf.get(pos + 1) == Some(&b'\n') {
                        start = pos + 2;
                        if newline_pos.get(1).copied() == Some(nl + 1) {
                            ni += 1;
                        }
                    } else {
                        start = pos + 1;
                    }
                    while ci < comma_pos.len() && (comma_pos[ci] as usize) < start {
                        ci += 1;
                    }
                    ni += 1;
                    bytes_consumed = start;
                    self.header_skipped = true;
                }
            }

            if !self.header_skipped {
                return Ok(0);
            }
        }

        let cols = self.num_columns;

        while ni < newline_pos.len() && rows_read < rows_to_read {
            let pos = newline_pos[ni] as usize;
            if pos >= original_len {
                break;
            }

            for _ in 0..cols.saturating_sub(1) {
                if ci < comma_pos.len() {
                    let end = comma_pos[ci] as usize;
                    self.push_field(&buf[start..end]);
                    start = end + 1;
                    ci += 1;
                }
            }

            self.push_field(&buf[start..pos]);

            if buf[pos] == b'\r' && buf.get(pos + 1) == Some(&b'\n') {
                start = pos + 2;
                if newline_pos.get(ni + 1).copied() == Some(newline_pos[ni] + 1) {
                    ni += 1;
                }
            } else {
                start = pos + 1;
            }

            self.num_rows += 1;
            rows_read += 1;
            ni += 1;
            bytes_consumed = start;
        }

        Ok(bytes_consumed)
    }

    pub fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        if self.num_rows == 0 {
            return Ok(None);
        }

        let num_rows = self.num_rows;

        let columns = (0..self.num_columns)
            .map(|col| self.build_column(col, num_rows))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        self.field_data.clear();
        self.field_offsets.clear();
        self.field_offsets.push(0);
        self.num_rows = 0;

        RecordBatch::try_new(self.schema.clone(), columns).map(Some)
    }

    pub const fn capacity(&self) -> usize {
        self.batch_size - self.num_rows
    }

    fn get_field(&self, row: usize, col: usize) -> &[u8] {
        let idx = row * self.num_columns + col;
        &self.field_data[self.field_offsets[idx]..self.field_offsets[idx + 1]]
    }

    fn get_field_str(&self, row: usize, col: usize) -> Result<&str, ArrowError> {
        let field = self.get_field(row, col);

        std::str::from_utf8(field).map_err(|e| {
            ArrowError::ParseError(format!("invalid utf at row {row}, col {col}: {e}"))
        })
    }

    fn build_column(&self, col: usize, num_rows: usize) -> Result<ArrayRef, ArrowError> {
        let field = self.schema.field(col);
        let nullable = field.is_nullable();

        match field.data_type() {
            DataType::Boolean => {
                let mut b = BooleanBuilder::with_capacity(num_rows);
                for row in 0..num_rows {
                    match self.get_field(row, col) {
                        [] if nullable => b.append_null(),
                        b"true" | b"TRUE" | b"True" | b"1" => b.append_value(true),
                        b"false" | b"FALSE" | b"False" | b"0" => b.append_value(false),
                        _ => {
                            return Err(ArrowError::ParseError(format!(
                                "cannot parse as Boolean at row {row}, col {col}"
                            )));
                        }
                    }
                }

                Ok(Arc::new(b.finish()))
            }
            DataType::Utf8 => {
                let mut b = StringBuilder::with_capacity(num_rows, num_rows * 16);
                for row in 0..num_rows {
                    let s = self.get_field_str(row, col)?;
                    if s.is_empty() && nullable {
                        b.append_null();
                    } else {
                        b.append_value(s);
                    }
                }
                Ok(Arc::new(b.finish()))
            }
            DataType::Int8 => build_primitive!(self, col, num_rows, nullable, Int8Builder, i8),
            DataType::Int16 => build_primitive!(self, col, num_rows, nullable, Int16Builder, i16),
            DataType::Int32 => build_primitive!(self, col, num_rows, nullable, Int32Builder, i32),
            DataType::Int64 => build_primitive!(self, col, num_rows, nullable, Int64Builder, i64),
            DataType::UInt8 => build_primitive!(self, col, num_rows, nullable, UInt8Builder, u8),
            DataType::UInt16 => {
                build_primitive!(self, col, num_rows, nullable, UInt16Builder, u16)
            }
            DataType::UInt32 => {
                build_primitive!(self, col, num_rows, nullable, UInt32Builder, u32)
            }
            DataType::UInt64 => {
                build_primitive!(self, col, num_rows, nullable, UInt64Builder, u64)
            }
            DataType::Float32 => {
                build_primitive!(self, col, num_rows, nullable, Float32Builder, f32)
            }
            DataType::Float64 => {
                build_primitive!(self, col, num_rows, nullable, Float64Builder, f64)
            }
            other => Err(ArrowError::NotYetImplemented(format!(
                "data type {other} not yet supported"
            ))),
        }
    }

    fn push_field(&mut self, raw: &[u8]) {
        if raw.len() >= 2 && raw[0] == b'"' && raw[raw.len() - 1] == b'"' {
            // quoted field: strip outer quotes, unescape "" -> "
            let inner = &raw[1..raw.len() - 1];
            let mut i = 0;
            while i < inner.len() {
                if inner[i] == b'"' && inner.get(i + 1) == Some(&b'"') {
                    self.field_data.push(b'"');
                    i += 2;
                } else {
                    self.field_data.push(inner[i]);
                    i += 1;
                }
            }
        } else {
            self.field_data.extend_from_slice(raw);
        }

        self.field_offsets.push(self.field_data.len());
    }
}

macro_rules! build_primitive {
    ($self:expr, $col:expr, $num_rows:expr, $nullable:expr, $builder:ty, $native:ty) => {{
        let mut b = <$builder>::with_capacity($num_rows);
        for row in 0..$num_rows {
            let s = $self.get_field_str(row, $col)?;
            if s.is_empty() && $nullable {
                b.append_null();
            } else {
                let v: $native = s.parse().map_err(|_| {
                    ArrowError::ParseError(format!(
                        "cannot parse '{}' as {} at row {}, col {}",
                        s,
                        stringify!($native),
                        row,
                        $col
                    ))
                })?;
                b.append_value(v);
            }
        }
        Ok(Arc::new(b.finish()) as ArrayRef)
    }};
}
use build_primitive;

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
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{Field, Schema};

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
        assert_eq!(decoder.capacity(), 0);

        let batch1 = decoder.flush().unwrap().unwrap();
        assert_eq!(batch1.num_rows(), 2);

        let consumed2 = decoder.decode(&input[consumed1..]).unwrap();
        assert_eq!(decoder.capacity(), 0);

        let batch2 = decoder.flush().unwrap().unwrap();
        assert_eq!(batch2.num_rows(), 2);

        assert_eq!(consumed1 + consumed2, input.len());
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
