use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BinaryViewBuilder, BooleanBuilder, Date32Builder, Date64Builder,
    Decimal128Builder, Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder,
    Int64Builder, LargeBinaryBuilder, LargeStringBuilder, StringBuilder, StringViewBuilder,
    TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
    TimestampSecondBuilder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_cast::parse::Parser;
use arrow_schema::{ArrowError, DataType, SchemaRef, TimeUnit};

use crate::classify::{COMMA, HIGH_NIBBLES, LOW_NIBBLES, NEWLINE, QUOTES};
use crate::u8x16;

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
    pub(crate) fn new(schema: SchemaRef, batch_size: usize, has_header: bool) -> Self {
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
        if buf.is_empty() || self.capacity() == 0 {
            return Ok(0);
        }

        // if we have cached bitsets from a previous scan, continue extracting from them
        if !self.cached_buf.is_empty() {
            return self.extract_from_cache();
        }

        self.cached_buf.clear();
        self.cached_buf.extend_from_slice(buf);

        let original_len = self.cached_buf.len();
        self.cached_buf.resize(original_len.next_multiple_of(64), 0);

        // phase 1+2: classify and build bitsets in one pass
        let low_nibbles = u8x16::from_slice_unchecked(&LOW_NIBBLES);
        let high_nibbles = u8x16::from_slice_unchecked(&HIGH_NIBBLES);
        let comma_bc = u8x16::broadcast(COMMA);
        let newline_bc = u8x16::broadcast(NEWLINE);
        let quote_bc = u8x16::broadcast(QUOTES);

        let cap = self.cached_buf.len() / 64;
        let mut comma_bitsets = Vec::with_capacity(cap);
        let mut newline_bitsets = Vec::with_capacity(cap);
        let mut quote_bitsets = Vec::with_capacity(cap);

        for chunk in self.cached_buf.chunks_exact(64) {
            let v0 = classify_one(&chunk[0..16], high_nibbles, low_nibbles);
            let v1 = classify_one(&chunk[16..32], high_nibbles, low_nibbles);
            let v2 = classify_one(&chunk[32..48], high_nibbles, low_nibbles);
            let v3 = classify_one(&chunk[48..64], high_nibbles, low_nibbles);

            comma_bitsets.push(build_u64_from_classified(v0, v1, v2, v3, comma_bc));
            newline_bitsets.push(build_u64_from_classified(v0, v1, v2, v3, newline_bc));
            quote_bitsets.push(build_u64_from_classified(v0, v1, v2, v3, quote_bc));
        }

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

        // cache bitsets and initialize iterator state
        self.cached_buf.truncate(original_len);
        self.cached_comma_bitsets = comma_bitsets;
        self.cached_newline_bitsets = newline_bitsets;
        self.comma_current = self.cached_comma_bitsets.first().copied().unwrap_or(0);
        self.comma_idx = 0;
        self.newline_current = self.cached_newline_bitsets.first().copied().unwrap_or(0);
        self.newline_idx = 0;
        self.cached_start = 0;
        self.cached_bytes_consumed = 0;

        // skip header
        if !self.header_skipped {
            if let Some(nl_pos) = self.next_newline() {
                if self.cached_buf[nl_pos] == b'\r'
                    && self.cached_buf.get(nl_pos + 1) == Some(&b'\n')
                {
                    self.cached_start = nl_pos + 2;
                    // skip the \n that follows \r
                    if self.peek_newline() == Some(nl_pos + 1) {
                        self.next_newline();
                    }
                } else {
                    self.cached_start = nl_pos + 1;
                }

                loop {
                    match self.peek_comma() {
                        Some(pos) if pos < self.cached_start => {
                            self.next_comma();
                        }
                        _ => break,
                    }
                }
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

        while rows_read < rows_to_read {
            let nl_pos = match self.next_newline() {
                Some(pos) => pos,
                None => break,
            };

            for col in 0..cols.saturating_sub(1) {
                if let Some(end) = self.next_comma() {
                    let start = self.cached_start;
                    self.push_field_to_column(col, start, end);
                    self.cached_start = end + 1;
                }
            }

            let start = self.cached_start;
            self.push_field_to_column(cols - 1, start, nl_pos);

            if self.cached_buf[nl_pos] == b'\r' && self.cached_buf.get(nl_pos + 1) == Some(&b'\n') {
                self.cached_start = nl_pos + 2;
                if self.peek_newline() == Some(nl_pos + 1) {
                    self.next_newline();
                }
            } else {
                self.cached_start = nl_pos + 1;
            }

            self.num_rows += 1;
            rows_read += 1;
            self.cached_bytes_consumed = self.cached_start;
        }

        // if we've consumed all cached newlines, return bytes consumed and clear cache
        if self.peek_newline().is_none() {
            let consumed = self.cached_bytes_consumed;
            self.clear_cache();
            return Ok(consumed);
        }

        // still have cached data — return 0 to signal "flush then call decode again"
        Ok(0)
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
    fn peek_comma(&self) -> Option<usize> {
        peek_bit(
            self.comma_current,
            self.comma_idx,
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

    #[inline(always)]
    fn peek_newline(&self) -> Option<usize> {
        peek_bit(
            self.newline_current,
            self.newline_idx,
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
            .map(|col| self.build_column(col, num_rows))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        for data in &mut self.col_data {
            data.clear();
        }

        for offsets in &mut self.col_offsets {
            offsets.clear();
            offsets.push(0);
        }

        self.num_rows = 0;

        RecordBatch::try_new(self.schema.clone(), columns).map(Some)
    }

    pub const fn capacity(&self) -> usize {
        self.batch_size - self.num_rows
    }

    fn build_column(&self, col: usize, num_rows: usize) -> Result<ArrayRef, ArrowError> {
        let field = self.schema.field(col);
        let nullable = field.is_nullable();
        let data = &self.col_data[col];
        let offsets = &self.col_offsets[col];

        match field.data_type() {
            DataType::Null => Ok(Arc::new(arrow_array::NullArray::new(num_rows))),
            DataType::Boolean => {
                let mut b = BooleanBuilder::with_capacity(num_rows);
                for row in 0..num_rows {
                    match &data[offsets[row]..offsets[row + 1]] {
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
            DataType::Utf8 => build_string_col!(
                data,
                offsets,
                col,
                num_rows,
                nullable,
                StringBuilder::with_capacity(num_rows, data.len())
            ),
            DataType::LargeUtf8 => build_string_col!(
                data,
                offsets,
                col,
                num_rows,
                nullable,
                LargeStringBuilder::with_capacity(num_rows, data.len())
            ),
            DataType::Utf8View => build_string_col!(
                data,
                offsets,
                col,
                num_rows,
                nullable,
                StringViewBuilder::with_capacity(num_rows)
            ),
            DataType::Binary => build_binary_col!(
                data,
                offsets,
                num_rows,
                nullable,
                BinaryBuilder::with_capacity(num_rows, data.len())
            ),
            DataType::LargeBinary => build_binary_col!(
                data,
                offsets,
                num_rows,
                nullable,
                LargeBinaryBuilder::with_capacity(num_rows, data.len())
            ),
            DataType::BinaryView => build_binary_col!(
                data,
                offsets,
                num_rows,
                nullable,
                BinaryViewBuilder::with_capacity(num_rows)
            ),
            DataType::Int8 => {
                build_int_col!(data, offsets, col, num_rows, nullable, Int8Builder, i8)
            }
            DataType::Int16 => {
                build_int_col!(data, offsets, col, num_rows, nullable, Int16Builder, i16)
            }
            DataType::Int32 => {
                build_int_col!(data, offsets, col, num_rows, nullable, Int32Builder, i32)
            }
            DataType::Int64 => {
                build_int_col!(data, offsets, col, num_rows, nullable, Int64Builder, i64)
            }
            DataType::UInt8 => {
                build_int_col!(data, offsets, col, num_rows, nullable, UInt8Builder, u8)
            }
            DataType::UInt16 => {
                build_int_col!(data, offsets, col, num_rows, nullable, UInt16Builder, u16)
            }
            DataType::UInt32 => {
                build_int_col!(data, offsets, col, num_rows, nullable, UInt32Builder, u32)
            }
            DataType::UInt64 => {
                build_int_col!(data, offsets, col, num_rows, nullable, UInt64Builder, u64)
            }
            DataType::Float32 => {
                build_float_col!(data, offsets, col, num_rows, nullable, Float32Builder, f32)
            }
            DataType::Float64 => {
                build_float_col!(data, offsets, col, num_rows, nullable, Float64Builder, f64)
            }
            DataType::Date32 => {
                build_parsed_col!(
                    data,
                    offsets,
                    col,
                    num_rows,
                    nullable,
                    Date32Builder,
                    arrow_array::types::Date32Type
                )
            }
            DataType::Date64 => {
                build_parsed_col!(
                    data,
                    offsets,
                    col,
                    num_rows,
                    nullable,
                    Date64Builder,
                    arrow_array::types::Date64Type
                )
            }
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Second => {
                    build_parsed_col!(
                        data,
                        offsets,
                        col,
                        num_rows,
                        nullable,
                        TimestampSecondBuilder,
                        arrow_array::types::TimestampSecondType
                    )
                }
                TimeUnit::Millisecond => {
                    build_parsed_col!(
                        data,
                        offsets,
                        col,
                        num_rows,
                        nullable,
                        TimestampMillisecondBuilder,
                        arrow_array::types::TimestampMillisecondType
                    )
                }
                TimeUnit::Microsecond => {
                    build_parsed_col!(
                        data,
                        offsets,
                        col,
                        num_rows,
                        nullable,
                        TimestampMicrosecondBuilder,
                        arrow_array::types::TimestampMicrosecondType
                    )
                }
                TimeUnit::Nanosecond => {
                    build_parsed_col!(
                        data,
                        offsets,
                        col,
                        num_rows,
                        nullable,
                        TimestampNanosecondBuilder,
                        arrow_array::types::TimestampNanosecondType
                    )
                }
            },
            DataType::Decimal128(precision, scale) => {
                simdutf8::basic::from_utf8(data).map_err(|e| {
                    ArrowError::ParseError(format!("invalid utf-8 in col {col}: {e}"))
                })?;

                let mut b = Decimal128Builder::with_capacity(num_rows)
                    .with_data_type(DataType::Decimal128(*precision, *scale));
                for row in 0..num_rows {
                    let raw = &data[offsets[row]..offsets[row + 1]];
                    if raw.is_empty() && nullable {
                        b.append_null();
                    } else {
                        let s = unsafe { std::str::from_utf8_unchecked(raw) };
                        let v = arrow_cast::parse::parse_decimal::<
                            arrow_array::types::Decimal128Type,
                        >(s, *precision, *scale)?;
                        b.append_value(v);
                    }
                }
                Ok(Arc::new(b.finish()))
            }
            other => Err(ArrowError::NotYetImplemented(format!(
                "data type {other} not yet supported"
            ))),
        }
    }

    fn push_field_to_column(&mut self, col: usize, start: usize, end: usize) {
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

macro_rules! build_string_col {
    ($data:expr, $offsets:expr, $col:expr, $num_rows:expr, $nullable:expr, $builder:expr) => {{
        simdutf8::basic::from_utf8($data)
            .map_err(|e| ArrowError::ParseError(format!("invalid utf-8 in col {}: {}", $col, e)))?;

        let mut b = $builder;
        for row in 0..$num_rows {
            let s =
                unsafe { std::str::from_utf8_unchecked(&$data[$offsets[row]..$offsets[row + 1]]) };
            if s.is_empty() && $nullable {
                b.append_null();
            } else {
                b.append_value(s);
            }
        }
        Ok(Arc::new(b.finish()) as ArrayRef)
    }};
}
use build_string_col;

macro_rules! build_parsed_col {
    ($data:expr, $offsets:expr, $col:expr, $num_rows:expr, $nullable:expr, $builder:ty, $arrow_type:ty) => {{
        simdutf8::basic::from_utf8($data)
            .map_err(|e| ArrowError::ParseError(format!("invalid utf-8 in col {}: {}", $col, e)))?;

        let mut b = <$builder>::with_capacity($num_rows);
        for row in 0..$num_rows {
            let raw = &$data[$offsets[row]..$offsets[row + 1]];
            if raw.is_empty() && $nullable {
                b.append_null();
            } else {
                let s = unsafe { std::str::from_utf8_unchecked(raw) };
                let v = <$arrow_type as Parser>::parse(s).ok_or_else(|| {
                    ArrowError::ParseError(format!(
                        "cannot parse '{}' at row {}, col {}",
                        s, row, $col
                    ))
                })?;
                b.append_value(v);
            }
        }
        Ok(Arc::new(b.finish()) as ArrayRef)
    }};
}
use build_parsed_col;

macro_rules! build_binary_col {
    ($data:expr, $offsets:expr, $num_rows:expr, $nullable:expr, $builder:expr) => {{
        let mut b = $builder;
        for row in 0..$num_rows {
            let raw = &$data[$offsets[row]..$offsets[row + 1]];
            if raw.is_empty() && $nullable {
                b.append_null();
            } else {
                b.append_value(raw);
            }
        }
        Ok(Arc::new(b.finish()) as ArrayRef)
    }};
}
use build_binary_col;

macro_rules! build_int_col {
    ($data:expr, $offsets:expr, $col:expr, $num_rows:expr, $nullable:expr, $builder:ty, $native:ty) => {{
        let mut b = <$builder>::with_capacity($num_rows);
        for row in 0..$num_rows {
            let raw = &$data[$offsets[row]..$offsets[row + 1]];
            if raw.is_empty() && $nullable {
                b.append_null();
            } else {
                let v: $native = atoi::atoi(raw).ok_or_else(|| {
                    ArrowError::ParseError(format!(
                        "cannot parse as {} at row {}, col {}",
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
use build_int_col;

macro_rules! build_float_col {
    ($data:expr, $offsets:expr, $col:expr, $num_rows:expr, $nullable:expr, $builder:ty, $native:ty) => {{
        let mut b = <$builder>::with_capacity($num_rows);
        for row in 0..$num_rows {
            let raw = &$data[$offsets[row]..$offsets[row + 1]];
            if raw.is_empty() && $nullable {
                b.append_null();
            } else {
                let s = simdutf8::basic::from_utf8(raw).map_err(|e| {
                    ArrowError::ParseError(format!(
                        "invalid utf-8 at row {}, col {}: {}",
                        row, $col, e
                    ))
                })?;
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
use build_float_col;

#[inline(always)]
fn classify_one(chunk: &[u8], high_nibbles: u8x16, low_nibbles: u8x16) -> u8x16 {
    let v = u8x16::from_slice_unchecked(chunk);
    let (high, low) = v.nibbles();
    high_nibbles.classify(high) & low_nibbles.classify(low)
}

#[inline(always)]
fn build_u64_from_classified(v0: u8x16, v1: u8x16, v2: u8x16, v3: u8x16, broadcast: u8x16) -> u64 {
    let a = v0.eq(broadcast).bitset() as u64;
    let b = v1.eq(broadcast).bitset() as u64;
    let c = v2.eq(broadcast).bitset() as u64;
    let d = v3.eq(broadcast).bitset() as u64;
    a | (b << 16) | (c << 32) | (d << 48)
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

#[inline(always)]
fn peek_bit(current: u64, idx: usize, bitsets: &[u64]) -> Option<usize> {
    let mut current = current;
    let mut idx = idx;

    while current == 0 {
        idx += 1;

        if idx >= bitsets.len() {
            return None;
        }

        current = unsafe { *bitsets.get_unchecked(idx) };
    }

    Some(idx * 64 + current.trailing_zeros() as usize)
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
