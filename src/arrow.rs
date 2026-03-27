use std::sync::Arc;

#[cfg(debug_assertions)]
use arrow_array::Array;
use arrow_array::builder::{
    BinaryBuilder, BinaryViewBuilder, BooleanBuilder, Date32Builder, Date64Builder,
    Decimal128Builder, Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder,
    Int64Builder, LargeBinaryBuilder, LargeStringBuilder, StringViewBuilder,
    TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
    TimestampSecondBuilder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow_array::{ArrayRef, StringArray};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_cast::parse::Parser;
use arrow_schema::{ArrowError, DataType, Field, TimeUnit};

pub fn build_column(
    field: &Field,
    data: Vec<u8>,
    offsets: Vec<usize>,
    col: usize,
    num_rows: usize,
) -> Result<ArrayRef, ArrowError> {
    let nullable = field.is_nullable();

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
        DataType::Utf8 => {
            simdutf8::basic::from_utf8(&data)
                .map_err(|e| ArrowError::ParseError(format!("invalid utf-8 in col {col}: {e}")))?;

            let nulls = if nullable {
                let mut null_count = 0;
                let valid = (0..num_rows)
                    .map(|row| {
                        let is_valid = offsets[row] != offsets[row + 1];
                        if !is_valid {
                            null_count += 1;
                        }
                        is_valid
                    })
                    .collect::<Vec<_>>();

                (null_count > 0).then_some(NullBuffer::from(valid))
            } else {
                None
            };

            let i32_offsets = offsets.iter().map(|&o| o as i32).collect::<Vec<_>>();

            // safety: offsets are monotonically non-decreasing and the last offset == data.len() by construction
            // push_feild_to_column only appends to col_data and pushes col_data.len() as the next offset
            let offset_buffer =
                unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(i32_offsets)) };

            let value_buffer = Buffer::from_vec(data);

            // safety: utf8 validated and offset invariants hold
            let array = unsafe { StringArray::new_unchecked(offset_buffer, value_buffer, nulls) };

            #[cfg(debug_assertions)]
            array
                .to_data()
                .validate_full()
                .expect("invalid stringarray");

            Ok(Arc::new(array))
        }
        DataType::LargeUtf8 => build_string_col!(
            &data,
            &offsets,
            col,
            num_rows,
            nullable,
            LargeStringBuilder::with_capacity(num_rows, data.len())
        ),
        DataType::Utf8View => build_string_col!(
            &data,
            &offsets,
            col,
            num_rows,
            nullable,
            StringViewBuilder::with_capacity(num_rows)
        ),
        DataType::Binary => build_binary_col!(
            &data,
            &offsets,
            num_rows,
            nullable,
            BinaryBuilder::with_capacity(num_rows, data.len())
        ),
        DataType::LargeBinary => build_binary_col!(
            &data,
            &offsets,
            num_rows,
            nullable,
            LargeBinaryBuilder::with_capacity(num_rows, data.len())
        ),
        DataType::BinaryView => build_binary_col!(
            &data,
            &offsets,
            num_rows,
            nullable,
            BinaryViewBuilder::with_capacity(num_rows)
        ),
        DataType::Int8 => {
            build_int_col!(&data, &offsets, col, num_rows, nullable, Int8Builder, i8)
        }
        DataType::Int16 => {
            build_int_col!(&data, &offsets, col, num_rows, nullable, Int16Builder, i16)
        }
        DataType::Int32 => {
            build_int_col!(&data, &offsets, col, num_rows, nullable, Int32Builder, i32)
        }
        DataType::Int64 => {
            build_int_col!(&data, &offsets, col, num_rows, nullable, Int64Builder, i64)
        }
        DataType::UInt8 => {
            build_int_col!(&data, &offsets, col, num_rows, nullable, UInt8Builder, u8)
        }
        DataType::UInt16 => {
            build_int_col!(&data, &offsets, col, num_rows, nullable, UInt16Builder, u16)
        }
        DataType::UInt32 => {
            build_int_col!(&data, &offsets, col, num_rows, nullable, UInt32Builder, u32)
        }
        DataType::UInt64 => {
            build_int_col!(&data, &offsets, col, num_rows, nullable, UInt64Builder, u64)
        }
        DataType::Float32 => {
            build_float_col!(
                &data,
                &offsets,
                col,
                num_rows,
                nullable,
                Float32Builder,
                f32
            )
        }
        DataType::Float64 => {
            build_float_col!(
                &data,
                &offsets,
                col,
                num_rows,
                nullable,
                Float64Builder,
                f64
            )
        }
        DataType::Date32 => {
            build_parsed_col!(
                &data,
                &offsets,
                col,
                num_rows,
                nullable,
                Date32Builder,
                arrow_array::types::Date32Type
            )
        }
        DataType::Date64 => {
            build_parsed_col!(
                &data,
                &offsets,
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
                    &data,
                    &offsets,
                    col,
                    num_rows,
                    nullable,
                    TimestampSecondBuilder,
                    arrow_array::types::TimestampSecondType
                )
            }
            TimeUnit::Millisecond => {
                build_parsed_col!(
                    &data,
                    &offsets,
                    col,
                    num_rows,
                    nullable,
                    TimestampMillisecondBuilder,
                    arrow_array::types::TimestampMillisecondType
                )
            }
            TimeUnit::Microsecond => {
                build_parsed_col!(
                    &data,
                    &offsets,
                    col,
                    num_rows,
                    nullable,
                    TimestampMicrosecondBuilder,
                    arrow_array::types::TimestampMicrosecondType
                )
            }
            TimeUnit::Nanosecond => {
                build_parsed_col!(
                    &data,
                    &offsets,
                    col,
                    num_rows,
                    nullable,
                    TimestampNanosecondBuilder,
                    arrow_array::types::TimestampNanosecondType
                )
            }
        },
        DataType::Decimal128(precision, scale) => {
            simdutf8::basic::from_utf8(&data)
                .map_err(|e| ArrowError::ParseError(format!("invalid utf-8 in col {col}: {e}")))?;

            let mut b = Decimal128Builder::with_capacity(num_rows)
                .with_data_type(DataType::Decimal128(*precision, *scale));
            for row in 0..num_rows {
                let raw = &data[offsets[row]..offsets[row + 1]];
                if raw.is_empty() && nullable {
                    b.append_null();
                } else {
                    let s = unsafe { std::str::from_utf8_unchecked(raw) };
                    let v = arrow_cast::parse::parse_decimal::<arrow_array::types::Decimal128Type>(
                        s, *precision, *scale,
                    )?;
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
