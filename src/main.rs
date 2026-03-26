use std::sync::Arc;

use arrow_csv2::ReaderBuilder;
use arrow_schema::{DataType, Field, Schema};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args().nth(1).expect("expect .csv file path");
    let raw = std::fs::read(path)?;

    let num_columns = raw
        .iter()
        .position(|&b| b == b'\n')
        .map(|nl| raw[..nl].iter().filter(|&&b| b == b',').count() + 1)
        .unwrap_or(1);

    let schema = Arc::new(Schema::new(
        (0..num_columns)
            .map(|i| Field::new(format!("c{i}"), DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));

    let mut decoder = ReaderBuilder::new(schema)
        .with_batch_size(8192)
        .build_decoder();

    let mut offset = 0;
    loop {
        let consumed = decoder.decode(&raw[offset..])?;
        offset += consumed;
        if consumed == 0 || decoder.capacity() == 0 {
            if let Some(batch) = decoder.flush()? {
                std::hint::black_box(batch.num_rows());
            }
            if consumed == 0 && decoder.capacity() > 0 {
                break;
            }
        }
    }

    Ok(())
}
