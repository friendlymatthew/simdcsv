use std::sync::Arc;

use arrow_csv2::ReaderBuilder;
use arrow_schema::{DataType, Field, Schema};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args().nth(1).expect("expect .csv file path");
    let file = std::fs::File::open(&path)?;

    let raw = std::fs::read(&path)?;
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

    let reader = ReaderBuilder::new(schema).with_batch_size(8192).build(file);

    let _ = reader.collect::<Result<Vec<_>, _>>()?;

    Ok(())
}
