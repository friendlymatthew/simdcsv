use std::sync::Arc;

use arrow_csv2::ReaderBuilder;
use arrow_schema::{DataType, Field, Schema};

fn taxi_zone_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("LocationID", DataType::Utf8, true),
        Field::new("Borough", DataType::Utf8, true),
        Field::new("Zone", DataType::Utf8, true),
        Field::new("service_zone", DataType::Utf8, true),
    ]))
}

fn decode_all(
    raw: &[u8],
    schema: Arc<Schema>,
    build: impl Fn(Arc<Schema>) -> arrow_csv2::Decoder,
) -> Vec<arrow_array::RecordBatch> {
    let mut decoder = build(schema);
    let mut offset = 0;
    let mut batches = Vec::new();
    loop {
        let consumed = decoder.decode(&raw[offset..]).unwrap();
        offset += consumed;
        if consumed == 0 || decoder.capacity() == 0 {
            if let Some(batch) = decoder.flush().unwrap() {
                batches.push(batch);
            }
            if consumed == 0 && decoder.capacity() > 0 {
                break;
            }
        }
    }
    batches
}

fn decode_all_arrow_csv(
    raw: &[u8],
    schema: Arc<Schema>,
) -> Vec<arrow_array::RecordBatch> {
    let mut decoder = arrow_csv::ReaderBuilder::new(schema)
        .with_header(true)
        .with_batch_size(8192)
        .build_decoder();

    let mut offset = 0;
    let mut batches = Vec::new();
    loop {
        let consumed = decoder.decode(&raw[offset..]).unwrap();
        offset += consumed;
        if consumed == 0 || decoder.capacity() == 0 {
            if let Some(batch) = decoder.flush().unwrap() {
                batches.push(batch);
            }
            if consumed == 0 {
                break;
            }
        }
    }
    batches
}

#[test]
fn arrow_csv2_matches_arrow_csv() {
    let raw = std::fs::read("taxi_zone_lookup.csv").expect("missing csv");
    let schema = taxi_zone_schema();

    let ours = decode_all(&raw, schema.clone(), |s| {
        ReaderBuilder::new(s)
            .with_header(true)
            .with_batch_size(8192)
            .build_decoder()
    });

    let theirs = decode_all_arrow_csv(&raw, schema);

    // same number of batches
    assert_eq!(ours.len(), theirs.len(), "batch count mismatch");

    for (i, (a, b)) in ours.iter().zip(&theirs).enumerate() {
        assert_eq!(a.num_rows(), b.num_rows(), "row count mismatch in batch {i}");
        assert_eq!(
            a.num_columns(),
            b.num_columns(),
            "column count mismatch in batch {i}"
        );

        for col in 0..a.num_columns() {
            let col_a = a
                .column(col)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            let col_b = b
                .column(col)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();

            for row in 0..a.num_rows() {
                assert_eq!(
                    col_a.value(row),
                    col_b.value(row),
                    "mismatch at batch {i}, col {col}, row {row}"
                );
            }
        }
    }
}
