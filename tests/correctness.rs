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

#[test]
fn taxi_zone_matches_arrow_csv() {
    let raw = std::fs::read("taxi_zone_lookup.csv").expect("missing csv");
    let schema = taxi_zone_schema();

    let ours = ReaderBuilder::new(schema.clone())
        .with_header(true)
        .with_batch_size(8192)
        .build(raw.as_slice())
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();

    let theirs = arrow_csv::ReaderBuilder::new(schema)
        .with_header(true)
        .with_batch_size(8192)
        .build(raw.as_slice())
        .unwrap()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();

    assert_eq!(ours.len(), theirs.len());

    for (a, b) in ours.iter().zip(&theirs) {
        assert_eq!(a, b);
    }
}

#[test]
fn clickbench_matches_arrow_csv() {
    let raw = std::fs::read("hits_100mb.csv").unwrap();
    let schema = arrow_csv2::clickbench::schema();

    let ours = ReaderBuilder::new(schema.clone())
        .with_batch_size(8192)
        .build(raw.as_slice())
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();

    let theirs = arrow_csv::ReaderBuilder::new(schema)
        .with_batch_size(8192)
        .build(raw.as_slice())
        .unwrap()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();

    assert_eq!(ours.len(), theirs.len());

    for (a, b) in ours.iter().zip(&theirs) {
        assert_eq!(a.num_rows(), b.num_rows());
        assert_eq!(a, b);
    }
}
