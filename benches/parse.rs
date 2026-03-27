use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};

const NUM_COLUMNS: usize = 105;
fn clickbench_schema() -> Arc<Schema> {
    Arc::new(Schema::new(
        (0..NUM_COLUMNS)
            .map(|i| Field::new(format!("c{i}"), DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ))
}

fn bench_clickbench(c: &mut Criterion) {
    let raw = std::fs::read("hits_100mb.csv")
        .expect("hits_100mb.csv not found, run: cargo run --release --bin slice_clickbench");

    let schema = clickbench_schema();

    c.bench_function("arrow-csv2::Decoder (clickbench 100MB)", |b| {
        b.iter(|| {
            let mut decoder = arrow_csv2::ReaderBuilder::new(schema.clone())
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
                    if consumed == 0 && decoder.capacity() > 0 {
                        break;
                    }
                }
            }
            batches
        });
    });

    c.bench_function("arrow-csv2::Reader (clickbench 100MB)", |b| {
        b.iter(|| {
            let reader = arrow_csv2::ReaderBuilder::new(schema.clone())
                .with_batch_size(8192)
                .build_buffered(raw.as_slice());

            reader.collect::<Result<Vec<_>, _>>().unwrap()
        });
    });

    c.bench_function("arrow-csv::Reader (clickbench 100MB)", |b| {
        b.iter(|| {
            let reader = arrow_csv::ReaderBuilder::new(schema.clone())
                .with_batch_size(8192)
                .build_buffered(raw.as_slice())
                .unwrap();

            reader.collect::<Result<Vec<_>, _>>().unwrap()
        });
    });
}

criterion_group!(benches, bench_clickbench);
criterion_main!(benches);
