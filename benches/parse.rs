use criterion::{Criterion, criterion_group, criterion_main};

fn bench_clickbench(c: &mut Criterion) {
    let raw = std::fs::read("hits_100mb.csv").expect("hits_100mb.csv not found");

    let schema = arrow_csv2::clickbench::schema();

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
