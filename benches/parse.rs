use criterion::{Criterion, criterion_group, criterion_main};
use simdcsv::read;

fn parse_majestic_million(c: &mut Criterion) {
    let raw = std::fs::read("majestic_million.csv").expect("csv not found");

    c.bench_function("parse majestic_million.csv", |b| {
        b.iter(|| {
            let mut data = raw.clone();
            read(&mut data)
        });
    });
}

criterion_group!(benches, parse_majestic_million);
criterion_main!(benches);
