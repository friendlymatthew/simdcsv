use arrow_csv2::ReaderBuilder;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let raw = std::fs::read("hits_100mb.csv").expect("not found");

    let schema = arrow_csv2::clickbench::schema();

    let reader = ReaderBuilder::new(schema)
        .with_batch_size(8192)
        .build_buffered(raw.as_slice());

    let _ = reader.collect::<Result<Vec<_>, _>>()?;

    Ok(())
}
