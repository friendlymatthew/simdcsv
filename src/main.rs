use anyhow::anyhow;
use simdcsv::reader::CsvReader;

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);

    let data = args.next().ok_or_else(|| anyhow!("No argument passed"))?;

    let mut reader = CsvReader::new(data.as_bytes());
    let rows = reader.read();

    println!("{:?}", rows);

    Ok(())
}
