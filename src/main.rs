use anyhow::anyhow;
use simdcsv::read;

fn main() -> anyhow::Result<()> {
    let path = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow!("pass in a .csv file path"))?;

    let data = std::fs::read(path)?;

    let rows = read(&data);

    println!("{:?}", rows);

    Ok(())
}
