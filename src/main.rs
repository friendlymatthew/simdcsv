use simdcsv::read;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args().nth(1).expect("expect .csv file path");
    let data = std::fs::read(path)?;
    let rows = read(&data);

    println!("{:?}", rows);

    Ok(())
}
