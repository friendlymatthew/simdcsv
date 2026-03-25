use simdcsv::read;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args().nth(1).expect("expect .csv file path");
    let mut data = std::fs::read(path)?;
    let rows = read(&mut data);

    println!("{:?}", rows);

    Ok(())
}
