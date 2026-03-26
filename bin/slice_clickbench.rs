use std::io::Read;

const BENCH_BYTES: usize = 100 * 1024 * 1024;

fn main() {
    let mut f =
        std::fs::File::open("hits.csv").expect("hits.csv not found. run ./download_clickbench.sh");

    let mut buf = vec![0u8; BENCH_BYTES];
    f.read_exact(&mut buf).expect("hits.csv too small");

    let end = buf.iter().rposition(|&b| b == b'\n').unwrap_or(buf.len());
    buf.truncate(end + 1);

    std::fs::write("hits_100mb.csv", &buf).expect("failed to write");
}
