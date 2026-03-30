#![warn(clippy::nursery)]

mod arrow;
mod classify;
pub mod clickbench;
mod decoder;
mod monoid;
mod parallel_csv_opener;
pub mod partition;
mod reader;
mod reader_builder;
mod simd;

pub use decoder::Decoder;
pub use parallel_csv_opener::ParallelCsvSource;
pub use reader::*;
pub use reader_builder::*;
