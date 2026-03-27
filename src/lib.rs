#![warn(clippy::nursery)]

mod classify;
mod decoder;
mod read;
mod reader;
mod reader_builder;
mod simd;

pub use decoder::*;
pub use read::*;
pub use reader::*;
pub use reader_builder::*;
pub(crate) use simd::*;
