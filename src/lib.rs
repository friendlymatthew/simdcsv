#![warn(clippy::nursery)]

mod classify;
mod read;
mod simd;

mod decoder;
mod reader_builder;

pub use decoder::*;
pub use read::*;
pub use reader_builder::*;
pub(crate) use simd::*;
