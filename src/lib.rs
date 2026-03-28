#![warn(clippy::nursery)]

mod arrow;
mod classify;
mod decoder;
mod reader;
mod reader_builder;
mod simd;

pub use decoder::*;
pub use reader::*;
pub use reader_builder::*;
pub(crate) use simd::*;
