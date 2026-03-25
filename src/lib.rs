#![warn(clippy::nursery)]

mod classify;
mod read;
mod simd;

pub use read::*;
pub(crate) use simd::*;
