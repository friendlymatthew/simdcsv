use std::arch::aarch64::{
    uint8x16_t, vandq_u8, vceqq_u8, vdupq_n_u8, vld1q_u8, vqtbl1q_u8, vshrq_n_u8, vst1q_u8,
};
use std::fmt::{Debug, Formatter};
use std::ops::BitAnd;

#[allow(non_camel_case_types)]
#[derive(Copy, Clone)]
pub struct u8x16(uint8x16_t);

impl u8x16 {
    pub const LANE_COUNT: usize = 16;

    /// warning! this assumes slice has 16 bytes
    /// will panic if slice is not 16 bytes
    pub fn from_slice_unchecked(slice: &[u8]) -> Self {
        assert_eq!(slice.len(), 16);

        unsafe { vld1q_u8(slice.as_ptr()) }.into()
    }

    pub fn broadcast(value: u8) -> Self {
        unsafe { vdupq_n_u8(value) }.into()
    }

    pub fn nibbles(&self) -> (Self, Self) {
        let inner = self.0;

        unsafe {
            let mask = vdupq_n_u8(0x0F);
            (
                vandq_u8(vshrq_n_u8::<4>(inner), mask).into(),
                vandq_u8(inner, mask).into(),
            )
        }
    }

    // Call from the lookup table
    pub fn classify(&self, values: Self) -> Self {
        unsafe { vqtbl1q_u8(self.0, values.0) }.into()
    }

    pub fn eq(&self, other: u8x16) -> Self {
        unsafe { vceqq_u8(self.0, other.0) }.into()
    }

    // figure out a better way to do this
    // maybe just get the MSB
    pub fn bitset(self) -> u16 {
        let bs: [u8; 16] = self.into();

        let mut mask = 0u16;

        for (i, b) in bs.into_iter().enumerate() {
            mask |= ((b != 0) as u16) << (15 - i);
        }

        mask
    }
}

impl BitAnd for u8x16 {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        unsafe { vandq_u8(self.0, rhs.0) }.into()
    }
}

impl From<uint8x16_t> for u8x16 {
    fn from(value: uint8x16_t) -> Self {
        Self(value)
    }
}

impl From<[u8; 16]> for u8x16 {
    fn from(value: [u8; 16]) -> Self {
        Self::from_slice_unchecked(&value)
    }
}

impl From<u8x16> for [u8; 16] {
    fn from(value: u8x16) -> Self {
        let mut temp = [0u8; 16];
        unsafe {
            vst1q_u8(temp.as_mut_ptr(), value.0);
        }

        temp
    }
}

impl Debug for u8x16 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let temp: [u8; 16] = (*self).into();

        f.debug_tuple("u8x16").field(&temp).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_slice() {
        let slice = &[1; 16];
        let lanes: [u8; 16] = u8x16::from_slice_unchecked(slice).into();

        assert_eq!(lanes, [1; 16])
    }

    #[test]
    fn test_broadcast() {
        let ones: [u8; 16] = u8x16::from_slice_unchecked(&[10; 16]).into();
        assert_eq!(ones, [10; 16]);
    }

    #[test]
    fn test_nibbles() {
        let slice =
            u8x16::from_slice_unchecked(&[0x2c, 0xD, 0xA, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

        let (hi, lo) = slice.nibbles();

        let hi: [u8; 16] = hi.into();
        let lo: [u8; 16] = lo.into();

        assert_eq!(hi, [2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(lo, [0xC, 0xD, 0xA, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    }
}
