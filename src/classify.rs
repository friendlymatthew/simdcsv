use crate::simd::{u8x16, u8x16x4};
use std::arch::aarch64::{
    uint8x16x4_t, vbslq_u8, vceqq_u8, vdupq_n_u8, vget_lane_u64, vld4q_u8, vqtbl4q_u8,
    vreinterpret_u64_s8, vreinterpretq_s16_u8, vshrn_n_s16,
};

pub const COMMA: u8 = 1;
pub const NEWLINE: u8 = 2;
pub const QUOTES: u8 = 4;

pub const LOW_NIBBLES: [u8; 16] = {
    let mut out = [0u8; 16];
    out[0x2] = QUOTES;
    out[0xA] = NEWLINE;
    out[0xC] = COMMA;
    out[0xD] = NEWLINE;

    out
};

pub const HIGH_NIBBLES: [u8; 16] = {
    let mut out = [0u8; 16];
    out[0x0] = NEWLINE;
    out[0x2] = COMMA | QUOTES;

    out
};

pub const BYTE_TABLE: [u8; 64] = {
    let mut out = [0u8; 64];
    out[0x0A] = NEWLINE;
    out[0x0D] = NEWLINE;
    out[0x2C] = COMMA;
    out[0x22] = QUOTES;
    out
};

pub struct ClassifyResult {
    pub comma_bitsets: Vec<u64>,
    pub newline_bitsets: Vec<u64>,
}

// classify structual characters and apply quote masking
// note: data must be a multiple of 64 bytes
pub fn classify_and_mask(data: &[u8]) -> ClassifyResult {
    let table = u8x16x4::from_slice_unchecked(&BYTE_TABLE);
    let comma_bc = u8x16::broadcast(COMMA);
    let newline_bc = u8x16::broadcast(NEWLINE);
    let quote_bc = u8x16::broadcast(QUOTES);
    let bitmask1 = u8x16::broadcast(0x55);
    let bitmask2 = u8x16::broadcast(0x33);

    let num_words = data.len() / 64;
    let mut comma_bitsets = Vec::with_capacity(num_words);
    let mut newline_bitsets = Vec::with_capacity(num_words);

    let mut carry = false;
    for chunk in data.chunks_exact(64) {
        let chunk: &[u8; 64] = chunk.try_into().unwrap();
        let (classified) = classify_four_lanes(chunk, table);

        let mut comma = build_eq_bitset(classified, comma_bc, bitmask1, bitmask2);
        let mut newline = build_eq_bitset(classified, newline_bc, bitmask1, bitmask2);
        let quote = build_eq_bitset(classified, quote_bc, bitmask1, bitmask2);

        let inside = unsafe { std::arch::aarch64::vmull_p64(quote, 0xFFFFFFFFFFFFFFFF_u64) } as u64;
        let outside = if carry { inside } else { !inside };
        carry ^= (quote.count_ones() & 1) != 0;

        comma &= outside;
        newline &= outside;

        comma_bitsets.push(comma);
        newline_bitsets.push(newline);
    }

    ClassifyResult {
        comma_bitsets,
        newline_bitsets,
    }
}

#[inline(always)]
pub fn classify_four_lanes(chunk: &[u8; 64], byte_table: u8x16x4) -> u8x16x4 {
    unsafe {
        let v = vld4q_u8(chunk.as_ptr());
        uint8x16x4_t(
            vqtbl4q_u8(byte_table.into(), v.0),
            vqtbl4q_u8(byte_table.into(), v.1),
            vqtbl4q_u8(byte_table.into(), v.2),
            vqtbl4q_u8(byte_table.into(), v.3),
        )
        .into()
    }
}

#[inline(always)]
pub fn build_eq_bitset(c: u8x16x4, bc: u8x16, bitmask1: u8x16, bitmask2: u8x16) -> u64 {
    let raw: uint8x16x4_t = c.into();
    unsafe {
        let t0 = vbslq_u8(
            bitmask1.into(),
            vceqq_u8(raw.0, bc.into()),
            vceqq_u8(raw.1, bc.into()),
        ); // 01010101...
        let t1 = vbslq_u8(
            bitmask1.into(),
            vceqq_u8(raw.2, bc.into()),
            vceqq_u8(raw.3, bc.into()),
        ); // 23232323...
        let combined = vbslq_u8(bitmask2.into(), t0, t1); // 01230123...
        let sum = vshrn_n_s16::<4>(vreinterpretq_s16_u8(combined));
        vget_lane_u64::<0>(vreinterpret_u64_s8(sum))
    }
}
