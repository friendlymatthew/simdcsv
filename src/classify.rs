use crate::u8x16;

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

pub struct ClassifyResult {
    pub comma_bitsets: Vec<u64>,
    pub newline_bitsets: Vec<u64>,
}

// classify structual characters and apply quote masking
// note: data must be a multiple of 64 bytes
pub fn classify_and_mask(data: &[u8]) -> ClassifyResult {
    let low_nib = u8x16::from_slice_unchecked(&LOW_NIBBLES);
    let high_nib = u8x16::from_slice_unchecked(&HIGH_NIBBLES);
    let comma_bc = u8x16::broadcast(COMMA);
    let newline_bc = u8x16::broadcast(NEWLINE);
    let quote_bc = u8x16::broadcast(QUOTES);

    let num_words = data.len() / 64;
    let mut comma_bitsets = Vec::with_capacity(num_words);
    let mut newline_bitsets = Vec::with_capacity(num_words);

    let mut carry = false;
    for chunk in data.chunks_exact(64) {
        let (c0, c1, c2, c3) = classify_four_lanes(chunk, high_nib, low_nib);

        let mut comma = build_eq_bitset(c0, c1, c2, c3, comma_bc);
        let mut newline = build_eq_bitset(c0, c1, c2, c3, newline_bc);
        let quote = build_eq_bitset(c0, c1, c2, c3, quote_bc);

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
fn classify_four_lanes(
    chunk: &[u8],
    high_nib: u8x16,
    low_nib: u8x16,
) -> (u8x16, u8x16, u8x16, u8x16) {
    let classify_one = |slice: &[u8]| {
        let v = u8x16::from_slice_unchecked(slice);
        let (h, l) = v.nibbles();
        high_nib.classify(h) & low_nib.classify(l)
    };

    (
        classify_one(&chunk[0..16]),
        classify_one(&chunk[16..32]),
        classify_one(&chunk[32..48]),
        classify_one(&chunk[48..64]),
    )
}

#[inline(always)]
fn build_eq_bitset(v0: u8x16, v1: u8x16, v2: u8x16, v3: u8x16, bc: u8x16) -> u64 {
    let a = v0.eq(bc).bitset() as u64;
    let b = v1.eq(bc).bitset() as u64;
    let c = v2.eq(bc).bitset() as u64;
    let d = v3.eq(bc).bitset() as u64;

    a | (b << 16) | (c << 32) | (d << 48)
}
