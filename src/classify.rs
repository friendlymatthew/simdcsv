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

// note: data must be a multiple of 16
pub fn classify(data: &[u8]) -> Vec<u8x16> {
    let low_nibbles = u8x16::from_slice_unchecked(&LOW_NIBBLES);
    let high_nibbles = u8x16::from_slice_unchecked(&HIGH_NIBBLES);

    let chunks = data.chunks_exact(u8x16::LANE_COUNT);

    let mut out = Vec::with_capacity(data.len() / 16);

    for chunk in chunks {
        let v = u8x16::from_slice_unchecked(chunk);
        let (high, low) = v.nibbles();

        let res = high_nibbles.classify(high) & low_nibbles.classify(low);
        out.push(res);
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_classify() {
        let mut data = b"a,b,c\nf,\"g\"".to_vec();
        data.resize(data.len().next_multiple_of(16), 0);

        let bitsets = classify(&data);

        assert_eq!(bitsets.len(), 1);
        let res: [u8; 16] = bitsets[0].into();

        assert_eq!(
            res,
            [
                0, COMMA, 0, COMMA, 0, NEWLINE, 0, COMMA, QUOTES, 0, QUOTES, 0, 0, 0, 0, 0,
            ]
        );
    }
}
