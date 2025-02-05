pub const COMMA_CLASS: u8 = 1;
pub const NEW_LINE_CLASS: u8 = 2;

pub const QUOTATION_CLASS: u8 = 3;

pub const LO_LOOKUP: [u8; 16] = [
    0,
    0,
    QUOTATION_CLASS,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    NEW_LINE_CLASS,
    0,
    COMMA_CLASS,
    NEW_LINE_CLASS,
    0,
    0,
];
pub const HI_LOOKUP: [u8; 16] = [
    NEW_LINE_CLASS,
    0,
    COMMA_CLASS | QUOTATION_CLASS,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
];

use crate::u8x16::u8x16;

#[derive(Debug)]
pub struct CsvClassifier<'a> {
    cursor: usize,
    data: &'a [u8],
}

impl<'a> CsvClassifier<'a> {
    pub const fn new(data: &'a [u8]) -> Self {
        Self { cursor: 0, data }
    }

    pub fn classify(&mut self) -> Vec<u8x16> {
        let mut bitsets = Vec::new();

        let high_nibble_lookup = u8x16::from_slice_unchecked(&HI_LOOKUP);
        let low_nibble_lookup = u8x16::from_slice_unchecked(&LO_LOOKUP);

        while self.cursor < self.data.len() {
            let (lanes, _aligned) = self.load_u8x16();

            let (hi_lanes, lo_lanes) = lanes.nibbles();
            let res = high_nibble_lookup.classify(hi_lanes) & low_nibble_lookup.classify(lo_lanes);

            bitsets.push(res);
        }

        bitsets
    }

    fn load_u8x16(&mut self) -> (u8x16, bool) {
        if self.cursor + u8x16::LANE_COUNT < self.data.len() {
            let slice = &self.data[self.cursor..self.cursor + u8x16::LANE_COUNT];
            self.cursor += u8x16::LANE_COUNT;

            return (u8x16::from_slice_unchecked(slice), true);
        }

        let slice = &self.data[self.cursor..];
        self.cursor = self.data.len();

        let mut temp = [0u8; 16];
        temp[..slice.len()].copy_from_slice(slice);

        let last = self.data[self.data.len() - 1];

        if last != 0x0A && last != 0x0D {
            temp[slice.len()] = 0x0A;
        }

        (u8x16::from_slice_unchecked(&temp), false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_classify() {
        let mut classifier = CsvClassifier::new(b"a,b,c\nf,\"g\"");
        let bitsets = classifier.classify();

        assert_eq!(bitsets.len(), 1);
        let res: [u8; 16] = bitsets[0].into();

        assert_eq!(
            res,
            [
                0,
                COMMA_CLASS,
                0,
                COMMA_CLASS,
                0,
                NEW_LINE_CLASS,
                0,
                COMMA_CLASS,
                QUOTATION_CLASS,
                0,
                QUOTATION_CLASS,
                NEW_LINE_CLASS, // we always add a \n at the end
                0,
                0,
                0,
                0,
            ]
        );
    }
}
