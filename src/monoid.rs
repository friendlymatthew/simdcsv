#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ClassifyResult {
    pub newlines_outside: Vec<u64>,
    pub newlines_inside: Vec<u64>,
    pub quote_parity: bool,
}

#[cfg(test)]
pub trait Monoid: Sized {
    fn identity() -> Self;
    fn combine(&self, rhs: &Self) -> Self;
}

#[cfg(test)]
impl Monoid for ClassifyResult {
    fn identity() -> Self {
        Self {
            newlines_outside: Vec::new(),
            newlines_inside: Vec::new(),
            quote_parity: false,
        }
    }

    fn combine(&self, rhs: &Self) -> Self {
        let (rhs_out, rhs_in) = if self.quote_parity {
            (&rhs.newlines_inside, &rhs.newlines_outside)
        } else {
            (&rhs.newlines_outside, &rhs.newlines_inside)
        };

        let mut out = self.newlines_outside.clone();
        out.extend_from_slice(rhs_out);

        let mut ins = self.newlines_inside.clone();
        ins.extend_from_slice(rhs_in);

        Self {
            newlines_outside: out,
            newlines_inside: ins,
            quote_parity: self.quote_parity ^ rhs.quote_parity,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    struct QuoteParity(bool);

    impl From<bool> for QuoteParity {
        fn from(b: bool) -> Self {
            Self(b)
        }
    }

    impl Monoid for QuoteParity {
        fn identity() -> Self {
            Self(false)
        }

        fn combine(&self, rhs: &Self) -> Self {
            Self(self.0 ^ rhs.0)
        }
    }

    #[test]
    fn test_quote_parity_laws_exhaustive() {
        let vals = [false.into(), true.into()];

        for &a in &vals {
            assert_eq!(QuoteParity::identity().combine(&a), a);
            assert_eq!(a.combine(&QuoteParity::identity()), a);

            for &b in &vals {
                for &c in &vals {
                    assert_eq!(a.combine(&b).combine(&c), a.combine(&b.combine(&c)));
                }
            }
        }
    }
}
