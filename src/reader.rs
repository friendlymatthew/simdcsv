use std::io::BufRead;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};

use crate::Decoder;

pub type Reader<R> = BufReader<std::io::BufReader<R>>;

#[derive(Debug)]
pub struct BufReader<R> {
    reader: R,
    decoder: Decoder,
    exhausted: bool,
    remainder: Vec<u8>,
}

impl<R: BufRead> BufReader<R> {
    pub(crate) const fn new(reader: R, decoder: Decoder) -> Self {
        Self {
            reader,
            decoder,
            exhausted: false,
            remainder: Vec::new(),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.decoder.schema()
    }

    pub fn into_inner(self) -> R {
        self.reader
    }

    fn read(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        loop {
            if self.exhausted {
                self.decoder.decode(&[])?;
                return self.decoder.flush();
            }

            let buf = self.reader.fill_buf()?;
            if buf.is_empty() {
                self.exhausted = true;
                continue;
            }

            if self.remainder.is_empty() {
                let consumed = self.decoder.decode(buf)?;
                if consumed > 0 {
                    self.reader.consume(consumed);
                } else {
                    self.remainder.extend_from_slice(buf);
                    let len = buf.len();
                    self.reader.consume(len);
                }
            } else {
                self.remainder.extend_from_slice(buf);
                let len = buf.len();
                self.reader.consume(len);

                let consumed = self.decoder.decode(&self.remainder)?;
                if consumed > 0 {
                    self.remainder.drain(..consumed);
                }
            }

            if self.decoder.capacity() == 0 {
                return self.decoder.flush();
            }
        }
    }
}

impl<R: BufRead> Iterator for BufReader<R> {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read().transpose()
    }
}
