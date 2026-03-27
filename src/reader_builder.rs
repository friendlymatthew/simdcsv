use std::io::{BufRead, Read};

use arrow_schema::SchemaRef;

use crate::Decoder;
use crate::reader;

#[derive(Debug)]
pub struct ReaderBuilder {
    schema: SchemaRef,
    batch_size: usize,
    // these are not included in v1
    // bounds: Bounds,
    // projection: Option<Vec<usize>>,
    has_header: bool,
    delimiter: u8,
}

impl ReaderBuilder {
    pub const fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            batch_size: 1_024,
            has_header: false,
            delimiter: b',',
        }
    }

    pub const fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub const fn with_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    pub const fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    pub fn build<R: Read>(self, reader: R) -> reader::Reader<R> {
        self.build_buffered(std::io::BufReader::new(reader))
    }

    pub fn build_buffered<R: BufRead>(self, reader: R) -> reader::BufReader<R> {
        reader::BufReader::new(reader, self.build_decoder())
    }

    pub fn build_decoder(self) -> Decoder {
        Decoder::new(self.schema, self.batch_size, self.has_header)
    }
}
