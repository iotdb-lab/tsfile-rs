use std::sync::Arc;

use crate::file::metadata::{ChunkMetadata, TimeseriesMetadata};
use crate::file::reader::{ChunkReader, RowIter, SectionReader, SensorReader};

#[derive(Debug)]
pub struct TsFileSensorReader<R: SectionReader> {
    reader: Arc<R>,
    meta: Vec<TimeseriesMetadata>,
}

impl<R: SectionReader> TsFileSensorReader<R> {
    pub fn new(reader: Arc<R>, meta: Vec<TimeseriesMetadata>) -> Self {
        Self {
            reader,
            meta,
        }
    }
}

impl<R: SectionReader> SensorReader for TsFileSensorReader<R> {
    fn metadata(&self) -> &Vec<TimeseriesMetadata> {
        &self.meta
    }

    fn get_chunk_reader(&self, i: usize) -> crate::error::Result<Box<dyn ChunkReader>> {
        todo!()
    }

    fn get_page_iter(&self, predicate: Box<dyn Fn(u64) -> bool>) -> crate::error::Result<RowIter> {
        todo!()
    }
}


pub struct DefaultChunkReader {}

impl ChunkReader for DefaultChunkReader {}