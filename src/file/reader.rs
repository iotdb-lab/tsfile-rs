use std::fs::File;
use std::io;
use std::io::{Cursor, Read};
use std::sync::Arc;

use snafu::{ResultExt, Snafu};

use crate::chunk::reader::PageHeader;
use crate::encoding::decoder::Field;
use crate::file::metadata::{
    ChunkMetadata, MetadataIndexNodeType, TimeseriesMetadata, TsFileMetadata,
};
use crate::utils::io::FileSource;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to read fixed length {} data: {}", len, source))]
    ReadFixedLength { len: usize, source: io::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub trait Length {
    fn len(&self) -> u64;
}

pub trait TryClone: Sized {
    fn try_clone(&self) -> std::io::Result<Self>;
}

pub trait SectionReader: Length {
    type T: Read;
    fn get_read(&self, start: u64, len: usize) -> Result<Self::T>;
    fn get_cursor(&self, start: u64, len: usize) -> Result<Cursor<Vec<u8>>>;
}

pub trait FileReader {
    fn metadata(&self) -> &TsFileMetadata;
    fn device_meta_iter(&self) -> Box<dyn DeviceMetadataIter<Item = MetadataIndexNodeType>>;
    fn get_device_reader();
    fn sensor_meta_iter(
        &self,
        device: &str,
    ) -> Box<dyn SensorMetadataIter<Item = TimeseriesMetadata>>;

    fn get_sensor_reader(&self, device: &str, sensor: &str) -> Option<Box<dyn SensorReader>>;
}

pub trait DeviceMetadataIter: Iterator {}

pub trait SensorMetadataIter: Iterator {}

pub trait DeviceReader {
    fn metadata(&self) -> Vec<TimeseriesMetadata>;

    fn get_sensor_reader(&self, i: usize) -> Result<Box<dyn SensorReader>>;
}

pub trait SensorReader {
    fn metadata(&self) -> &Vec<ChunkMetadata>;

    fn number_of_chunks(&self) -> usize;

    fn get_chunk_reader(
        &self,
        i: usize,
    ) -> Result<Box<dyn ChunkReader<Item = Box<dyn PageReader>>>>;
}

pub trait ChunkReader: Iterator {}

pub trait PageReader {
    fn header(&self) -> &PageHeader;
    fn data(&self) -> Result<(Vec<Field>, Vec<Field>)>;
}

pub struct RowIter {
    current_row_group: usize,
    num_row_groups: usize,
    iters: Vec<Box<dyn PageReader>>,
}

impl RowIter {
    pub fn new(iters: Vec<Box<dyn PageReader>>) -> Self {
        Self {
            current_row_group: 0,
            num_row_groups: 0,
            iters,
        }
    }
}

impl Length for File {
    fn len(&self) -> u64 {
        self.metadata().map(|m| m.len()).unwrap_or(0u64)
    }
}

impl SectionReader for File {
    type T = FileSource<File>;

    fn get_read(&self, start: u64, length: usize) -> Self::T {
        FileSource::new(self, start, length)
    }

    fn get_cursor(&self, start: u64, len: usize) -> Result<Cursor<Vec<u8>>> {
        let mut reader = self.get_read(start, len);
        let mut data = vec![0; len];
        reader
            .read_exact(&mut data)
            .context(ReadFixedLength { len })?;
        Ok(Cursor::new(data))
    }
}

impl TryClone for File {
    fn try_clone(&self) -> std::io::Result<Self> {
        self.try_clone()
    }
}
