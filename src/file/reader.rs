use std::fs::File;
use std::io::{Cursor, Read};

use crate::error::{Result, TsFileError};
use crate::file::metadata::{ChunkMetadata, MetadataIndexEntry, MetadataIndexNodeType, TimeseriesMetadata, TimeseriesMetadataType, TsFileMetadata};
use crate::utils::io::FileSource;

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

    fn binary_search_meta(&self, root: MetadataIndexNodeType, device: String, sensor: String) -> Option<(MetadataIndexEntry, i64)>;

    fn device_meta_iter(&self) -> Box<dyn DeviceMetadataIter<Item=MetadataIndexNodeType>>;

    fn sensor_meta_iter(&self, device: String) -> Box<dyn SensorMetadataIter<Item=TimeseriesMetadata>>;
    //
    // fn get_filter_iter(&self, sensor_path: &str, predicate: &dyn Fn(u64) -> bool) -> Result<RowIter>;
}

pub trait DeviceMetadataIter: Iterator {}

pub trait SensorMetadataIter: Iterator {}

pub trait DeviceReader {
    fn metadata(&self) -> Vec<TimeseriesMetadata>;

    fn get_sensor_reader(&self, i: usize) -> Result<Box<dyn SensorReader>>;
}

pub trait SensorReader {
    fn metadata(&self) -> Vec<ChunkMetadata>;

    fn get_chunk_page_reader(&self, i: usize) -> Result<Box<dyn PageReader>>;

    fn get_chunk_reader(&self, i: usize) -> Result<Box<dyn ChunkReader>>;

    fn get_page_iter(&self, predicate: dyn Fn(u64) -> bool) -> Result<RowIter>;
}

pub trait ChunkReader {}

pub trait PageReader {}

pub struct RowIter {
    current_row_group: usize,
    num_row_groups: usize,
}

impl RowIter {
    fn new() {}
}

impl Length for File {
    fn len(&self) -> u64 {
        self.metadata().map(|m| m.len()).unwrap_or(0u64)
    }
}

impl SectionReader for File {
    type T = FileSource<File>;

    fn get_read(&self, start: u64, length: usize) -> Result<Self::T> {
        Ok(FileSource::new(self, start, length))
    }

    fn get_cursor(&self, start: u64, len: usize) -> Result<Cursor<Vec<u8>>> {
        match self.get_read(start, len) {
            Ok(mut reader) => {
                let mut data = vec![0; len];
                reader.read_exact(&mut data);
                Ok(Cursor::new(data))
            }
            Err(e) => { Err(e) }
        }
    }
}

impl TryClone for File {
    fn try_clone(&self) -> std::io::Result<Self> {
        self.try_clone()
    }
}