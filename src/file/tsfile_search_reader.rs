use std::convert::TryFrom;
use std::fs::File;
use std::io::{Cursor, Read};
use std::path::Path;
use std::sync::Arc;

use crate::error::Result;
use crate::error::TsFileError;
use crate::file::footer;
use crate::file::metadata::{MetadataIndexNodeType, TsFileMetadata};
use crate::file::metadata::MetadataIndexNodeType::*;
use crate::file::reader::{DeviceMetadataIter, FileReader, SectionReader, SensorMetadataIter};
use crate::utils::queue::Queue;

impl TryFrom<File> for TsFileSearchReader<File> {
    type Error = TsFileError;

    fn try_from(file: File) -> Result<Self> {
        Self::new(file)
    }
}

impl<'a> TryFrom<&'a Path> for TsFileSearchReader<File> {
    type Error = TsFileError;

    fn try_from(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        Self::try_from(file)
    }
}

impl TryFrom<String> for TsFileSearchReader<File> {
    type Error = TsFileError;

    fn try_from(path: String) -> Result<Self> {
        Self::try_from(Path::new(&path))
    }
}

impl<'a> TryFrom<&'a str> for TsFileSearchReader<File> {
    type Error = TsFileError;

    fn try_from(path: &str) -> Result<Self> {
        Self::try_from(Path::new(&path))
    }
}

pub struct TsFileSearchReader<R: SectionReader> {
    reader: Arc<R>,
    metadata: TsFileMetadata,
    all_devices: Vec<String>,
}

impl<R: 'static + SectionReader> FileReader for TsFileSearchReader<R> {
    fn metadata(&self) -> &TsFileMetadata {
        &self.metadata
    }

    fn device_meta_iter(&self) -> Box<dyn DeviceMetadataIter<Item=MetadataIndexNodeType>> {
        let mut stack = Vec::new();
        stack.push(self.metadata.file_meta().metadata_index().clone());
        Box::new(DeviceMetadataReader::new(self.reader.clone(), stack))
    }

    fn sensor_meta_iter(&self, device: &str) -> Box<dyn SensorMetadataIter<Item=MetadataIndexNodeType>> {
        let mut stack = Vec::new();
        stack.push(self.metadata.file_meta().metadata_index().clone());
        Box::new(SensorMetadataReader::new(self.reader.clone(), stack, device))
    }
}

pub struct DeviceMetadataReader<R: SectionReader> {
    reader: Arc<R>,
    stack: Vec<MetadataIndexNodeType>,
}

pub struct SensorMetadataReader<R: SectionReader> {
    reader: Arc<R>,
    stack: Vec<MetadataIndexNodeType>,
    device: String,
}

impl<R: SectionReader> DeviceMetadataIter for DeviceMetadataReader<R> {}

impl<R: SectionReader> SensorMetadataIter for SensorMetadataReader<R> {}

impl<R: SectionReader> DeviceMetadataReader<R> {
    pub fn new(reader: Arc<R>, stack: Vec<MetadataIndexNodeType>) -> Self {
        Self {
            reader,
            stack,
        }
    }
}

impl<R: SectionReader> SensorMetadataReader<R> {
    pub fn new(reader: Arc<R>, stack: Vec<MetadataIndexNodeType>, device: String) -> Self {
        Self {
            reader,
            stack,
            device,
        }
    }
}

impl<R: SectionReader> Iterator for DeviceMetadataReader<R> {
    type Item = MetadataIndexNodeType;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stack.is_empty() {
            return None;
        }
        while !self.stack.is_empty() {
            match self.stack.pop()? {
                InternalDevice(c) => {
                    let start = c.children().get(0).unwrap();
                    let end = c.end_offset();
                    let len = (end - start.offset()) as usize;
                    if let Ok(mut reader) = self
                        .reader
                        .get_read(start.offset() as u64, len) {
                        let mut data = vec![0; len];
                        reader.read_exact(&mut data).ok();
                        let mut cursor = Cursor::new(data);

                        let mut types = Vec::new();
                        for _ in 0..c.children().len() {
                            if let Ok(t) = MetadataIndexNodeType::new(&mut cursor) {
                                types.push(t);
                            }
                        }
                        while !types.is_empty() {
                            self.stack.push(types.pop()?);
                        }
                    }
                }
                LeafDevice(c) => {
                    return Some(MetadataIndexNodeType::LeafDevice(c));
                }
                _ => {}
            }
        }
        None
    }
}

impl<R: SectionReader> Iterator for SensorMetadataReader<R> {
    type Item = MetadataIndexNodeType;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stack.is_empty() {
            return None;
        }
        while !self.stack.is_empty() {
            match self.stack.pop()? {
                InternalDevice(c) | InternalMeasurement(c) | LeafDevice(c) => {
                    let start = c.children().get(0).unwrap();
                    let end = c.end_offset();
                    let len = (end - start.offset()) as usize;
                    if let Ok(mut reader) = self
                        .reader
                        .get_read(start.offset() as u64, len) {
                        let mut data = vec![0; len];
                        reader.read_exact(&mut data).ok();
                        let mut cursor = Cursor::new(data);

                        let mut types = Vec::new();
                        for _ in 0..c.children().len() {
                            if let Ok(t) = MetadataIndexNodeType::new(&mut cursor) {
                                types.push(t);
                            }
                        }
                        while !types.is_empty() {
                            self.stack.push(types.pop()?);
                        }
                    }
                }
                LeafMeasurement(c) => {
                    return Some(MetadataIndexNodeType::LeafMeasurement(c));
                }
            }
        }
        None
    }
}


// pub struct DeviceIter {
//     devices: Vec<MetadataIndexNodeType::LeafDevice>,
// }
//
// impl DeviceIter {
//     pub fn new(reader: , root: MetadataIndexNodeType) -> Result<DeviceIter> {
//         let mut tree: Vec<MetadataIndexEntry> = Vec::new();
//         if let InternalDevice(metadata) = root {
//             let c = metadata.children();
//             let first = c.get(0)?.offset();
//
//             for i in c.len()..0 {
//                 tree.push(c.get(i)?.clone());
//             }
//             let len = (first - c.get(c.len())?.offset()) as usize;
//
//             let mut reader = reader.get_read(first as u64, len as usize)?;
//             let mut data = vec![0; len];
//             reader.read_exact(&mut data);
//             let mut cursor = Cursor::new(data);
//
//             Ok(Self {
//                 reader: None,
//                 index_tree: tree,
//                 cursor,
//             })
//         }
//         Err(TsFileError::General("123".to_string()));
//     }
// }
//
// impl Iterator for DeviceIter {
//     type Item = Result<Box<dyn DeviceReader>>;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         if self.cursor.is_empty() {
//             None
//         }
//         let result = MetadataIndexNodeType::new(&mut self.cursor);
//
//
//         None
//     }
// }


impl<R: 'static + SectionReader> TsFileSearchReader<R> {
    pub fn new(file: R) -> Result<Self> {
        let metadata = footer::parser_metadata(&file)?;
        Ok(Self {
            reader: Arc::new(file),
            metadata,
            all_devices: vec![],
        })
    }
}
