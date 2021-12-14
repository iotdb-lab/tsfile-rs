use std::convert::TryFrom;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use crate::error::Result;
use crate::error::TsFileError;
use crate::file::footer;
use crate::file::metadata::MetadataIndexNodeType::*;
use crate::file::metadata::{MetadataIndexNodeType, TimeseriesMetadata,
                            TimeseriesMetadataType, TsFileMetadata,
};
use crate::file::reader::{
    DeviceMetadataIter, FileReader, SectionReader, SensorMetadataIter, SensorReader,
};

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

    // fn binary_search_meta(&self, root: MetadataIndexNodeType, device: String, sensor: String) -> Option<(MetadataIndexEntry, i64)> {
    //     let binary_search = |c: &MetaDataIndexNode, calc: Box<Fn(&MetadataIndexEntry) -> Ordering>| -> Option<(i64, i64, usize)> {
    //         let index = match c.children()
    //             .binary_search_by(calc) {
    //             Ok(r) => { r }
    //             Err(r) => { if r == 0 { return None; } else { r - 1 } }
    //         };
    //         let start = c.children().get(index)?.offset();
    //         let len = if index == c.children().len() - 1 {
    //             c.end_offset() - start
    //         } else {
    //             c.children().get(index + 1)?.offset() - start
    //         };
    //         Some((start, len, index))
    //     };
    //
    //
    //     let mut stack = Vec::new();
    //     stack.push(root);
    //     while !stack.is_empty() {
    //         let index = match stack.pop()? {
    //             InternalDevice(c) | LeafDevice(c) | InternalMeasurement(c) => {
    //                 binary_search(&c, Box::new(|x| x.name().cmp(&device)))
    //             }
    //             LeafMeasurement(c) => {
    //                 return match binary_search(&c, Box::new(|x| x.name().cmp(&sensor))) {
    //                     None => { None }
    //                     Some((_, len, index)) => {
    //                         Some((c.children().get(index)?.clone(), len))
    //                     }
    //                 };
    //             }
    //         };
    //         match index {
    //             None => { return None; }
    //             Some((s, len, _)) => {
    //                 if let Ok(mut reader) = self.reader.get_read(s as u64, len as usize)
    //                 {
    //                     let mut data = vec![0; len as usize];
    //                     reader.read_exact(&mut data);
    //                     if let Ok(result) = MetadataIndexNodeType::new(&mut Cursor::new(data)) {
    //                         stack.push(result);
    //                     }
    //                 }
    //             }
    //         }
    //     }
    //     None
    // }

    fn device_meta_iter(&self) -> Box<dyn DeviceMetadataIter<Item=MetadataIndexNodeType>> {
        let mut stack = Vec::new();
        stack.push(self.metadata.file_meta().metadata_index().clone());
        Box::new(DeviceMetadataReader::new(self.reader.clone(), stack))
    }

    fn get_device_reader() {
        todo!()
    }

    fn sensor_meta_iter(
        &self,
        device: String,
    ) -> Box<dyn SensorMetadataIter<Item=TimeseriesMetadata>> {
        let mut stack = Vec::new();
        stack.push(self.metadata.file_meta().metadata_index().clone());
        Box::new(SensorMetadataReader::new(
            self.reader.clone(),
            stack,
            device,
        ))
    }

    fn get_sensor_reader(&self, device: String, sensor: String) -> Box<dyn SensorReader> {
        todo!()
    }
}

impl<R: 'static + SectionReader> TsFileSearchReader<R> {}

pub struct DeviceMetadataReader<R: SectionReader> {
    reader: Arc<R>,
    stack: Vec<MetadataIndexNodeType>,
}

pub struct SensorMetadataReader<R: SectionReader> {
    reader: Arc<R>,
    stack: Vec<MetadataIndexNodeType>,
    ts_stack: Vec<TimeseriesMetadata>,
    device: String,
}

impl<R: SectionReader> DeviceMetadataIter for DeviceMetadataReader<R> {}

impl<R: SectionReader> SensorMetadataIter for SensorMetadataReader<R> {}

impl<R: SectionReader> DeviceMetadataReader<R> {
    pub fn new(reader: Arc<R>, stack: Vec<MetadataIndexNodeType>) -> Self {
        Self { reader, stack }
    }
}

impl<R: SectionReader> SensorMetadataReader<R> {
    pub fn new(reader: Arc<R>, stack: Vec<MetadataIndexNodeType>, device: String) -> Self {
        Self {
            reader,
            stack,
            ts_stack: Vec::new(),
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
                    if let Ok(mut cursor) = self.reader.get_cursor(start.offset() as u64, len) {
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
    type Item = TimeseriesMetadata;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.ts_stack.is_empty() {
            return self.ts_stack.pop();
        }

        if self.stack.is_empty() {
            return None;
        }

        while !self.stack.is_empty() {
            match self.stack.pop()? {
                InternalDevice(c) | LeafDevice(c) => {
                    let index = match c
                        .children()
                        .binary_search_by(|x| x.name().cmp(&self.device))
                    {
                        Ok(r) => r,
                        Err(r) => {
                            if r == 0 {
                                return None;
                            } else {
                                r - 1
                            }
                        }
                    };

                    let child_num = c.children().len();

                    let start = c.children().get(index)?.offset();
                    let len = if index == child_num - 1 {
                        c.end_offset() - start
                    } else {
                        c.children().get(index + 1)?.offset() - start
                    };
                    if let Ok(mut cursor) = self.reader.get_cursor(start as u64, len as usize) {
                        if let Ok(t) = MetadataIndexNodeType::new(&mut cursor) {
                            self.stack.push(t);
                        }
                    }
                }
                InternalMeasurement(c) => {
                    let start = c.children().get(0)?.offset();
                    let len = c.end_offset() - start;
                    let child_num = c.children().len();

                    if let Ok(mut cursor) = self.reader.get_cursor(start as u64, len as usize) {
                        let mut types = Vec::new();
                        for _ in 0..child_num {
                            if let Ok(t) = MetadataIndexNodeType::new(&mut cursor) {
                                types.push(t);
                            }
                        }
                        while !types.is_empty() {
                            if let Some(data) = types.pop() {
                                self.stack.push(data);
                            }
                        }
                    }
                }
                LeafMeasurement(c) => {
                    for i in 0..c.children().len() {
                        let start = c.children().get(i).unwrap();
                        let end = if i == c.children().len() - 1 {
                            c.end_offset()
                        } else {
                            c.children().get(i + 1)?.offset()
                        };
                        let len = (end - start.offset()) as usize;
                        if let Ok(mut cursor) = self.reader.get_cursor(start.offset() as u64, len) {
                            while cursor.position() < len as u64 {
                                if let Ok(t) = TimeseriesMetadataType::new(&mut cursor) {
                                    self.ts_stack.push(t);
                                }
                            }
                        }
                    }
                }
            }
        }
        self.ts_stack.pop()
    }
}

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
