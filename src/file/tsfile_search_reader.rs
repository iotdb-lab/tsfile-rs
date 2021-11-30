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
use crate::file::reader::{DeviceMetadataReader, FileReader, SectionReader};
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

    fn get_device_search_reader(&self) -> Box<dyn DeviceMetadataReader<Item=MetadataIndexNodeType>> {
        let mut queue = Queue::new();
        queue.push(self.metadata.file_meta().metadata_index().clone());
        Box::new(DeviceSearchReader {
            reader: self.reader.clone(),
            stack: queue,
        })
    }

    // fn all_devices(&mut self) -> &Vec<String> {
    //     if self.all_devices.is_empty() {
    //         let mut devices: Vec<String> = Vec::new();
    //         let index = self.metadata.file_meta().metadata_index();
    //         let mut queue: Queue<MetadataIndexNodeType> = Queue::new();
    //         queue.push(index.clone());
    //         while !queue.is_empty() {
    //             let x = queue.pop();
    //             match x {
    //                 InternalDevice(c) => {
    //                     let start = c.children().get(0).unwrap();
    //                     let end = c.end_offset();
    //                     let len = (end - start.offset()) as usize;
    //                     if let Ok(mut reader) = self
    //                         .reader
    //                         .get_read(start.offset() as u64, len) {
    //                         let mut data = vec![0; len];
    //                         reader.read_exact(&mut data);
    //                         let mut cursor = Cursor::new(data);
    //
    //                         for i in 0..c.children().len() {
    //                             if let Ok(t) = MetadataIndexNodeType::new(&mut cursor) {
    //                                 queue.push(t)
    //                             }
    //                         }
    //                     }
    //                 }
    //                 LeafDevice(m) => {
    //                     m.children()
    //                         .into_iter()
    //                         .map(|x| x.name())
    //                         .into_iter()
    //                         .for_each(|x| devices.push(x.to_string()));
    //                 }
    //                 _ => {}
    //             }
    //         }
    //         self.all_devices = devices
    //     }
    //
    //     &self.all_devices
    // }
    //
    // fn get_device_iter(&self) -> Result<DeviceIter> {
    //     todo!()
    // }
    //
    // fn get_device_reader(&self, device_name: &str) -> Result<Box<dyn crate::file::reader::DeviceReader>> {
    //     todo!()
    // }
    //
    // fn get_sensor_iter(&self, sensor_path: &str) -> Result<RowIter> {
    //     todo!()
    // }
    //
    // fn get_filter_iter(&self, sensor_path: &str, predicate: &dyn Fn(u64) -> bool) -> Result<RowIter> {
    //     todo!()
    // }
}

pub struct DeviceSearchReader<R: SectionReader> {
    reader: Arc<R>,
    stack: Queue<MetadataIndexNodeType>,
}

impl<R: SectionReader> DeviceMetadataReader for DeviceSearchReader<R> {
    fn metadata(&self) {
        todo!()
    }
}

impl<R: SectionReader> DeviceSearchReader<R> {
    fn new(reader: Arc<R>, stack: Queue<MetadataIndexNodeType>) -> Self {
        Self {
            reader,
            stack,
        }
    }
}


impl<R: SectionReader> Iterator for DeviceSearchReader<R> {
    type Item = MetadataIndexNodeType;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stack.is_empty() {
            return None;
        }
        while !self.stack.is_empty() {
            match self.stack.pop() {
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

                        for _ in 0..c.children().len() {
                            if let Ok(t) = MetadataIndexNodeType::new(&mut cursor) {
                                self.stack.push(t)
                            }
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