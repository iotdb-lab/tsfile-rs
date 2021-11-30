use std::borrow::{Borrow, BorrowMut};
use std::convert::TryFrom;
use std::fs::File;
use std::io::{Cursor, Read};
use std::ops::Deref;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc::channel;

use crate::error::Result;
use crate::error::TsFileError;
use crate::file::footer;
use crate::file::metadata::{MetaDataIndexNode, MetadataIndexNodeType, TsFileMetadata};
use crate::file::metadata::MetadataIndexNodeType::{InternalDevice, LeafDevice};
use crate::file::reader::{DeviceReader, FileReader, RowIter, SectionReader};
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

impl<R: SectionReader> FileReader for TsFileSearchReader<R> {
    fn metadata(&self) -> &TsFileMetadata {
        &self.metadata
    }

    fn all_devices(&mut self) -> &Vec<String> {
        if self.all_devices.is_empty() {
            let mut devices: Vec<String> = Vec::new();
            let index = self.metadata.file_meta().metadata_index();
            let mut queue: Queue<MetadataIndexNodeType> = Queue::new();
            queue.push(index.clone());
            while !queue.is_empty() {
                let x = queue.pop();
                match x {
                    InternalDevice(c) => {
                        let start = c.children().get(0).unwrap();
                        let end = c.end_offset();
                        let len = (end - start.offset()) as usize;
                        if let Ok(mut reader) = self
                            .reader
                            .get_read(start.offset() as u64, len) {
                            let mut data = vec![0; len];
                            reader.read_exact(&mut data);
                            let mut cursor = Cursor::new(data);

                            for i in 0..c.children().len() {
                                if let Ok(t) = MetadataIndexNodeType::new(&mut cursor) {
                                    queue.push(t)
                                }
                            }
                        }
                    }
                    LeafDevice(m) => {
                        m.children()
                            .into_iter()
                            .map(|x| x.name())
                            .into_iter()
                            .for_each(|x| devices.push(x.to_string()));
                    }
                    _ => {}
                }
            }
            self.all_devices = devices
        }

        &self.all_devices
    }

    // fn get_device_iter(&self) -> Result<DeviceIter> {
    //     todo!()
    // }

    fn get_device_reader(&self, device_name: &str) -> Result<Box<dyn crate::file::reader::DeviceReader>> {
        todo!()
    }

    fn get_sensor_iter(&self, sensor_path: &str) -> Result<RowIter> {
        todo!()
    }

    fn get_filter_iter(&self, sensor_path: &str, predicate: &dyn Fn(u64) -> bool) -> Result<RowIter> {
        todo!()
    }
}

// pub struct DeviceIter {
//     reader: Option<DeviceReader>,
//
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