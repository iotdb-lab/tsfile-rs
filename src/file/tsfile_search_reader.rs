use std::convert::TryFrom;
use std::fs::File;
use std::io::{Cursor, Read};
use std::path::Path;
use std::sync::Arc;

use crate::error::Result;
use crate::error::TsFileError;
use crate::file::footer;
use crate::file::metadata::{MetadataIndexNodeType, TsFileMetadata};
use crate::file::metadata::MetadataIndexNodeType::InternalDevice;
use crate::file::reader::{FileReader, RowIter, SectionReader};

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

    fn all_devices(&self) -> &Vec<String> {
        if self.all_devices.is_empty() {
            let index = self.metadata.file_meta().metadata_index();
            match index {
                InternalDevice(root) => {
                    if let Some(start) = root.children().get(0) {
                        let len = root.end_offset() - start.offset();
                        if let Ok(mut sReader) = self
                            .reader
                            .get_read(start.offset() as u64, len as usize) {
                            let mut data = vec![0; len as usize];
                            sReader.read(&mut data)?;
                            let mut cursor = Box::new(Cursor::new(data));

                            for i in 0..root.children().len() {
                                if let Ok(InternalDevice(c)) = MetadataIndexNodeType::new(&mut cursor) {

                                }
                            }
                            MetadataIndexNodeType::new()
                        }
                    }
                }
                _ => {}
            }
        }

        &self.all_devices
    }

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