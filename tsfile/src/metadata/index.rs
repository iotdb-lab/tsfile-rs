use std::borrow::{Borrow, BorrowMut};
use std::fs::File;
use std::io::{Chain, Cursor, Read, Seek};

use byteorder::{BigEndian, ByteOrder, LittleEndian, ReadBytesExt};

use crate::error::Result;
use crate::error::TsFileError;
use crate::error::TsFileError::General;
use crate::metadata::bloom_filter::BloomFilter;
use crate::metadata::index::MetadataIndexNodeType::{InternalDevice, InternalMeasurement, LeafDevice, LeafMeasurement};
use crate::reader::{ChunkReader, TsFileReader};

#[derive(Debug)]
pub enum MetadataIndexNodeType {
    InternalDevice(MetaDataIndexNode),
    LeafDevice(MetaDataIndexNode),
    InternalMeasurement(MetaDataIndexNode),
    LeafMeasurement(MetaDataIndexNode),
}

impl MetadataIndexNodeType {
    pub fn new(data: &mut Cursor<Vec<u8>>) -> Result<Self> {
        let mut vec = vec![0; 4];
        data.read(&mut vec);

        let len = BigEndian::read_i32(&vec);

        let mut children: Vec<MetadataIndexEntry> = Vec::with_capacity(len as usize);
        for i in 0..len {
            children.push(MetadataIndexEntry::new(data.borrow_mut()).unwrap());
        }

        let mut vec = vec![0; 8];
        data.read(&mut vec);
        let end_offset = BigEndian::read_i64(&vec);

        let mut vec = vec![0; 1];
        data.read(&mut vec);

        let node = MetaDataIndexNode {
            children,
            end_offset,
        };
        match vec[0] {
            0 => Ok(InternalDevice(node)),
            1 => Ok(LeafDevice(node)),
            2 => Ok(InternalMeasurement(node)),
            3 => Ok(LeafMeasurement(node)),
            _ => Err(General(format!("123")))
        }
    }
}

#[derive(Debug)]
pub struct MetaDataIndexNode {
    children: Vec<MetadataIndexEntry>,
    end_offset: i64,
}


#[derive(Debug)]
pub struct MetadataIndexEntry {
    name: String,
    offset: i64,
}

impl MetadataIndexEntry {
    fn new(data: &mut Cursor<Vec<u8>>) -> Result<Self> {
        let mut vec = vec![0; 4];
        data.read(&mut vec);

        let str_len = BigEndian::read_i32(&vec);

        if str_len < 0 {
            return Err(TsFileError::General(format!("{}", 123)));
        }

        if str_len == 0 {}

        let mut vec: Vec<u8> = vec![0; str_len as usize];
        data.read(&mut vec);
        let result = String::from_utf8(vec).unwrap();

        let mut vec = vec![0; 8];
        data.read(&mut vec);
        let offset = BigEndian::read_i64(&vec);

        Ok(Self {
            name: result,
            offset,
        })
    }
}


