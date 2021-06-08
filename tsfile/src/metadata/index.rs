use std::borrow::{Borrow, BorrowMut};
use std::fs::File;
use std::io::{Chain, Cursor, Read, Seek};

use byteorder::{BigEndian, ByteOrder, LittleEndian, ReadBytesExt};

use crate::error::Result;
use crate::error::TsFileError;
use crate::reader::{ChunkReader, TsFileReader};
use crate::metadata::index::MetadataIndexNodeType::{InternalDevice, LeafDevice, InternalMeasurement, LeafMeasurement};
use crate::error::TsFileError::General;
use bit_set::BitSet;
use std::u8::MIN;

#[derive(Debug)]
pub struct TsFileMetadata {
    size: u64,
    fileMeta: FileMeta,
}

impl TsFileMetadata {
    pub fn parser(mut data: Cursor<Vec<u8>>) -> Result<Self> {
        // metadataIndex
        let metadata_index = MetadataIndexNodeType::new(&mut data).unwrap();
        // totalChunkNum
        let mut buffer = vec![0; 4];
        data.read(&mut buffer);
        let total_chunk_num = BigEndian::read_i32(&buffer);
        // invalidChunkNum
        let mut buffer = vec![0; 4];
        data.read(&mut buffer);
        let invalid_chunk_num = BigEndian::read_i32(&buffer);

        // versionInfo
        let mut buffer = vec![0; 4];
        data.read(&mut buffer);
        let version_size = BigEndian::read_i32(&buffer);

        let mut version_info = Vec::with_capacity(version_size as usize);

        for i in 0..version_size {
            let mut buffer = vec![0; 8];
            data.read(&mut buffer);
            let version_pos = BigEndian::read_i64(&buffer);

            let mut buffer = vec![0; 8];
            data.read(&mut buffer);
            let version = BigEndian::read_i64(&buffer);

            version_info.push((version_pos, version));
        }

        // metaOffset
        let mut buffer = vec![0; 8];
        data.read(&mut buffer);
        let meta_offset = BigEndian::read_i64(&buffer);

        // read bloom filter
        let mut bloom_filter = None;
        let length = data.get_ref().capacity();
        if data.position() < length as u64 {
            let mut buffer = vec![0; 4];
            data.read(&mut buffer);
            let byte_length = BigEndian::read_i32(&buffer);

            let mut bytes = vec![0; byte_length as usize];
            data.read(&mut bytes);

            let mut buffer = vec![0; 4];
            data.read(&mut buffer);
            let filter_size = BigEndian::read_i32(&buffer);
            let hash_function_size = BigEndian::read_i32(&buffer);
            bloom_filter = Some(BloomFilter::new(bytes, filter_size, hash_function_size));
        }

        Ok(Self {
            size: 0,
            fileMeta: FileMeta {
                metadata_index,
                total_chunk_num,
                invalid_chunk_num,
                version_info,
                meta_offset,
                bloom_filter,
            },
        })
    }
}


#[derive(Debug)]
pub struct FileMeta {
    metadata_index: MetadataIndexNodeType,
    total_chunk_num: i32,
    invalid_chunk_num: i32,
    version_info: Vec<(i64, i64)>,
    meta_offset: i64,
    bloom_filter: Option<BloomFilter>,
}

#[derive(Debug)]
pub struct BloomFilter {
    MINIMAL_SIZE: i32,
    MAXIMAL_HASH_FUNCTION_SIZE: i32,
    SEEDS: Vec<i32>,
    size: i32,
    hashFunctionSize: i32,
    bits: BitSet,
    func: Vec<HashFunction>,
}

impl BloomFilter {
    pub fn new(data: Vec<u8>, filter_size: i32, hash_function_size: i32) -> Self {
        let seeds = vec![5, 7, 11, 19, 31, 37, 43, 59];
        let hash_function_size = std::cmp::min(8, hash_function_size);

        let mut func: Vec<HashFunction> = Vec::with_capacity(hash_function_size as usize);
        for i in 0..hash_function_size {
            func.push(HashFunction::new(filter_size, seeds[i as usize]));
        }


        Self {
            size: filter_size,
            MINIMAL_SIZE: 256,
            MAXIMAL_HASH_FUNCTION_SIZE: 8,
            SEEDS: seeds,
            hashFunctionSize: hash_function_size,
            func,
            bits: BitSet::from_bytes(&data[8..]),
        }
        //
        // this.size = size;
        // this.hashFunctionSize = hashFunctionSize;
        // func = new HashFunction[hashFunctionSize];
        // for (int i = 0; i < hashFunctionSize; i++) {
        //     func[i] = new HashFunction(size, SEEDS[i]);
        // }
        //
        // bits = BitSet.valueOf(bytes);
    }
}

#[derive(Debug)]
pub struct HashFunction {
    cap: i32,
    seed: i32,
}

impl HashFunction {
    pub fn new(filter_size: i32, seed: i32) -> Self {
        Self {
            cap: filter_size,
            seed,
        }
    }
}


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


