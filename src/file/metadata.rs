use std::borrow::BorrowMut;
use std::io::{Cursor, Read};

use bit_set::BitSet;
use byteorder::{BigEndian, ByteOrder};
use varint::VarintRead;

use crate::error::{Result, TsFileError};
use crate::error::TsFileError::General;
use crate::file::metadata::MetadataIndexNodeType::{InternalDevice, InternalMeasurement, LeafDevice, LeafMeasurement};
use crate::utils::io::{BigEndianReader, VarIntReader};

#[derive(Debug)]
pub struct TsFileMetadata {
    size: u64,
    file_meta: FileMeta,
}

impl TsFileMetadata {
    pub fn file_meta(&self) -> &FileMeta {
        &self.file_meta
    }
}

#[derive(Debug)]
pub struct FileMeta {
    metadata_index: MetadataIndexNodeType,
    meta_offset: i64,
    bloom_filter: Option<BloomFilter>,
}

impl FileMeta {
    pub fn new(index: MetadataIndexNodeType, offset: i64, filter: Option<BloomFilter>) -> Self {
        FileMeta {
            metadata_index: index,
            meta_offset: offset,
            bloom_filter: filter,
        }
    }

    pub fn bloom_filter(&self) -> &Option<BloomFilter> {
        &self.bloom_filter
    }

    pub fn metadata_index(&self) -> &MetadataIndexNodeType { &self.metadata_index }
}


#[derive(Debug)]
pub struct BloomFilter {
    minimal_size: i32,
    maximal_hash_function_size: i32,
    seeds: Vec<u32>,
    size: u32,
    hash_function_size: u32,
    bits: BitSet,
    func: Vec<HashFunction>,
}

impl BloomFilter {
    pub fn contains(&self, path: &str) -> bool {
        if path.is_empty() {
            return false;
        }
        let mut ret = true;
        let mut index: usize = 0;
        while ret && index < self.hash_function_size as usize {
            ret = self.bits.contains(self.func[index].hash(path) as usize);
            index += 1;
        }
        return ret;
    }
}


#[derive(Debug)]
pub struct HashFunction {
    cap: u32,
    seed: u32,
}

impl HashFunction {
    pub fn hash(&self, path: &str) -> i32 {
        let hash_data = murmurhash3::murmurhash3_x64_128(&mut path.as_bytes(), self.seed as u64);
        let data = hash_data.0 as i32 + hash_data.1 as i32;
        data % self.cap as i32
    }
}

#[derive(Debug)]
pub enum MetadataIndexNodeType {
    InternalDevice(MetaDataIndexNode),
    LeafDevice(MetaDataIndexNode),
    InternalMeasurement(MetaDataIndexNode),
    LeafMeasurement(MetaDataIndexNode),
}

impl Clone for MetadataIndexNodeType {
    fn clone(&self) -> Self {
        match self {
            InternalDevice(m) => { InternalDevice(m.clone()) }
            LeafDevice(m) => { LeafDevice(m.clone()) }
            InternalMeasurement(m) => { InternalMeasurement(m.clone()) }
            LeafMeasurement(m) => { LeafMeasurement(m.clone()) }
        }
    }
}

#[derive(Debug)]
pub struct MetaDataIndexNode {
    children: Vec<MetadataIndexEntry>,
    end_offset: i64,
}

impl MetaDataIndexNode {
    pub fn children(&self) -> &Vec<MetadataIndexEntry> {
        &self.children
    }

    pub fn end_offset(&self) -> i64 {
        self.end_offset
    }
}

impl Clone for MetaDataIndexNode {
    fn clone(&self) -> Self {
        let mut vec: Vec<MetadataIndexEntry> = Vec::with_capacity(self.children.len());
        Vec::clone_from(&mut vec, self.children());
        MetaDataIndexNode {
            children: vec,
            end_offset: self.end_offset,
        }
    }

    fn clone_from(&mut self, source: &Self) {
        todo!()
    }
}


#[derive(Debug)]
pub struct MetadataIndexEntry {
    name: String,
    offset: i64,
}

impl MetadataIndexEntry {
    pub fn offset(&self) -> i64 {
        self.offset
    }
    pub fn name(&self) -> &str { self.name.as_str() }
}

impl Clone for MetadataIndexEntry {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            offset: self.offset.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        todo!()
    }
}

#[derive(Debug)]
pub struct TimeseriesMetadata {}

impl TimeseriesMetadata {
    pub fn new(cursor: &mut Cursor<Vec<u8>>) -> Result<TimeseriesMetadata> {
        todo!()
    }
}

pub struct ChunkMetadata {}

impl TsFileMetadata {
    pub fn parser(mut data: Cursor<Vec<u8>>) -> Result<Self> {
        // metadataIndex
        let metadata_index = MetadataIndexNodeType::new(&mut data).unwrap();
        // metaOffset
        let meta_offset = data.read_big_endian_i64();

        // read bloom filter
        let mut bloom_filter = None;
        let length = data.get_ref().capacity();
        if data.position() < length as u64 {
            match data.read_unsigned_varint_32() {
                Ok(bloom_filter_size) => {
                    let mut bytes = vec![0; bloom_filter_size as usize];
                    data.read(&mut bytes)?;

                    let filter_size = data.read_unsigned_varint_32().unwrap();
                    let hash_function_size = data.read_unsigned_varint_32().unwrap();
                    bloom_filter = Some(BloomFilter::new(bytes, filter_size, hash_function_size));
                    Ok(Self {
                        size: 0,
                        file_meta: FileMeta::new(metadata_index, meta_offset, bloom_filter),
                    })
                }
                Err(e) => {
                    Err(TsFileError::General(e.to_string()))
                }
            }
        } else {
            Ok(Self {
                size: 0,
                file_meta: FileMeta::new(metadata_index, meta_offset, bloom_filter),
            })
        }
    }
}

impl BloomFilter {
    pub fn new(data: Vec<u8>, filter_size: u32, hash_function_size: u32) -> Self {
        let seeds = vec![5, 7, 11, 19, 31, 37, 43, 59];
        let hash_function_size = std::cmp::min(8, hash_function_size);

        let mut func: Vec<HashFunction> = Vec::with_capacity(hash_function_size as usize);
        for i in 0..hash_function_size {
            func.push(HashFunction::new(filter_size, seeds[i as usize]));
        }


        Self {
            size: filter_size,
            minimal_size: 256,
            maximal_hash_function_size: 8,
            seeds,
            hash_function_size,
            func,
            bits: BitSet::from_bytes(&data[8..]),
        }
    }
}

impl HashFunction {
    pub fn new(filter_size: u32, seed: u32) -> Self {
        Self {
            cap: filter_size,
            seed,
        }
    }
}

impl MetadataIndexNodeType {
    pub fn new(data: &mut Cursor<Vec<u8>>) -> Result<Self> {
        match data.read_unsigned_varint_32() {
            Ok(len) => {
                let mut children: Vec<MetadataIndexEntry> = Vec::with_capacity(len as usize);
                for _i in 0..len {
                    children.push(MetadataIndexEntry::new(data.borrow_mut()).unwrap());
                }

                let end_offset = data.read_big_endian_i64();

                let mut vec = vec![255; 1];
                data.read(&mut vec)?;

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
            Err(e) => {
                return Err(TsFileError::General(e.to_string()));
            }
        }
    }
}

impl MetadataIndexEntry {
    fn new(data: &mut Cursor<Vec<u8>>) -> Result<Self> {
        match data.read_varint_string() {
            Ok(str) => {
                Ok(Self {
                    name: str,
                    offset: data.read_big_endian_i64(),
                })
            }
            Err(e) => {
                Err(TsFileError::General(e.to_string()))
            }
        }
    }
}


