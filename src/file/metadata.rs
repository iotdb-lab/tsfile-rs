use std::borrow::BorrowMut;
use std::convert::TryFrom;
use std::io::{Cursor, Read};
use std::sync::Arc;

use bit_set::BitSet;
use byteorder::{BigEndian, ReadBytesExt};
use snafu::ResultExt;
use varint::VarintRead;

use crate::file::metadata::MetadataIndexNodeType::{
    InternalDevice, InternalMeasurement, LeafDevice, LeafMeasurement,
};
use crate::file::metadata::TSDataType::Boolean;
use crate::file::metadata::TimeseriesMetadataType::{MoreChunks, OneChunk};
use crate::file::statistics::*;
use crate::utils::cursor;
use crate::utils::cursor::VarIntReader;
use snafu::{Snafu};
use crate::file::statistics;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to read VarInt or string: {}", source))]
    ReadVarInt { source: cursor::Error },
    #[snafu(display("Unable to read cursor data: {}", source))]
    ReadCursorData { source: std::io::Error },
    #[snafu(display("Unable to parser {} type statistics: {}", s_type, source))]
    ParserStatistics { s_type: String, source: statistics::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

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
    metadata_index: Arc<MetadataIndexNodeType>,
    meta_offset: i64,
    bloom_filter: Option<BloomFilter>,
}

impl FileMeta {
    pub fn new(index: MetadataIndexNodeType, offset: i64, filter: Option<BloomFilter>) -> Self {
        FileMeta {
            metadata_index: Arc::new(index),
            meta_offset: offset,
            bloom_filter: filter,
        }
    }

    pub fn bloom_filter(&self) -> &Option<BloomFilter> {
        &self.bloom_filter
    }

    pub fn metadata_index(&self) -> &MetadataIndexNodeType {
        &self.metadata_index
    }
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
        ret
    }
}

#[derive(Debug)]
pub struct HashFunction {
    cap: u32,
    seed: u32,
}

impl HashFunction {
    pub fn hash(&self, path: &str) -> i32 {
        let hash_data = murmurhash3::murmurhash3_x64_128(path.as_bytes(), self.seed as u64);
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
            InternalDevice(m) => InternalDevice(m.clone()),
            LeafDevice(m) => LeafDevice(m.clone()),
            InternalMeasurement(m) => InternalMeasurement(m.clone()),
            LeafMeasurement(m) => LeafMeasurement(m.clone()),
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

    fn clone_from(&mut self, _source: &Self) {
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
    pub fn name(&self) -> &str {
        self.name.as_str()
    }
}

impl Clone for MetadataIndexEntry {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            offset: self.offset,
        }
    }
}

#[derive(Debug)]
pub enum TimeseriesMetadataType {
    OneChunk,
    MoreChunks,
}

#[derive(Debug)]
pub struct TimeseriesMetadata {
    chunk_metadata_list: Vec<ChunkMetadata>,
    chunk_metadata_list_size: u32,
    measurement_id: String,
    data_type: TSDataType,
    metadata_type: TimeseriesMetadataType,
}

impl TimeseriesMetadata {
    pub fn chunk_metadata_list(self) -> Vec<ChunkMetadata> {
        self.chunk_metadata_list
    }
    pub fn measurement_id(&self) -> &str {
        self.measurement_id.as_str()
    }
}

impl TimeseriesMetadataType {
    pub fn new(cursor: &mut Cursor<Vec<u8>>) -> Result<TimeseriesMetadata> {
        let meta_type = match cursor.read_u8().context(ReadCursorData)? {
            0 => TimeseriesMetadataType::OneChunk,
            _ => TimeseriesMetadataType::MoreChunks,
        };
        let measurement_id = cursor.read_varint_string().context(ReadVarInt)?;
        let data_type = TSDataType::new(cursor.read_u8().context(ReadCursorData)?);
        let chunk_metadata_list_size = cursor.read_unsigned_varint_32().context(ReadCursorData)?;

        let statistics = Arc::new(match data_type {
            Boolean => Statistic::Boolean(BooleanStatistics::try_from(cursor.borrow_mut()).context(ParserStatistics { s_type: "Boolean".to_string() })?),
            TSDataType::Int32 => {
                Statistic::Int32(IntegerStatistics::try_from(cursor.borrow_mut()).context(ParserStatistics { s_type: "Int32".to_string() })?)
            }
            TSDataType::Int64 => Statistic::Int64(LongStatistics::try_from(cursor.borrow_mut()).context(ParserStatistics { s_type: "Int64".to_string() })?),
            TSDataType::FLOAT => Statistic::FLOAT(FloatStatistics::try_from(cursor.borrow_mut()).context(ParserStatistics { s_type: "FLOAT".to_string() })?),
            TSDataType::DOUBLE => {
                Statistic::DOUBLE(DoubleStatistics::try_from(cursor.borrow_mut()).context(ParserStatistics { s_type: "DOUBLE".to_string() })?)
            }
            TSDataType::TEXT => Statistic::TEXT(BinaryStatistics::try_from(cursor.borrow_mut()).context(ParserStatistics { s_type: "TEXT".to_string() })?),
        });
        let end_pos = cursor.position() + chunk_metadata_list_size as u64;
        let mut chunk_metadata_list = Vec::new();
        while cursor.position() < end_pos {
            let offset_chunk_header = cursor.read_i64::<BigEndian>().context(ReadCursorData)?;

            let statistic = match meta_type {
                OneChunk => statistics.clone(),
                MoreChunks => Arc::new(match data_type {
                    Boolean => Statistic::Boolean(BooleanStatistics::try_from(cursor.borrow_mut()).context(ParserStatistics { s_type: "Boolean".to_string() })?),
                    TSDataType::Int32 => {
                        Statistic::Int32(IntegerStatistics::try_from(cursor.borrow_mut()).context(ParserStatistics { s_type: "Int32".to_string() })?)
                    }
                    TSDataType::Int64 => Statistic::Int64(LongStatistics::try_from(cursor.borrow_mut()).context(ParserStatistics { s_type: "Int64".to_string() })?),
                    TSDataType::FLOAT => Statistic::FLOAT(FloatStatistics::try_from(cursor.borrow_mut()).context(ParserStatistics { s_type: "FLOAT".to_string() })?),
                    TSDataType::DOUBLE => {
                        Statistic::DOUBLE(DoubleStatistics::try_from(cursor.borrow_mut()).context(ParserStatistics { s_type: "DOUBLE".to_string() })?)
                    }
                    TSDataType::TEXT => Statistic::TEXT(BinaryStatistics::try_from(cursor.borrow_mut()).context(ParserStatistics { s_type: "TEXT".to_string() })?),
                }),
            };
            chunk_metadata_list.push(ChunkMetadata::new(
                measurement_id.clone(),
                offset_chunk_header,
                data_type.clone(),
                statistic,
            ));
        }
        Ok(TimeseriesMetadata {
            measurement_id,
            data_type,
            metadata_type: meta_type,
            chunk_metadata_list_size,
            chunk_metadata_list,
        })
    }
}

#[derive(Debug)]
pub enum TSDataType {
    Boolean,
    Int32,
    Int64,
    FLOAT,
    DOUBLE,
    TEXT,
}

impl Clone for TSDataType {
    fn clone(&self) -> Self {
        match self {
            Boolean => Self::Boolean,
            TSDataType::Int32 => Self::Int32,
            TSDataType::Int64 => Self::Int64,
            TSDataType::FLOAT => Self::FLOAT,
            TSDataType::DOUBLE => Self::DOUBLE,
            TSDataType::TEXT => Self::TEXT,
        }
    }
}

impl TSDataType {
    pub fn new(id: u8) -> Self {
        match id {
            0 => Self::Boolean,
            1 => Self::Int32,
            2 => Self::Int64,
            3 => Self::FLOAT,
            4 => Self::DOUBLE,
            _ => Self::TEXT,
        }
    }
    // fn new(flag: u8, cursor: &mut Cursor<Vec<u8>>) -> Result<TSDataType> {
    //     match flag {
    //         0 => Ok(Self::Boolean(BooleanStatistics::try_from(cursor).unwrap())),
    //         1 => Ok(Self::Int32(IntegerStatistics::try_from(cursor).unwrap())),
    //         2 => Ok(Self::Int64(LongStatistics::try_from(cursor).unwrap())),
    //         3 => Ok(Self::FLOAT(FloatStatistics::try_from(cursor).unwrap())),
    //         4 => Ok(Self::DOUBLE(DoubleStatistics::try_from(cursor).unwrap())),
    //         5 => Ok(Self::TEXT(BinaryStatistics::try_from(cursor).unwrap())),
    //         _ => Err(TsFileError::General("123".to_string())),
    //     }
    // }

    fn int_id(&self) -> u8 {
        match self {
            Boolean => 0,
            TSDataType::Int32 => 1,
            TSDataType::Int64 => 2,
            TSDataType::FLOAT => 3,
            TSDataType::DOUBLE => 4,
            TSDataType::TEXT => 5,
        }
    }
}

#[derive(Debug)]
pub struct ChunkMetadata {
    measurement_uid: String,
    ts_data_type: TSDataType,
    offset_chunk_header: i64,
    statistic: Arc<Statistic>,
}

impl ChunkMetadata {
    fn new(
        measurement_uid: String,
        offset_chunk_header: i64,
        ts_data_type: TSDataType,
        statistic: Arc<Statistic>,
    ) -> Self {
        Self {
            measurement_uid,
            ts_data_type,
            offset_chunk_header,
            statistic,
        }
    }

    pub fn ts_data_type(&self) -> &TSDataType {
        &self.ts_data_type
    }

    pub fn offset_chunk_header(&self) -> i64 {
        self.offset_chunk_header
    }

    pub fn statistic(&self) -> Arc<Statistic> {
        self.statistic.clone()
    }
}

impl TsFileMetadata {
    pub fn parser(mut data: Cursor<Vec<u8>>) -> Result<Self> {
        // metadataIndex
        let metadata_index = MetadataIndexNodeType::new(&mut data).unwrap();
        // metaOffset
        let meta_offset = data.read_i64::<BigEndian>().context(ReadCursorData)?;

        // read bloom filter
        let mut bloom_filter = None;
        let length = data.get_ref().capacity();
        if data.position() < length as u64 {
            let bloom_filter_size = data.read_unsigned_varint_32().context(ReadCursorData)?;
            let mut bytes = vec![0; bloom_filter_size as usize];
            data.read_exact(&mut bytes).context(ReadCursorData)?;

            let filter_size = data.read_unsigned_varint_32().context(ReadCursorData)?;
            let hash_function_size = data.read_unsigned_varint_32().context(ReadCursorData)?;
            bloom_filter = Some(BloomFilter::new(bytes, filter_size, hash_function_size));
            Ok(Self {
                size: 0,
                file_meta: FileMeta::new(metadata_index, meta_offset, bloom_filter),
            })
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
        let len = data.read_unsigned_varint_32().context(ReadCursorData)?;
        let mut children: Vec<MetadataIndexEntry> = Vec::with_capacity(len as usize);
        for _i in 0..len {
            children.push(MetadataIndexEntry::new(data.borrow_mut()).unwrap());
        }

        let end_offset = data.read_i64::<BigEndian>().context(ReadCursorData)?;

        let mut vec = vec![255; 1];
        data.read_exact(&mut vec);

        let node = MetaDataIndexNode {
            children,
            end_offset,
        };
        match vec[0] {
            0 => Ok(InternalDevice(node)),
            1 => Ok(LeafDevice(node)),
            2 => Ok(InternalMeasurement(node)),
            _ => Ok(LeafMeasurement(node)),
        }
    }
}

impl MetadataIndexEntry {
    fn new(data: &mut Cursor<Vec<u8>>) -> Result<Self> {
        let name = data.read_varint_string().context(ReadVarInt)?;
        let offset = data.read_i64::<BigEndian>().context(ReadCursorData)?;
        Ok(Self {
            name,
            offset,
        })
    }
}
