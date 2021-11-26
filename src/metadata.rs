use crate::metadata::index::MetadataIndexNodeType;
use crate::metadata::bloom_filter::BloomFilter;
use std::io::{Read, Cursor};
use crate::error::Result;
use byteorder::{BigEndian, ByteOrder, LittleEndian, ReadBytesExt};


pub mod index;
mod bloom_filter;

#[derive(Debug)]
pub struct TsFileMetadata {
    size: u64,
    fileMeta: FileMeta,
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

