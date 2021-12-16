use std::borrow::BorrowMut;
use std::io::{Cursor, Read};
use std::sync::Arc;

use byteorder::{BigEndian, ReadBytesExt};
use varint::VarintRead;
use crate::encoding::decoder::{BinaryDecoder, LongBinaryDecoder};

use crate::error::{Result, TsFileError};
use crate::file::compress::Snappy;
use crate::file::metadata::{ChunkMetadata, TimeseriesMetadata, TSDataType};
use crate::file::reader::{ChunkReader, PageReader, RowIter, SectionReader, SensorReader};
use crate::file::statistics::{
    BinaryStatistics, BooleanStatistics, DoubleStatistics, FloatStatistics, IntegerStatistics,
    LongStatistics, Statistic,
};
use crate::utils::io::*;

#[derive(Debug)]
pub struct TsFileSensorReader<R: SectionReader> {
    reader: Arc<R>,
    meta: Vec<ChunkMetadata>,
}

impl<R: SectionReader> TsFileSensorReader<R> {
    pub fn new(reader: Arc<R>, meta: Vec<TimeseriesMetadata>) -> Self {
        let chunks: Vec<ChunkMetadata> = meta
            .into_iter()
            .flat_map(|x| x.chunk_metadata_list())
            .collect();

        Self {
            reader,
            meta: chunks,
        }
    }
}

impl<R: 'static + SectionReader> SensorReader for TsFileSensorReader<R> {
    fn metadata(&self) -> &Vec<ChunkMetadata> {
        &self.meta
    }

    fn number_of_chunks(&self) -> usize {
        self.meta.len()
    }

    fn get_chunk_reader(
        &self,
        i: usize,
    ) -> Result<Box<dyn ChunkReader<Item=Box<dyn PageReader>>>> {
        match &self.meta.get(i) {
            None => Err(TsFileError::General("123".to_string())),
            Some(chunk) => {
                let offset = chunk.offset_chunk_header();
                //TODO 多读取了一部分数据
                let mut header_reader = self.reader.get_cursor(offset as u64, 1 * 1024)?;

                let chunk_header = ChunkHeader::try_from(header_reader.borrow_mut())?;

                let first_page = header_reader.position() + offset as u64;

                Ok(Box::new(DefaultChunkReader::new(
                    self.reader
                        .get_cursor(first_page, chunk_header.data_size as usize)?,
                    chunk_header,
                    chunk.statistic(),
                )?))
            }
        }
    }

    fn get_page_iter(&self, predicate: Box<dyn Fn(u64) -> bool>) -> crate::error::Result<RowIter> {
        todo!()
    }
}

pub struct DefaultChunkReader {
    cursor: Cursor<Vec<u8>>,
    header: ChunkHeader,
    pages: Vec<Box<dyn PageReader>>,
    statistic: Arc<Statistic>,
}

impl DefaultChunkReader {
    pub fn new(
        mut cursor: Cursor<Vec<u8>>,
        header: ChunkHeader,
        statistic: Arc<Statistic>,
    ) -> Result<Self> {
        let mut pages: Vec<Box<dyn PageReader>> = Vec::new();
        while cursor.position() < header.data_size as u64 {
            //pages
            match header.chunk_type {
                //chunk only have one page
                5 => {
                    let uncompressed_size = cursor.read_unsigned_varint_32()?;
                    let compressed_size = cursor.read_unsigned_varint_32()?;

                    let mut data = vec![0; compressed_size as usize];
                    cursor.read_exact(&mut data)?;
                    pages.push(Box::new(DefaultPageReader {
                        header: PageHeader::new(
                            uncompressed_size,
                            compressed_size,
                            statistic.clone(),
                        ),
                        time_decoder: LongBinaryDecoder::new(),
                        data: Cursor::new(Vec::from(data)),
                    }));
                }
                _ => {
                    let uncompressed_size = cursor.read_unsigned_varint_32()?;
                    let compressed_size = cursor.read_unsigned_varint_32()?;
                    let page_statistic = Arc::new(match *statistic {
                        Statistic::Boolean(_) => {
                            Statistic::Boolean(BooleanStatistics::try_from(cursor.borrow_mut())?)
                        }
                        Statistic::Int32(_) => {
                            Statistic::Int32(IntegerStatistics::try_from(cursor.borrow_mut())?)
                        }
                        Statistic::Int64(_) => {
                            Statistic::Int64(LongStatistics::try_from(cursor.borrow_mut())?)
                        }
                        Statistic::FLOAT(_) => {
                            Statistic::FLOAT(FloatStatistics::try_from(cursor.borrow_mut())?)
                        }
                        Statistic::DOUBLE(_) => {
                            Statistic::DOUBLE(DoubleStatistics::try_from(cursor.borrow_mut())?)
                        }
                        Statistic::TEXT(_) => {
                            Statistic::TEXT(BinaryStatistics::try_from(cursor.borrow_mut())?)
                        }
                    });

                    let mut data = vec![0; compressed_size as usize];
                    cursor.read_exact(&mut data)?;

                    pages.push(Box::new(DefaultPageReader {
                        header: PageHeader::new(uncompressed_size, compressed_size, page_statistic),
                        time_decoder: LongBinaryDecoder::new(),
                        data: Cursor::new(Vec::from(data)),
                    }));
                }
            }
        }

        Ok(Self {
            cursor,
            header,
            pages,
            statistic,
        })
    }
}

impl Iterator for DefaultChunkReader {
    type Item = Box<dyn PageReader>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pages.len() == 0 {
            ()
        }

        self.pages.pop()
    }
}

impl ChunkReader for DefaultChunkReader {}

impl PageReader for DefaultPageReader {
    fn header(&self) -> &PageHeader {
        &self.header
    }

    fn data(&self) {
        println!("comp size:{:?},un_comp size:{:?}", self.header.compressed_size, self.header.uncompressed_size);
        let mut data = Cursor::new(self.data.un_compress());
        let time_len = data.read_unsigned_varint_32().expect("123");

        let mut time_data: Vec<u8> = vec![0; time_len as usize];
        data.read_exact(&mut time_data);
        let result = LongBinaryDecoder::new().decode(&mut Cursor::new(time_data)).expect("123");

        println!("time_len:{:?}, result:{:?}", time_len, result);
    }
}

#[derive(Debug)]
pub struct DefaultPageReader {
    header: PageHeader,
    time_decoder: LongBinaryDecoder,
    data: Cursor<Vec<u8>>,
}

#[derive(Debug)]
pub struct PageHeader {
    uncompressed_size: u32,
    compressed_size: u32,
    statistics: Arc<Statistic>,
}

impl PageHeader {
    pub fn new(uncompressed_size: u32, compressed_size: u32, statistics: Arc<Statistic>) -> Self {
        Self {
            uncompressed_size,
            compressed_size,
            statistics,
        }
    }
}

pub struct ChunkHeader {
    chunk_type: u8,
    measurement_id: String,
    data_size: u32,
    data_type: TSDataType,
    compression_type: CompressionType,
    encoding_type: TSEncoding,
}

impl TryFrom<&mut Cursor<Vec<u8>>> for ChunkHeader {
    type Error = TsFileError;

    fn try_from(cursor: &mut Cursor<Vec<u8>>) -> std::result::Result<Self, Self::Error> {
        //mark
        let chunk_type = cursor.read_u8()?;
        let measurement_id = cursor.read_varint_string()?;
        let data_size = cursor.read_unsigned_varint_32()?;
        let data_type = TSDataType::new(cursor.read_u8()?)?;
        let compression_type = CompressionType::new(cursor.read_u8()?);
        let encoding_type = TSEncoding::new(cursor.read_u8()?);

        Ok(Self {
            chunk_type,
            measurement_id,
            data_size,
            data_type,
            compression_type,
            encoding_type,
        })
    }
}

pub enum CompressionType {
    UNCOMPRESSED,
    SNAPPY,
    GZIP,
    LZO,
    SDT,
    PAA,
    PLA,
    LZ4,
}

impl CompressionType {
    pub fn new(id: u8) -> Self {
        match id {
            0 => Self::UNCOMPRESSED,
            1 => Self::SNAPPY,
            2 => Self::GZIP,
            3 => Self::LZO,
            4 => Self::SDT,
            5 => Self::PAA,
            6 => Self::PLA,
            _ => Self::LZ4,
        }
    }
}

pub enum TSEncoding {
    PLAIN,
    PLAIN_DICTIONARY,
    RLE,
    DIFF,
    TS_2DIFF,
    BITMAP,
    GORILLA_V1,
    REGULAR,
    GORILLA,
}

impl TSEncoding {
    pub fn new(id: u8) -> Self {
        match id {
            0 => Self::PLAIN,
            1 => Self::PLAIN_DICTIONARY,
            2 => Self::RLE,
            3 => Self::DIFF,
            4 => Self::TS_2DIFF,
            5 => Self::BITMAP,
            6 => Self::GORILLA_V1,
            7 => Self::REGULAR,
            _ => Self::GORILLA,
        }
    }
}
