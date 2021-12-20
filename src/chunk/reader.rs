use std::borrow::BorrowMut;
use std::io::{Cursor, Read};
use std::sync::Arc;

use crate::encoding::decoder::{Decoder, Field, IntPlainDecoder, LongBinaryDecoder};
use byteorder::ReadBytesExt;
use snafu::{ensure, OptionExt, ResultExt};
use varint::VarintRead;

use crate::chunk::reader::Error::GetCursor;
use crate::file::compress;
use crate::file::compress::Snappy;
use crate::file::metadata::{ChunkMetadata, TSDataType, TimeseriesMetadata};
use crate::file::reader::{ChunkReader, PageReader, RowIter, SectionReader, SensorReader};
use crate::file::statistics::{
    BinaryStatistics, BooleanStatistics, DoubleStatistics, FloatStatistics, IntegerStatistics,
    LongStatistics, Statistic,
};
use crate::utils::io::*;
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to decompress chunk data: {}", source))]
    DecompressChunkData { source: compress::Error },
    #[snafu(display("Unable to decompress chunk data: {}", source))]
    DecodePageData { source: compress::Error },
    #[snafu(display("Unable to get chunk reader i:{}, max length:{}, {}", i, len, source))]
    GetChunkReaderI {
        i: i32,
        len: usize,
        source: compress::Error,
    },
    #[snafu(display("Unable to get cursor from reader, {}", source))]
    GetCursor { source: crate::file::reader::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

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
    ) -> Result<Box<dyn ChunkReader<Item = Box<dyn PageReader>>>> {
        let chunk_meta = &self.meta.get(i);
        ensure!(
            !chunk_meta.is_some(),
            GetChunkReaderI {
                i,
                len: self.meta.len()
            }
        );
        let chunk = chunk_meta.unwrap();
        let offset = chunk.offset_chunk_header();
        //TODO 多读取了一部分数据
        let mut header_reader = self
            .reader
            .get_cursor(offset as u64, 1 * 1024)
            .context(GetCursor {})?;
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
                        value_decoder: match header.data_type {
                            TSDataType::Int32 => Box::new(IntPlainDecoder::new()),
                            _ => Box::new(IntPlainDecoder::new()),
                        },
                        data: Cursor::new(data),
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
                        value_decoder: match header.data_type {
                            TSDataType::Int32 => Box::new(IntPlainDecoder::new()),
                            _ => Box::new(IntPlainDecoder::new()),
                        },
                        data: Cursor::new(data),
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
        if self.pages.is_empty() {
            return None;
        }

        self.pages.pop()
    }
}

impl ChunkReader for DefaultChunkReader {}

impl PageReader for DefaultPageReader {
    fn header(&self) -> &PageHeader {
        &self.header
    }

    fn data(&self) -> Result<(Vec<Field>, Vec<Field>)> {
        let mut data = Cursor::new(self.data.un_compress().context(DecompressChunkData)?);
        let time_len = data.read_unsigned_varint_32().expect("123");

        let mut time_data: Vec<u8> = vec![0; time_len as usize];
        data.read_exact(&mut time_data);
        let time = LongBinaryDecoder::new()
            .decode(&mut Cursor::new(time_data))
            .expect("123");
        let data = self
            .value_decoder
            .decode(&mut data)
            .context(DecodePageData)?;
        Ok((time, data))
    }
}

pub struct DefaultPageReader {
    header: PageHeader,
    value_decoder: Box<dyn Decoder>,
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
    Uncompressed,
    Snappy,
    Gzip,
    Lzo,
    Sdt,
    Paa,
    Pla,
    LZ4,
}

impl CompressionType {
    pub fn new(id: u8) -> Self {
        match id {
            0 => Self::Uncompressed,
            1 => Self::Snappy,
            2 => Self::Gzip,
            3 => Self::Lzo,
            4 => Self::Sdt,
            5 => Self::Paa,
            6 => Self::Pla,
            _ => Self::LZ4,
        }
    }
}

pub enum TSEncoding {
    Plain,
    PlainDictionary,
    Rle,
    Diff,
    Ts2diff,
    Bitmap,
    GorillaV1,
    Regular,
    Gorilla,
}

impl TSEncoding {
    pub fn new(id: u8) -> Self {
        match id {
            0 => Self::Plain,
            1 => Self::PlainDictionary,
            2 => Self::Rle,
            3 => Self::Diff,
            4 => Self::Ts2diff,
            5 => Self::Bitmap,
            6 => Self::GorillaV1,
            7 => Self::Regular,
            _ => Self::Gorilla,
        }
    }
}
