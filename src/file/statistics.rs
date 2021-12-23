use std::borrow::BorrowMut;
use std::convert::TryFrom;
use std::io::Cursor;

use crate::utils::cursor;
use crate::utils::cursor::VarIntReader;
use byteorder::{BigEndian, ReadBytesExt};
use snafu::{ResultExt, Snafu};
use varint::VarintRead;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to read unsigned VarInt data: {}", source))]
    ReadUnsignedVarInt { source: std::io::Error },
    #[snafu(display("Unable to read cursor data: {}", source))]
    ReadCursorData { source: std::io::Error },
    #[snafu(display("Unable to read cursor data: {}", source))]
    ReadVarData { source: cursor::Error },
}

#[derive(Debug)]
pub enum Statistic {
    Boolean(BooleanStatistics),
    Int32(IntegerStatistics),
    Int64(LongStatistics),
    FLOAT(FloatStatistics),
    DOUBLE(DoubleStatistics),
    TEXT(BinaryStatistics),
}

#[derive(Debug)]
pub struct StatisticHeader {
    is_empty: bool,
    count: i32,
    start_time: i64,
    end_time: i64,
}

#[derive(Debug)]
pub struct BinaryStatistics {
    header: StatisticHeader,
    first_value: String,
    last_value: String,
}

#[derive(Debug)]
pub struct BooleanStatistics {
    header: StatisticHeader,
    first_value: bool,
    last_value: bool,
    sum_value: i64,
}

#[derive(Debug)]
pub struct IntegerStatistics {
    header: StatisticHeader,
    min_value: i32,
    max_value: i32,
    first_value: i32,
    last_value: i32,
    sum_value: i64,
}

#[derive(Debug)]
pub struct LongStatistics {
    header: StatisticHeader,
    min_value: i64,
    max_value: i64,
    first_value: i64,
    last_value: i64,
    sum_value: f64,
}

#[derive(Debug)]
pub struct DoubleStatistics {
    header: StatisticHeader,
    min_value: f64,
    max_value: f64,
    first_value: f64,
    last_value: f64,
    sum_value: f64,
}

#[derive(Debug)]
pub struct FloatStatistics {
    header: StatisticHeader,
    min_value: f32,
    max_value: f32,
    first_value: f32,
    last_value: f32,
    sum_value: f64,
}

impl TryFrom<&mut Cursor<Vec<u8>>> for StatisticHeader {
    type Error = Error;

    fn try_from(cursor: &mut Cursor<Vec<u8>>) -> Result<Self, Self::Error> {
        let count = cursor
            .read_unsigned_varint_32()
            .context(ReadUnsignedVarInt)? as i32;
        let start_time = cursor.read_i64::<BigEndian>().context(ReadCursorData)?;
        let end_time = cursor.read_i64::<BigEndian>().context(ReadCursorData)?;
        Ok(Self {
            count,
            start_time,
            end_time,
            is_empty: false,
        })
    }
}

impl TryFrom<&mut Cursor<Vec<u8>>> for BooleanStatistics {
    type Error = Error;

    fn try_from(cursor: &mut Cursor<Vec<u8>>) -> Result<Self, Self::Error> {
        Ok(Self {
            header: StatisticHeader::try_from(cursor.borrow_mut())?,
            first_value: cursor.read_bool().context(ReadVarData)?,
            last_value: cursor.read_bool().context(ReadVarData)?,
            sum_value: cursor.read_i64::<BigEndian>().context(ReadCursorData)?,
        })
    }
}

impl TryFrom<&'_ mut Cursor<Vec<u8>>> for IntegerStatistics {
    type Error = Error;

    fn try_from(cursor: &'_ mut Cursor<Vec<u8>>) -> Result<Self, Self::Error> {
        Ok(Self {
            header: StatisticHeader::try_from(cursor.borrow_mut())?,
            min_value: cursor.read_i32::<BigEndian>().context(ReadCursorData)?,
            max_value: cursor.read_i32::<BigEndian>().context(ReadCursorData)?,
            first_value: cursor.read_i32::<BigEndian>().context(ReadCursorData)?,
            last_value: cursor.read_i32::<BigEndian>().context(ReadCursorData)?,
            sum_value: cursor.read_i64::<BigEndian>().context(ReadCursorData)?,
        })
    }
}

impl TryFrom<&'_ mut Cursor<Vec<u8>>> for FloatStatistics {
    type Error = Error;

    fn try_from(cursor: &'_ mut Cursor<Vec<u8>>) -> Result<Self, Self::Error> {
        Ok(Self {
            header: StatisticHeader::try_from(cursor.borrow_mut())?,
            min_value: cursor.read_f32::<BigEndian>().context(ReadCursorData)?,
            max_value: cursor.read_f32::<BigEndian>().context(ReadCursorData)?,
            first_value: cursor.read_f32::<BigEndian>().context(ReadCursorData)?,
            last_value: cursor.read_f32::<BigEndian>().context(ReadCursorData)?,
            sum_value: cursor.read_f64::<BigEndian>().context(ReadCursorData)?,
        })
    }
}

impl TryFrom<&'_ mut Cursor<Vec<u8>>> for DoubleStatistics {
    type Error = Error;

    fn try_from(cursor: &'_ mut Cursor<Vec<u8>>) -> Result<Self, Self::Error> {
        Ok(Self {
            header: StatisticHeader::try_from(cursor.borrow_mut())?,
            min_value: cursor.read_f64::<BigEndian>().context(ReadCursorData)?,
            max_value: cursor.read_f64::<BigEndian>().context(ReadCursorData)?,
            first_value: cursor.read_f64::<BigEndian>().context(ReadCursorData)?,
            last_value: cursor.read_f64::<BigEndian>().context(ReadCursorData)?,
            sum_value: cursor.read_f64::<BigEndian>().context(ReadCursorData)?,
        })
    }
}

impl TryFrom<&'_ mut Cursor<Vec<u8>>> for LongStatistics {
    type Error = Error;

    fn try_from(cursor: &'_ mut Cursor<Vec<u8>>) -> Result<Self, Self::Error> {
        Ok(Self {
            header: StatisticHeader::try_from(cursor.borrow_mut())?,
            min_value: cursor.read_i64::<BigEndian>().context(ReadCursorData)?,
            max_value: cursor.read_i64::<BigEndian>().context(ReadCursorData)?,
            first_value: cursor.read_i64::<BigEndian>().context(ReadCursorData)?,
            last_value: cursor.read_i64::<BigEndian>().context(ReadCursorData)?,
            sum_value: cursor.read_f64::<BigEndian>().context(ReadCursorData)?,
        })
    }
}

impl TryFrom<&'_ mut Cursor<Vec<u8>>> for BinaryStatistics {
    type Error = Error;

    fn try_from(_value: &'_ mut Cursor<Vec<u8>>) -> Result<Self, Self::Error> {
        todo!()
    }
}
