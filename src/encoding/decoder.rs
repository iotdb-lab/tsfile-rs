use std::io::Cursor;
use varint::VarintRead;
use crate::error::Result;
use crate::utils::io::{BigEndianReader, PackWidthReader};

#[derive(Debug)]
pub enum Field {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    FLOAT(f32),
    DOUBLE(f64),
    TEXT(Vec<u8>),
}

pub trait BinaryDecoder {
    fn new() -> Self;
    fn decode(&self, data: &mut Cursor<Vec<u8>>) -> Result<Vec<Field>>;
}

#[derive(Debug)]
pub struct LongBinaryDecoder {}


impl BinaryDecoder for LongBinaryDecoder {
    fn new() -> Self {
        Self {}
    }

    fn decode(&self, data: &mut Cursor<Vec<u8>>) -> Result<Vec<Field>> {
        let packNum = data.read_big_endian_i32();
        let packWidth = data.read_big_endian_i32();
        let minDeltaBase = data.read_big_endian_i64();
        let previous = data.read_big_endian_i64();
        let mut result = Vec::with_capacity(packNum as usize);

        for i in 0..packNum {
            let value = data.read_pack_width_long(packWidth * i, packWidth);
            result.push(Field::Int64(previous + minDeltaBase + value));
        };

        Ok(result)
    }
}