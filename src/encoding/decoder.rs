use std::io::Cursor;

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

pub trait Decoder {
    fn new() -> Self
        where
            Self: Sized;
    fn decode(&self, data: &mut Cursor<Vec<u8>>) -> Result<Vec<Field>>;
}

pub trait BinaryDelta: Decoder {}

pub struct LongBinaryDecoder {}

impl Decoder for LongBinaryDecoder {
    fn new() -> Self {
        Self {}
    }

    fn decode(&self, data: &mut Cursor<Vec<u8>>) -> Result<Vec<Field>> {
        let pack_num = data.read_big_endian_i32();
        let pack_width = data.read_big_endian_i32();
        let min_delta_base = data.read_big_endian_i64();
        let mut previous = data.read_big_endian_i64();
        let mut result = Vec::with_capacity(pack_num as usize);

        for i in 0..pack_num {
            let value = data.read_pack_width_long(pack_width * i, pack_width);
            previous = previous + min_delta_base + value;
            result.push(Field::Int64(previous));
        }

        Ok(result)
    }
}

pub trait PlainDecoder: Decoder {}

pub struct IntPlainDecoder {}

impl Decoder for IntPlainDecoder {
    fn new() -> Self {
        Self {}
    }

    fn decode(&self, data: &mut Cursor<Vec<u8>>) -> Result<Vec<Field>> {
        let mut result = Vec::new();
        while data.position() < data.get_ref().len() as u64 {
            result.push(Field::Int32(data.read_big_endian_i32()));
        }

        Ok(result)
    }
}
