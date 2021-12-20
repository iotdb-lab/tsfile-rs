use byteorder::{BigEndian, ReadBytesExt};
use snafu::{ResultExt, Snafu};
use std::io;
use std::io::{Cursor, Read};
use varint::VarintRead;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to read unsigned VarInt32: {}", source))]
    ReadUnsignedVarInt { source: io::Error },
    #[snafu(display("Unable to convert data to UTF8-String: {}", source))]
    ReadUTF8String { source: io::Error },
    #[snafu(display("Unable to read fixed length {} data: {}", len, source))]
    ReadFixedLengthData { len: usize, source: io::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub trait VarIntReader: VarintRead {
    fn read_varint_string(&mut self) -> Result<String> {
        let varint = self.read_unsigned_varint_32().context(ReadUnsignedVarInt)?;
        let mut len: usize = (varint >> 1) as usize;
        if (varint & 1) != 0 {
            len = !len;
        }

        let mut data: Vec<u8> = vec![0; len];
        self.read_exact(&mut data)
            .context(ReadFixedLengthData { len })?;
        Ok(String::from_utf8(data).context(ReadUTF8String)?)
    }
}

pub trait PackWidthReader: Read {
    fn read_pack_width_long(&mut self, pos: i32, width: i32) -> Result<i64> {
        let mut data: Vec<u8> = vec![0; width as usize];
        self.read_exact(&mut data).context(ReadFixedLengthData {
            len: width as usize,
        })?;

        let mut temp: i32;
        let mut value: i64 = 0;
        for i in 0..width {
            temp = (pos + width - 1 - i) / 8;
            let mut offset = pos + width - 1 - i;
            offset %= 8;
            let byte = if ((0xff & data[temp as usize]) & (1 << (7 - offset))) != 0 {
                1
            } else {
                0
            };
            let offset = i % 64;
            value = if byte == 1 {
                value | (1 << (offset))
            } else {
                value & (1 << (offset))
            }
        }
        Ok(value)
    }
}

impl VarIntReader for Cursor<Vec<u8>> {}

impl PackWidthReader for Cursor<Vec<u8>> {}
