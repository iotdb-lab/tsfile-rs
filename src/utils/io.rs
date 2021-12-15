use core::{cmp, fmt};
use std::cell::RefCell;
use std::io::{Cursor, Read, Result, Seek, SeekFrom};

use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use varint::VarintRead;

use crate::file::reader::{Length, TryClone};

const DEFAULT_BUF_SIZE: usize = 8 * 1024;

pub trait TsFileReader: Read + Seek + Length + TryClone {}

impl<T: Read + Seek + Length + TryClone> TsFileReader for T {}

pub struct FileSource<R: TsFileReader> {
    reader: RefCell<R>,
    start: u64,
    end: u64,
    buf: Vec<u8>,
    buf_pos: usize,
    buf_cap: usize,
}

pub trait Position {
    /// Returns position in the stream.
    fn pos(&self) -> u64;
}

impl<R: TsFileReader> fmt::Debug for FileSource<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileSource")
            .field("reader", &"OPAQUE")
            .field("start", &self.start)
            .field("end", &self.end)
            .field("buf.len", &self.buf.len())
            .field("buf_pos", &self.buf_pos)
            .field("buf_cap", &self.buf_cap)
            .finish()
    }
}

impl<R: TsFileReader> FileSource<R> {
    pub fn new(fd: &R, start: u64, length: usize) -> Self {
        let reader = RefCell::new(fd.try_clone().unwrap());
        Self {
            reader,
            start,
            end: start + length as u64,
            buf: vec![0_u8; DEFAULT_BUF_SIZE],
            buf_pos: 0,
            buf_cap: 0,
        }
    }

    fn fill_inner_buf(&mut self) -> Result<&[u8]> {
        if self.buf_pos >= self.buf_cap {
            // If we've reached the end of our internal buffer then we need to fetch
            // some more data from the underlying reader.
            // Branch using `>=` instead of the more correct `==`
            // to tell the compiler that the pos..cap slice is always valid.
            debug_assert!(self.buf_pos == self.buf_cap);
            let mut reader = self.reader.borrow_mut();
            reader.seek(SeekFrom::Start(self.start))?; // always seek to start before reading
            self.buf_cap = reader.read(&mut self.buf)?;
            self.buf_pos = 0;
        }
        Ok(&self.buf[self.buf_pos..self.buf_cap])
    }

    fn skip_inner_buf(&mut self, buf: &mut [u8]) -> Result<usize> {
        // discard buffer
        self.buf_pos = 0;
        self.buf_cap = 0;
        // read directly into param buffer
        let mut reader = self.reader.borrow_mut();
        reader.seek(SeekFrom::Start(self.start))?; // always seek to start before reading
        let nread = reader.read(buf)?;
        self.start += nread as u64;
        Ok(nread)
    }
}

impl<R: TsFileReader> Read for FileSource<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let bytes_to_read = cmp::min(buf.len(), (self.end - self.start) as usize);
        let buf = &mut buf[0..bytes_to_read];

        // If we don't have any buffered data and we're doing a massive read
        // (larger than our internal buffer), bypass our internal buffer
        // entirely.
        if self.buf_pos == self.buf_cap && buf.len() >= self.buf.len() {
            return self.skip_inner_buf(buf);
        }
        let nread = {
            let mut rem = self.fill_inner_buf()?;
            // copy the data from the inner buffer to the param buffer
            rem.read(buf)?
        };
        // consume from buffer
        self.buf_pos = cmp::min(self.buf_pos + nread, self.buf_cap);

        self.start += nread as u64;
        Ok(nread)
    }
}

impl<R: TsFileReader> Position for FileSource<R> {
    fn pos(&self) -> u64 {
        self.start
    }
}

impl<R: TsFileReader> Length for FileSource<R> {
    fn len(&self) -> u64 {
        self.end - self.start
    }
}

pub trait BigEndianReader: Read {
    fn read_big_endian_i32(&mut self) -> i32 {
        let mut buffer = vec![0; 4];
        self.read_exact(&mut buffer);
        BigEndian::read_i32(&buffer)
    }

    fn read_big_endian_i64(&mut self) -> i64 {
        let mut vec = vec![0; 8];
        self.read_exact(&mut vec);
        BigEndian::read_i64(&vec)
    }

    fn read_bool(&mut self) -> bool {
        let result = self.read_u8().unwrap();
        match result {
            0 => false,
            _ => true,
        }
    }
}

pub trait VarIntReader: VarintRead {
    fn read_varint_string(&mut self) -> Result<String> {
        match self.read_unsigned_varint_32() {
            Ok(len) => {
                let mut x: i32 = (len >> 1) as i32;
                if (len & 1) != 0 {
                    x = !x;
                }

                let mut data: Vec<u8> = vec![0; x as usize];
                self.read_exact(&mut data)?;
                Ok(String::from_utf8(data).unwrap())
            }
            Err(e) => Err(e),
        }
    }

    fn read_varint_string_len(&mut self) -> Result<(u32, String)> {
        match self.read_unsigned_varint_32() {
            Ok(len) => {
                let mut x: i32 = (len >> 1) as i32;
                if (len & 1) != 0 {
                    x = !x;
                }

                let mut data: Vec<u8> = vec![0; x as usize];
                self.read_exact(&mut data)?;
                Ok((len, String::from_utf8(data).unwrap()))
            }
            Err(e) => Err(e),
        }
    }
}

impl VarIntReader for Cursor<Vec<u8>> {}

impl BigEndianReader for Cursor<Vec<u8>> {}
