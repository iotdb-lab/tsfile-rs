use std::io::{Read, Seek, SeekFrom, Result};
use std::cell::RefCell;
use core::{fmt, cmp};
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