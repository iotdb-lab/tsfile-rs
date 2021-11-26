use std::convert::TryInto;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use crate::error::Result;
use core::cmp;

pub trait Length {
    fn len(&self) -> u64;
}

pub trait TryClone: Sized {
    fn try_clone(&self) -> std::io::Result<Self>;
}

impl Length for File {
    fn len(&self) -> u64 {
        self.metadata().map(|m| m.len()).unwrap_or(0u64)
    }
}

impl TryClone for File {
    fn try_clone(&self) -> std::io::Result<Self> {
        self.try_clone()
    }
}

pub trait ChunkReader: Length {
    type T: Read;
    fn get_read(&self, start: u64, length: usize) -> Result<Self::T>;
}

impl ChunkReader for File {
    type T = FileSource<File>;

    fn get_read(&self, start: u64, length: usize) -> Result<Self::T> {
        Ok(FileSource::new(self, start, length))
    }
}

pub trait TsFileReader: Read + Seek + Length + TryClone {}

impl<T: Read + Seek + Length + TryClone> TsFileReader for T {}


pub struct FileSource<R: TsFileReader> {
    reader: R,
    start: u64,
    end: u64,
    buf: Vec<u8>,
    buf_pos: usize,
}

impl<R: TsFileReader> FileSource<R> {
    pub fn new(fd: &R, start: u64, len: usize) -> Self {
        let mut reader = fd.try_clone().unwrap();
        reader.seek(SeekFrom::Start(start));
        Self {
            reader,
            start,
            end: start + len as u64,
            buf: vec![0_u8; 1024],
            buf_pos: 0,
        }
    }
}

impl<R: TsFileReader> Read for FileSource<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let result = self.reader.read(buf)?;

        Ok(result)
    }
}