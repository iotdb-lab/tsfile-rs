use std::io::{Read, Cursor};

use crate::FOOTER_SIZE;
use crate::error::Result;
use crate::metadata::TsFileMetadata;
use crate::reader::ChunkReader;
use crate::error::TsFileError;
use byteorder::{LittleEndian, ByteOrder, BigEndian};
use std::borrow::BorrowMut;

pub fn parser_metadata<R: ChunkReader>(chunk_reader: R) -> Result<TsFileMetadata> {
    let file_size = chunk_reader.len();
    if file_size < (FOOTER_SIZE as u64) {
        return Err(general_err!(
            "Invalid TsFile. Size is smaller than footer"
        ));
    }

    let mut result = chunk_reader
        .get_read(file_size - FOOTER_SIZE as u64, FOOTER_SIZE)?;

    let mut end_buf = vec![0; FOOTER_SIZE];
    result.read_exact(&mut end_buf)?;

    if end_buf[4..] != [b'T', b's', b'F', b'i', b'l', b'e'] {
        return Err(general_err!("Invalid TsFile. Corrupt footer"));
    }

    let metadata_len = BigEndian::read_i32(&end_buf[0..4]);
    if metadata_len < 0 {
        return Err(general_err!(
            "Invalid TsFile. Metadata length is less than zero ({})",
            metadata_len
        ));
    }

    let footer_metadata_pos = file_size - FOOTER_SIZE as u64 - metadata_len as u64;

    let metadata_reader = chunk_reader
        .get_read(footer_metadata_pos, metadata_len as usize)?;
    let mut metadata_reader = Box::new(metadata_reader);

    let mut data = vec![0; metadata_len as usize];
    let result = metadata_reader.read(&mut data)?;
    if result != metadata_len as usize {
        return Err(general_err!("Invalid TsFile. TsFileMetadata error"));
    }

    TsFileMetadata::parser(Cursor::new(data))
}