use std::io::{Cursor, Read};

use byteorder::{BigEndian, ByteOrder};

use crate::error::Result;
use crate::error::TsFileError;
use crate::file::metadata::TsFileMetadata;
use crate::file::reader::SectionReader;
use crate::FOOTER_SIZE;

pub fn parser_metadata<R: SectionReader>(reader: &R) -> Result<TsFileMetadata> {
    let file_size = reader.len();
    if file_size < (FOOTER_SIZE as u64) {
        return Err(general_err!(
            "Invalid TsFile. Size is smaller than footer"
        ));
    }

    let mut result = reader
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

    let metadata_reader = reader
        .get_read(footer_metadata_pos, metadata_len as usize)?;
    let mut metadata_reader = Box::new(metadata_reader);

    let mut data = vec![0; metadata_len as usize];
    let result = metadata_reader.read(&mut data)?;
    if result != metadata_len as usize {
        return Err(general_err!("Invalid TsFile. TsFileMetadata error"));
    }

    TsFileMetadata::parser(Cursor::new(data))
}