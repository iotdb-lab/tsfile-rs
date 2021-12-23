use std::io::{Cursor, Read};

use byteorder::{BigEndian, ByteOrder};
use snafu::{ensure, ResultExt};

use crate::file::metadata::TsFileMetadata;
use crate::file::reader::SectionReader;
use crate::FOOTER_SIZE;

use crate::file::metadata;
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to parser metadata: {}", source))]
    ParserMetadata { source: metadata::Error },
    #[snafu(display("Invalid TsFile. {}", detail))]
    InvalidTsFile { detail: String },
    #[snafu(display("Unable to read cursor: {}", source))]
    ReadCursorData { source: std::io::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub fn parser_metadata<R: SectionReader>(reader: &R) -> Result<TsFileMetadata> {
    let file_size = reader.len();
    ensure!(
        file_size >= (FOOTER_SIZE as u64),
        InvalidTsFile {
            detail: "Size is smaller than footer".to_string()
        }
    );

    let mut result = reader.get_read(file_size - FOOTER_SIZE as u64, FOOTER_SIZE);

    let mut end_buf = vec![0; FOOTER_SIZE];
    result.read_exact(&mut end_buf).context(ReadCursorData)?;

    ensure!(
        end_buf[4..] == [b'T', b's', b'F', b'i', b'l', b'e'],
        InvalidTsFile {
            detail: "Corrupt footer".to_string()
        }
    );

    let metadata_len = BigEndian::read_i32(&end_buf[0..4]);
    ensure!(
        metadata_len >= 0,
        InvalidTsFile {
            detail: " Metadata length is less than zero".to_string()
        }
    );

    let footer_metadata_pos = file_size - FOOTER_SIZE as u64 - metadata_len as u64;

    let metadata_reader = reader.get_read(footer_metadata_pos, metadata_len as usize);
    let mut metadata_reader = Box::new(metadata_reader);

    let mut data = vec![0; metadata_len as usize];
    let result = metadata_reader.read(&mut data).context(ReadCursorData)?;
    ensure!(
        result == metadata_len as usize,
        InvalidTsFile {
            detail: "TsFileMetadata error".to_string()
        }
    );
    TsFileMetadata::parser(Cursor::new(data)).context(ParserMetadata)
}
