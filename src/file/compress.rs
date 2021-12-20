use snafu::{ResultExt, Snafu};
use snap::raw::Decoder;
use std::io::Cursor;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to decompress vec: {}", source))]
    DecompressVec { source: snap::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub trait Snappy {
    // fn compress(&self) -> Vec<u8>;
    fn un_compress(&self) -> Result<Vec<u8>>;
}

impl Snappy for Cursor<Vec<u8>> {
    fn un_compress(&self) -> Result<Vec<u8>> {
        Decoder::new()
            .decompress_vec(self.get_ref())
            .context(DecompressVec)
    }
}
