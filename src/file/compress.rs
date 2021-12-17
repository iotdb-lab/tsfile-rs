use std::io::Cursor;

use snap::raw::Decoder;

pub trait Snappy {
    // fn compress(&self) -> Vec<u8>;
    fn un_compress(&self) -> Vec<u8>;
}

impl Snappy for Cursor<Vec<u8>> {
    fn un_compress(&self) -> Vec<u8> {
        Decoder::new().decompress_vec(self.get_ref()).expect("123")
    }
}
