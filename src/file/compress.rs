use std::io;
use std::io::Cursor;

use snap::raw::Decoder;

pub trait Snappy {
    fn compress(&mut self) -> Vec<u8>;
    fn un_compress(&mut self) -> Vec<u8>;
}


impl Snappy for Cursor<Vec<u8>> {
    fn compress(&mut self) -> Vec<u8> {
        let encode_data = Vec::new();
        let mut encoder = snap::write::FrameEncoder::new(encode_data);
        io::copy(self, &mut encoder);
        encoder.get_ref().to_vec()
    }

    fn un_compress(&mut self) -> Vec<u8> {
        Decoder::new().decompress_vec(self.get_ref()).expect("123")
    }
}

