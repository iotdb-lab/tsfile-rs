const MAGIC_STRING: &str = "TsFile";
const VERSION_NUMBER_V2: &str = "000002";
const VERSION_NUMBER: u8 = 0x03;
const FOOTER_SIZE: usize = 10;

#[macro_use]
pub mod error;
pub mod file;
pub mod chunk;
mod utils;


mod tests {
    use crate::file::tsfile_search_reader::TsFileSearchReader;
    use std::convert::TryFrom;
    use std::fs::read;
    use crate::file::reader::FileReader;

    #[test]
    fn it_works() {
        let path = "/Users/liudawei/allfiles/workspace/rust/TsFile-rs/1637893124311-1-3-0.tsfile";
        if let Ok(mut reader) = TsFileSearchReader::try_from(path) {
            // let x = reader.metadata();
            // let file_meta = x.file_meta();
            // let x1 = x.file_meta().metadata_index();
            {
                let x = reader.all_devices();
                println!("{:?}", x);
            }
            // match x1 {
            //
            // }
            // println!("{:?}", x1);
            //
            // if let Some(bf) = file_meta.bloom_filter() {
            //     if bf.contains("root.group_0.d_0") {
            //         println!("xxxxxxxxx:{:?}", 1);
            //     };
            // }
        }
        // let r1 = File::open(&Path::new(path)).unwrap();
        // let metadata = parser_metadata(r1);
        // println!("{:?}", metadata);
    }
}
