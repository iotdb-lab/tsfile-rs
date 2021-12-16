const MAGIC_STRING: &str = "TsFile";
const VERSION_NUMBER_V2: &str = "000002";
const VERSION_NUMBER: u8 = 0x03;
const FOOTER_SIZE: usize = 10;

#[macro_use]
pub mod error;
pub mod chunk;
pub mod file;
pub mod utils;
pub mod encoding;

mod tests {
    use std::fs::read;

    use crate::file::reader::FileReader;
    use crate::file::tsfile_search_reader::TsFileSearchReader;

    #[test]
    fn it_works() {
        let path = "/Users/liudawei/allfiles/workspace/rust/TsFile-rs/1637893124311-1-3-0.tsfile";
        if let Ok(reader) = TsFileSearchReader::try_from(path) {
            // let x = reader.metadata();
            // let file_meta = x.file_meta();
            // let root = reader.metadata().file_meta().metadata_index();
            // let option = reader.binary_search_meta(root.clone(), "root.group_0.d_0".to_string(), "s_0".to_string());
            if let Some(option) =
            reader.get_sensor_reader("root.group_0.d_0".to_string(), "s_0".to_string())
            {
                if let Ok(x) = option.get_chunk_reader(0) {
                    x.for_each(|mut y| println!("{:?}",y.data()));
                }
            }
            // let x = reader.sensor_meta_iter("root.group_0.d_0".to_string());
            // x.for_each(|x| println!("{:?}", x.get))
        }
    }
}
