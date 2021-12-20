const MAGIC_STRING: &str = "TsFile";
const VERSION_NUMBER_V2: &str = "000002";
const VERSION_NUMBER: u8 = 0x03;
const FOOTER_SIZE: usize = 10;

#[macro_use]
pub mod error;
pub mod chunk;
pub mod encoding;
pub mod file;
pub mod utils;

mod tests {
    use crate::file::metadata::MetadataIndexNodeType::{InternalDevice, LeafDevice};

    #[test]
    fn it_works() {
        use crate::file::reader::FileReader;
        use crate::file::tsfile_search_reader::TsFileSearchReader;

        let path = "/Users/liudawei/allfiles/workspace/rust/TsFile-rs/1637893124311-1-3-0.tsfile";
        if let Ok(reader) = TsFileSearchReader::try_from(path) {
            let device_meta = reader.device_meta_iter();
            device_meta.for_each(|meta|
                match meta {
                    InternalDevice(f) | LeafDevice(f) => {
                        let device_name = f.children().first().expect("123").name();
                        let sensors = reader.sensor_meta_iter(device_name);
                        sensors.for_each(|s|
                            if let Some(option) =
                            reader.get_sensor_reader(device_name, s.measurement_id())
                            {
                                if let Ok(x) = option.get_chunk_reader(0) {
                                    x.for_each(|y| println!("{:?}", y.data()));
                                }
                            }
                        )
                    }
                    _ => {}
                });


            // let x1 = reader.sensor_meta_iter();
            // x1.for_each(|f| f.for_each(|y| println!("{:?}", y)));
            // let x = reader.metadata();
            // let file_meta = x.file_meta();
            // let root = reader.metadata().file_meta().metadata_index();
            // let option = reader.binary_search_meta(root.clone(), "root.group_0.d_0".to_string(), "s_0".to_string());
            // if let Some(option) =
            // reader.get_sensor_reader("root.group_0.d_0", "s_0")
            // {
            //     if let Ok(x) = option.get_chunk_reader(0) {
            //         x.for_each(|y| println!("{:?}", y.data()));
            //     }
            // }
            // let x = reader.sensor_meta_iter("root.group_0.d_0".to_string());
            // x.for_each(|x| println!("{:?}", x.get))
        }
    }
}
