const MAGIC_STRING: &str = "TsFile";
const VERSION_NUMBER_V2: &str = "000002";
const VERSION_NUMBER: u8 = 0x03;
const FOOTER_SIZE: usize = 10;

#[macro_use]
pub mod error;
pub mod metadata;
pub mod reader;
pub mod footer;


mod tests {
    use std::fs::File;
    use std::path::Path;

    use crate::reader::{FileSource, Length};
    use std::io::Read;
    use crate::footer::parser_metadata;

    #[test]
    fn it_works() {
        let path = "/Users/liudawei/allfiles/github/incubator-iotdb/data/data/sequence/root.sg1/0/1609135472595-183-0.tsfile";
        let r1 = File::open(&Path::new(path)).unwrap();
        let metadata = parser_metadata(r1);
        println!("{:?}", metadata);
    }
}
