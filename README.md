# rust_tsfile

IoTDB TsFile structure implemented in Rust language

base on tsfile version 000003, iotdb 0.12.0

# reader

✅ parser metadata

[] parser Chunk and Page

[] filter data

# How to use

````rust
let path = "/Users/liudawei/allfiles/rust/TsFile-rs/1637893124311-1-3-0.tsfile";
//create a tsfile reader
if let Ok(reader) = TsFileSearchReader::try_from(path) {
//get metadata 
let x = reader.metadata();
println! ("{:?}", x);

//get root node of file's index tree 
let root = reader.metadata().file_meta().metadata_index();

//use the method to query a sensor's TimeseriesMetadata
let option = reader.binary_search_meta(root.clone(), "root.group_0.d_0".to_string(), "s_0".to_string());

//iter for devices
let x = reader.device_meta_iter();
x.for_each( | x | println ! ("{:?}", x))

//iter all sensor for a device
let x = reader.sensor_meta_iter("root.group_0.d_0".to_string());
x.for_each( | x | println ! ("{:?}", x))

}

````
