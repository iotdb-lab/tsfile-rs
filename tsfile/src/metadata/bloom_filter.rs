use bit_set::BitSet;

#[derive(Debug)]
pub struct BloomFilter {
    MINIMAL_SIZE: i32,
    MAXIMAL_HASH_FUNCTION_SIZE: i32,
    SEEDS: Vec<i32>,
    size: i32,
    hashFunctionSize: i32,
    bits: BitSet,
    func: Vec<HashFunction>,
}

impl BloomFilter {
    pub fn new(data: Vec<u8>, filter_size: i32, hash_function_size: i32) -> Self {
        let seeds = vec![5, 7, 11, 19, 31, 37, 43, 59];
        let hash_function_size = std::cmp::min(8, hash_function_size);

        let mut func: Vec<HashFunction> = Vec::with_capacity(hash_function_size as usize);
        for i in 0..hash_function_size {
            func.push(HashFunction::new(filter_size, seeds[i as usize]));
        }


        Self {
            size: filter_size,
            MINIMAL_SIZE: 256,
            MAXIMAL_HASH_FUNCTION_SIZE: 8,
            SEEDS: seeds,
            hashFunctionSize: hash_function_size,
            func,
            bits: BitSet::from_bytes(&data[8..]),
        }
    }
}

#[derive(Debug)]
pub struct HashFunction {
    cap: i32,
    seed: i32,
}

impl HashFunction {
    pub fn new(filter_size: i32, seed: i32) -> Self {
        Self {
            cap: filter_size,
            seed,
        }
    }
}
