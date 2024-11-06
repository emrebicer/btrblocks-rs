fn main() {
    btrblocks::configure_btrblocks(3);
    btrblocks::set_log_level(LogLevel::Info.into());

    let relation = btrblocks::new_relation();

    unsafe{
        // TODO: generate the data from rust
        btrblocks::relation_add_column_int_random(relation,  640000, (1 << 12) - 1, 40, 42);
        btrblocks::relation_add_column_double_random(relation,  640000, (1 << 12) - 1, 40, 69);

        let datablock = btrblocks::new_datablock(relation);

        let tuple_count = btrblocks::relation_get_tuple_count(relation);
        let input = btrblocks::relation_get_chunk(relation, 0, tuple_count, 0);
        let size = btrblocks::chunk_get_tuple_count(input) as usize * std::mem::size_of::<f64>() * 2;

        // Create a buffer for output (it is just a unique pointer to [u8])
        let output = btrblocks::new_buffer(size);

        // Compress the input
        let stats = btrblocks::datablock_compress(datablock, input, output);
        println!("Stats:");
        println!("\t Input size: {}", btrblocks::chunk_size_bytes(input));
        println!("\t Output size: {}", btrblocks::stats_total_data_size(stats));
        println!("\t Compression ratio: {}", btrblocks::stats_compression_ratio(stats));

        // Decompress the output
        let decompressed = btrblocks::datablock_decompress(datablock, output);

        // Compare the input and output
        let res = btrblocks::compare_chunks(relation, input, decompressed);

        if res {
            println!("decompressed data matches original data");
        } else {
            println!("decompressed data does not match original data");
        }
    }

}

#[cxx::bridge]
mod btrblocks {

    #[namespace = "btrblocks"]
    unsafe extern "C++" {
        type Relation;
        type Chunk;
        type Datablock;
        type OutputBlockStats;
    }

    #[namespace = "btrblocksWrapper"]
    unsafe extern "C++" {
        include!("btrblocks_wrapper.hpp");

        type Buffer;

        fn configure_btrblocks(max_depth: u32);
        fn set_log_level(level: i32);
        fn new_relation() -> *mut Relation;

        unsafe fn relation_add_column_int_random(
            relation: *mut Relation,
            size: u32,
            unique: u32,
            runlength: u32,
            seed: i32,
        );

        unsafe fn relation_add_column_double_random(
            relation: *mut Relation,
            size: u32,
            unique: u32,
            runlength: u32,
            seed: i32,
        );

        unsafe fn relation_get_tuple_count(relation: *mut Relation) -> u64;
        unsafe fn chunk_get_tuple_count(relation: *mut Chunk) -> u64;
        unsafe fn chunk_size_bytes(relation: *mut Chunk) -> usize;
        unsafe fn relation_get_chunk(relation: *mut Relation, range_start: u64, range_end: u64, size: usize) -> *mut Chunk;
        unsafe fn new_datablock(relation: *mut Relation) -> *mut Datablock;
        unsafe fn datablock_compress(datablock: *mut Datablock, chunk: *mut Chunk, buffer: *mut Buffer) -> *mut OutputBlockStats;
        unsafe fn datablock_decompress(datablock: *mut Datablock, chunk: *mut Buffer) -> *mut Chunk;
        unsafe fn new_buffer(size: usize) -> *mut Buffer;
        unsafe fn compare_chunks(rel: *mut Relation, c1: *mut Chunk, c2: *mut Chunk) -> bool;
        unsafe fn stats_total_data_size(rel: *mut OutputBlockStats) -> usize;
        unsafe fn stats_compression_ratio(rel: *mut OutputBlockStats) -> f64;
    }
}

pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Critical,
    Off
}

impl Into<i32> for LogLevel {
    fn into(self) -> i32 {
        match self {
            LogLevel::Trace => 0,
            LogLevel::Debug => 1,
            LogLevel::Info => 2,
            LogLevel::Warn => 3,
            LogLevel::Error => 4,
            LogLevel::Critical => 5,
            LogLevel::Off => 6,
        }
    }
}

