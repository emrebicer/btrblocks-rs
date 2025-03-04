#[allow(clippy::module_inception)]
#[cxx::bridge]
pub mod ffi {

    #[namespace = "btrblocks"]
    unsafe extern "C++" {
        type Relation;
        type Chunk;
        type Datablock;
        type OutputBlockStats;
    }

    #[namespace = "btrWrapper"]
    unsafe extern "C++" {
        include!("btrblocks_wrapper.hpp");

        type Buffer;
        type IntMMapVector;
        type DoubleMMapVector;

        fn configure_btrblocks(max_depth: u32, block_size: u32);
        fn set_log_level(level: i32);
        fn new_relation() -> *mut Relation;
        fn new_int_mmapvector(vec: &Vec<i32>) -> *mut IntMMapVector;
        fn new_double_mmapvector(vec: &Vec<f64>) -> *mut DoubleMMapVector;
        fn get_file_metadata(metadata_bytes: &Vec<u8>) -> Vec<u32>;
        fn decompress_column_i32(
            column_part_bytes: &Vec<u8>,
            part_ending_indexes: &Vec<usize>,
            num_chunks: u32,
        ) -> Result<Vec<i32>>;
        fn decompress_column_part_i32(
            part_bytes: &Vec<u8>,
            metadata_bytes: &Vec<u8>,
            column_index: u32,
            part_index: u32,
        ) -> Result<Vec<i32>>;

        fn decompress_column_string(
            column_part_bytes: &Vec<u8>,
            part_ending_indexes: &Vec<usize>,
            num_chunks: u32,
        ) -> Result<Vec<String>>;
        fn decompress_column_part_string(
            part_bytes: &Vec<u8>,
            metadata_bytes: &Vec<u8>,
            column_index: u32,
            part_index: u32,
        ) -> Result<Vec<String>>;

        fn decompress_column_f64(
            column_part_bytes: &Vec<u8>,
            part_ending_indexes: &Vec<usize>,
            num_chunks: u32,
        ) -> Result<Vec<f64>>;
        fn decompress_column_part_f64(
            part_bytes: &Vec<u8>,
            metadata_bytes: &Vec<u8>,
            column_index: u32,
            part_index: u32,
        ) -> Result<Vec<f64>>;

        fn compress_column_i32(btr_path: String, data: &Vec<i32>, column_index: u32)
            -> Result<u32>;

        fn compress_column_f64(btr_path: String, data: &Vec<f64>, column_index: u32)
            -> Result<u32>;

        fn compress_column_string(
            btr_path: String,
            data: &Vec<String>,
            column_index: u32,
            binary_path: String,
        ) -> Result<u32>;

        fn get_num_chunks(row_count: u64) -> Result<u32>;
        fn get_file_metadata_bytes(
            num_columns: u32,
            num_chunks: u32,
            parts: Vec<u32>,
        ) -> Result<Vec<u8>>;

        unsafe fn relation_add_column_int(
            relation: *mut Relation,
            column_name: String,
            btr_vec: *mut IntMMapVector,
        );
        unsafe fn relation_add_column_double(
            relation: *mut Relation,
            column_name: String,
            btr_vec: *mut DoubleMMapVector,
        );
        unsafe fn relation_get_tuple_count(relation: *mut Relation) -> u64;
        unsafe fn chunk_get_tuple_count(relation: *mut Chunk) -> u64;
        unsafe fn chunk_size_bytes(relation: *mut Chunk) -> usize;
        unsafe fn relation_get_chunk(
            relation: *mut Relation,
            ranges: &Vec<u64>,
            size: usize,
        ) -> *mut Chunk;
        unsafe fn new_datablock(relation: *mut Relation) -> *mut Datablock;
        unsafe fn datablock_compress(
            datablock: *mut Datablock,
            chunk: *mut Chunk,
            buffer: *mut Buffer,
        ) -> *mut OutputBlockStats;
        unsafe fn datablock_decompress(datablock: *mut Datablock, chunk: *mut Buffer)
            -> *mut Chunk;
        fn new_buffer(size: usize) -> *mut Buffer;
        unsafe fn compare_chunks(rel: *mut Relation, c1: *mut Chunk, c2: *mut Chunk) -> bool;
        unsafe fn stats_total_data_size(rel: *mut OutputBlockStats) -> usize;
        unsafe fn stats_compression_ratio(rel: *mut OutputBlockStats) -> f64;
    }
}
