#[cxx::bridge]
pub mod ffi {

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
        type IntMMapVector;
        type DoubleMMapVector;

        fn configure_btrblocks(max_depth: u32);
        fn set_log_level(level: i32);
        fn new_relation() -> *mut Relation;
        fn new_int_mmapvector(vec: &Vec<i32>) -> *mut IntMMapVector;
        fn new_double_mmapvector(vec: &Vec<f64>) -> *mut DoubleMMapVector;
        fn get_file_metadata(btr_metadata_path: String) -> Vec<u32>;
        fn decompress_column_into_file(
            btr_path: String,
            column_index: u32,
            output_path: String,
        ) -> Result<()>;
        fn decompress_column_i32(btr_path: String, column_index: u32) -> Result<Vec<i32>>;
        fn decompress_column_string(btr_path: String, column_index: u32) -> Result<Vec<String>>;
        fn decompress_column_f64(btr_path: String, column_index: u32) -> Result<Vec<f64>>;
        //fn csv_to_btr(
        //    csv_path: String,
        //    btr_path: String,
        //    binary_path: String,
        //    schema_yaml_path: String,
        //) -> Result<()>;

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
