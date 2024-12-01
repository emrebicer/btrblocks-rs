use std::path::PathBuf;
use temp_dir::TempDir;

use crate::ffi::ffi;

pub fn configure(max_depth: u32) {
    ffi::configure_btrblocks(max_depth);
}

pub fn set_log_level(level: LogLevel) {
    ffi::set_log_level(level.into());
}

pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Critical,
    Off,
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

#[derive(Debug)]
pub enum ColumnType {
    Integer = 0,
    Double = 1,
    String = 2,
    Skip,
    // The next types are out of scope
    Float,
    Bigint,
    SmallInt,
    Undefined,
}

impl From<u32> for ColumnType {
    fn from(value: u32) -> Self {
        match value {
            0 => ColumnType::Integer,
            1 => ColumnType::Double,
            2 => ColumnType::String,
            3 => ColumnType::Skip,
            4 => ColumnType::Float,
            5 => ColumnType::Bigint,
            6 => ColumnType::SmallInt,
            _ => ColumnType::Undefined,
        }
    }
}

#[derive(Debug)]
pub struct FileMetadata {
    pub num_columns: u32,
    pub num_chunks: u32,
    pub parts: Vec<ColumnPartInfo>,
}

impl FileMetadata {
    /// Get the BtrBlocks metadata from the given btr path
    pub fn from_btr_path(mut btr_path: PathBuf) -> Self {
        // TODO: Should be a result
        btr_path.push("metadata");
        let path_str = btr_path.to_str().expect("must be a valid path").to_string();
        let raw_metadata: Vec<u32> = ffi::get_file_metadata(path_str);

        let mut it = raw_metadata.iter();

        let num_columns = it.next().expect("num_columns must exists in the data");
        let num_chunks = it.next().expect("num_chunks must exists in the data");

        let mut parts = vec![];
        while let Some(part_type) = it.next() {
            let num_parts = it
                .next()
                .expect("if there is a part_type, there also must be the num_parts");
            parts.push(ColumnPartInfo {
                r#type: (*part_type).into(),
                num_parts: *num_parts,
            });
        }

        FileMetadata {
            num_columns: *num_columns,
            num_chunks: *num_chunks,
            parts,
        }
    }
}

#[derive(Debug)]
pub struct ColumnPartInfo {
    pub r#type: ColumnType,
    pub num_parts: u32,
}

pub struct Relation {
    inner: *mut ffi::Relation,
}

impl Relation {
    pub fn new() -> Self {
        Self {
            inner: ffi::new_relation(),
        }
    }

    pub fn add_column_int(&self, column_name: String, btr_vec: IntMMapVector) {
        unsafe { ffi::relation_add_column_int(self.inner, column_name, btr_vec.inner) }
    }

    pub fn add_column_double(&self, column_name: String, btr_vec: DoubleMMapVector) {
        unsafe { ffi::relation_add_column_double(self.inner, column_name, btr_vec.inner) }
    }

    pub fn tuple_count(&self) -> u64 {
        unsafe { ffi::relation_get_tuple_count(self.inner) }
    }

    pub fn chunk(&self, ranges: &Vec<(u64, u64)>, size: usize) -> Chunk {
        let mut ranges_flat = Vec::new();
        for range in ranges {
            ranges_flat.push(range.0);
            ranges_flat.push(range.1);
        }
        unsafe {
            let ffi_chunk = ffi::relation_get_chunk(self.inner, &ranges_flat, size);
            Chunk::new(ffi_chunk)
        }
    }

    // TODO: not sure if this is a nice api to compare chunks
    // (it also does not exist at the actual btrblocks lib, more of an example impl in example)
    // Perhaps a function like chunk.compare(other: &Chunk) would make sense
    // if ffi::Chunk has enough properties to allow that
    pub fn compare_chunks(&self, c1: &Chunk, c2: &Chunk) -> bool {
        unsafe { ffi::compare_chunks(self.inner, c1.inner, c2.inner) }
    }
}

pub struct Chunk {
    inner: *mut ffi::Chunk,
}

impl Chunk {
    pub fn new(ffi_chunk: *mut ffi::Chunk) -> Self {
        Self { inner: ffi_chunk }
    }

    pub fn tuple_count(&self) -> u64 {
        unsafe { ffi::chunk_get_tuple_count(self.inner) }
    }

    pub fn size_bytes(&self) -> usize {
        unsafe { ffi::chunk_size_bytes(self.inner) }
    }
}

pub struct IntMMapVector {
    inner: *mut ffi::IntMMapVector,
}

impl IntMMapVector {
    pub fn new(vec: &Vec<i32>) -> Self {
        Self {
            inner: ffi::new_int_mmapvector(vec),
        }
    }
}

pub struct DoubleMMapVector {
    inner: *mut ffi::DoubleMMapVector,
}

impl DoubleMMapVector {
    pub fn new(vec: &Vec<f64>) -> Self {
        Self {
            inner: ffi::new_double_mmapvector(vec),
        }
    }
}

pub struct Datablock {
    inner: *mut ffi::Datablock,
}

impl Datablock {
    pub fn new(relation: &Relation) -> Self {
        unsafe {
            Self {
                inner: ffi::new_datablock(relation.inner),
            }
        }
    }

    pub fn compress(&self, chunk: &Chunk, buffer: &Buffer) -> OutputBlockStats {
        unsafe {
            let ffi_output_block_stats =
                ffi::datablock_compress(self.inner, chunk.inner, buffer.inner);
            OutputBlockStats::new(ffi_output_block_stats)
        }
    }

    pub fn decompress(&self, buffer: &Buffer) -> Chunk {
        unsafe {
            let ffi_chunk = ffi::datablock_decompress(self.inner, buffer.inner);
            Chunk::new(ffi_chunk)
        }
    }
}

pub struct OutputBlockStats {
    inner: *mut ffi::OutputBlockStats,
}

impl OutputBlockStats {
    pub fn new(ffi_output_block_stats: *mut ffi::OutputBlockStats) -> Self {
        Self {
            inner: ffi_output_block_stats,
        }
    }

    pub fn total_data_size(&self) -> usize {
        unsafe { ffi::stats_total_data_size(self.inner) }
    }

    pub fn compression_ratio(&self) -> f64 {
        unsafe { ffi::stats_compression_ratio(self.inner) }
    }
}

pub struct Buffer {
    inner: *mut ffi::Buffer,
}

impl Buffer {
    pub fn new(size: usize) -> Self {
        Self {
            inner: ffi::new_buffer(size),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Btr {
    /// The path to the btrblocks compressed directory
    btr_path: PathBuf,
}

impl Btr {
    /// Construct a Btr object from an existing BtrBlocks compressed file
    pub fn from_path(btr_path: PathBuf) -> Self {
        Self { btr_path }
    }

    /// Construct a Btr object from an existing CSV file by compressing it
    /// `btr_path` is the target path for the BtrBlocks compressed file output
    //pub fn from_csv(csv_path: PathBuf, btr_path: PathBuf, scheme_yaml_path: PathBuf) -> Result<Self, String>{
    //    // TODO: manually test this, if it works, add unit tests
    //    let bin_temp_dir = TempDir::new().expect("should not fail to create a temp dir for binary data");
    //    match ffi::csv_to_btr(
    //        csv_path.to_str().expect("should be a valid path").to_string(),
    //        btr_path.to_str().expect("should be a valid path").to_string(),
    //        format!("{}/", bin_temp_dir.path().to_str().expect("should be a valid path")),
    //        scheme_yaml_path.to_str().expect("should be a valid path").to_string(),
    //    ) {
    //        Ok(_) => Ok({
    //            Self{
    //                btr_path,
    //            }
    //        }),
    //        Err(err) => Err(err.to_string()),
    //    }
    //}

    pub fn file_metadata(&self) -> FileMetadata {
        FileMetadata::from_btr_path(self.btr_path.clone())
    }

    pub fn decompress_column_into_file(
        &self,
        column_index: u32,
        output_path: PathBuf,
    ) -> Result<(), String> {
        match ffi::decompress_column_into_file(
            self.btr_path
                .to_str()
                .expect("must be a valid path")
                .to_string(),
            column_index,
            output_path
                .to_str()
                .expect("must be a valid path")
                .to_string(),
        ) {
            Ok(_) => Ok(()),
            Err(err) => Err(err.to_string()),
        }
    }

    pub fn decompress_column_i32(&self, column_index: u32) -> Result<Vec<i32>, String> {
        match ffi::decompress_column_i32(
            self.btr_path
                .to_str()
                .expect("must be a valid path")
                .to_string(),
            column_index,
        ) {
            Ok(vec) => Ok(vec),
            Err(err) => Err(err.to_string()),
        }
    }

    pub fn decompress_column_string(&self, column_index: u32) -> Result<Vec<String>, String> {
        match ffi::decompress_column_string(
            self.btr_path
                .to_str()
                .expect("must be a valid path")
                .to_string(),
            column_index,
        ) {
            Ok(vec) => Ok(vec),
            Err(err) => Err(err.to_string()),
        }
    }

    pub fn decompress_column_f64(&self, column_index: u32) -> Result<Vec<f64>, String> {
        match ffi::decompress_column_f64(
            self.btr_path
                .to_str()
                .expect("must be a valid path")
                .to_string(),
            column_index,
        ) {
            Ok(vec) => Ok(vec),
            Err(err) => Err(err.to_string()),
        }
    }
}
