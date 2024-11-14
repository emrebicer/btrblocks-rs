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

pub struct Relation {
    inner: *mut ffi::Relation,
}

impl Relation {
    pub fn new() -> Self {
        Self {
            inner: ffi::new_relation(),
        }
    }

    pub fn add_column_int(&self, column_name: &String, btr_vec: IntMMapVector) {
        unsafe { ffi::relation_add_column_int(self.inner, column_name, btr_vec.inner) }
    }

    pub fn add_column_double(&self, column_name: &String, btr_vec: DoubleMMapVector) {
        unsafe { ffi::relation_add_column_double(self.inner, column_name, btr_vec.inner) }
    }

    pub fn tuple_count(&self) -> u64 {
        unsafe { ffi::relation_get_tuple_count(self.inner) }
    }

    // TODO: The original api expexts a vec of std::tuple, which cxx can't
    // generate bindings for, so might just create a hacky way to get around it
    // where rs api expects a Vec<(i32, i32)> and sends a Vec<i32> to ffi wrapper
    // then construct the vec<std::tuple> there...
    pub fn chunk(&self, range_start: u64, range_end: u64, size: usize) -> Chunk {
        unsafe {
            let ffi_chunk = ffi::relation_get_chunk(self.inner, range_start, range_end, size);
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
