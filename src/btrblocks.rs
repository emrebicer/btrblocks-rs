use crate::{
    error::BtrBlocksError,
    mount::{oneshot_fs::BtrBlocksOneShotFs, realtime_fs::BtrBlocksRealtimeFs},
    stream::CsvDecompressionStream,
    util::{ensure_protocol, parse_generic_url, string_to_btr_url},
    Result,
};
use csv::ReaderBuilder;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use fuser::{BackgroundSession, MountOption};
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectStore, WriteMultipart};
use serde::Deserialize;
use std::io::Write;
use std::{cmp::max, collections::HashMap, fs::File, path::PathBuf};
use temp_dir::TempDir;
use url::Url;

use crate::ffi::ffi;

pub fn configure(max_depth: u32, block_size: u32) {
    ffi::configure_btrblocks(max_depth, block_size);
}

pub fn set_log_level(level: LogLevel) {
    ffi::set_log_level(level.into());
}

#[derive(Debug, Deserialize)]
pub struct Schema {
    pub columns: Vec<ColumnMetadata>,
}

impl Schema {
    pub fn new(columns: Vec<ColumnMetadata>) -> Self {
        Self { columns }
    }
}

#[derive(Debug, Deserialize)]
pub struct ColumnMetadata {
    pub name: String,
    pub r#type: ColumnType,
}

impl ColumnMetadata {
    pub fn new(name: String, r#type: ColumnType) -> Self {
        Self { name, r#type }
    }
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

impl From<LogLevel> for i32 {
    fn from(val: LogLevel) -> Self {
        match val {
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

#[derive(Debug, Deserialize, Copy, Clone)]
#[serde(rename_all = "lowercase")]
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

impl From<ColumnType> for String {
    fn from(val: ColumnType) -> String {
        match val {
            ColumnType::Integer => "integer".to_string(),
            ColumnType::Double => "double".to_string(),
            ColumnType::String => "string".to_string(),
            ColumnType::Skip => "skip".to_string(),
            ColumnType::Float => "float".to_string(),
            ColumnType::Bigint => "bigint".to_string(),
            ColumnType::SmallInt => "smallint".to_string(),
            ColumnType::Undefined => "undefined".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub num_columns: u32,
    pub num_chunks: u32,
    pub parts: Vec<ColumnPartInfo>,
}

impl FileMetadata {
    pub async fn bytes_from_btr_url(btr_url: Url) -> Result<Vec<u8>> {
        let metadata_url = btr_url
            .join("metadata")
            .map_err(|err| BtrBlocksError::Url(err.to_string()))?;

        let (store, path) =
            parse_generic_url(&metadata_url).map_err(|err| BtrBlocksError::Url(err.to_string()))?;

        let metadata_bytes = store
            .get(&path)
            .await
            .map_err(|err| BtrBlocksError::Custom(err.to_string()))?
            .bytes()
            .await
            .map_err(|err| BtrBlocksError::Custom(err.to_string()))?;

        Ok(metadata_bytes.to_vec())
    }

    /// Get the BtrBlocks metadata from the given btr url
    pub async fn from_btr_url(btr_url: Url) -> Result<Self> {
        let metadata_bytes = FileMetadata::bytes_from_btr_url(btr_url).await?;

        let raw_metadata: Vec<u32> = ffi::get_file_metadata(&metadata_bytes);

        let mut it = raw_metadata.iter();

        let num_columns = it.next().ok_or(BtrBlocksError::Metadata(
            "num_columns must exists in the data".to_string(),
        ))?;
        let num_chunks = it.next().ok_or(BtrBlocksError::Metadata(
            "num_chunks must exists in the data".to_string(),
        ))?;

        let mut parts = vec![];
        while let Some(part_type) = it.next() {
            let num_parts = it.next().ok_or(BtrBlocksError::Metadata(
                "if there is a part_type, there also must be the num_parts".to_string(),
            ))?;
            parts.push(ColumnPartInfo {
                r#type: (*part_type).into(),
                num_parts: *num_parts,
            });
        }

        Ok(FileMetadata {
            num_columns: *num_columns,
            num_chunks: *num_chunks,
            parts,
        })
    }

    pub fn to_schema_ref(&self) -> Result<SchemaRef> {
        let mut fields = vec![];
        let mut header_str = String::new();

        let column_count = max(self.parts.len(), 1);

        for (counter, column) in self.parts.iter().enumerate() {
            let data_type = match column.r#type {
                ColumnType::Integer => DataType::Int32,
                ColumnType::Double => DataType::Float64,
                ColumnType::String => DataType::Utf8,
                _ => DataType::Null,
            };

            let field_name = format!("column_{counter}");

            fields.push(Field::new(field_name.clone(), data_type, true));
            header_str.push_str(field_name.as_str());

            if counter + 1 < column_count {
                header_str.push(',');
            }
        }

        Ok(SchemaRef::new(ArrowSchema::new(fields)))
    }
}

#[derive(Debug, Copy, Clone)]
pub struct ColumnPartInfo {
    pub r#type: ColumnType,
    pub num_parts: u32,
}

pub struct Relation {
    inner: *mut ffi::Relation,
}

impl Default for Relation {
    fn default() -> Self {
        Self::new()
    }
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
    btr_url: Url,
    metadata_bytes_cache: Vec<u8>,
}

impl Btr {
    /// Construct a Btr object from an existing BtrBlocks compressed file
    pub async fn from_url(btr_url: String) -> Result<Self> {
        let btr_url = string_to_btr_url(&mut btr_url.clone())?;
        let metadata_bytes_cache = FileMetadata::bytes_from_btr_url(btr_url.clone()).await?;
        Ok(Self {
            btr_url,
            metadata_bytes_cache,
        })
    }

    /// Construct a Btr object from an existing CSV file by compressing it
    /// `btr_path` is the target path for the BtrBlocks compressed file output
    /// Currently the csv file can be from supported object stores, but resulting btr path
    /// must only be on the local fs
    pub async fn from_csv(
        csv_url: Url,
        btr_path: PathBuf,
        schema: Schema,
        has_headers: bool,
    ) -> Result<Self> {
        // Read the csv file in memory
        let (store, path) =
            parse_generic_url(&csv_url).map_err(|err| BtrBlocksError::Url(err.to_string()))?;

        let csv_bytes = store
            .get(&path)
            .await
            .map_err(|err| BtrBlocksError::Custom(err.to_string()))?
            .bytes()
            .await
            .map_err(|err| BtrBlocksError::Custom(err.to_string()))?
            .to_vec();

        let mut rdr = ReaderBuilder::new()
            .has_headers(has_headers)
            .from_reader(csv_bytes.as_slice());

        // Create a HashMap to store the data for each column
        let mut columns_data: HashMap<String, Vec<String>> = HashMap::new();
        for col in &schema.columns {
            columns_data.insert(col.name.clone(), Vec::new());
        }

        let btr_path_str = btr_path
            .to_str()
            .ok_or(BtrBlocksError::Path("must be a valid path".to_string()))?
            .to_string();

        let mut raw_parts_data = vec![];
        let mut type_and_thread_handles = vec![];

        let records: Vec<csv::Result<csv::StringRecord>> = rdr.records().collect();

        // Iterate over the columns in the given schema
        for (btr_col_index, col) in schema.columns.iter().enumerate() {
            match col.r#type {
                ColumnType::Integer => {
                    let mut data = vec![];
                    for record_res in &records {
                        let record = record_res
                            .as_ref()
                            .map_err(|err| BtrBlocksError::Custom(err.to_string()))?;

                        data.push(
                            record
                                .get(btr_col_index)
                                .unwrap_or("0")
                                .parse::<i32>()
                                .map_err(|err| BtrBlocksError::Custom(err.to_string()))?,
                        );
                    }

                    let btr_path_str = btr_path_str.clone();
                    let handle = tokio::spawn(async move {
                        ffi::compress_column_i32(btr_path_str, &data, btr_col_index as u32)
                            .map_err(|err| BtrBlocksError::BtrBlocksLibWrapper(err.to_string()))
                    });

                    type_and_thread_handles.push((0, handle));
                }
                ColumnType::Double => {
                    let mut data = vec![];
                    for record_res in &records {
                        let record = record_res
                            .as_ref()
                            .map_err(|err| BtrBlocksError::Custom(err.to_string()))?;

                        data.push(
                            record
                                .get(btr_col_index)
                                .unwrap_or("0.0")
                                .parse::<f64>()
                                .map_err(|err| BtrBlocksError::Custom(err.to_string()))?,
                        );
                    }

                    let btr_path_str = btr_path_str.clone();
                    let handle = tokio::spawn(async move {
                        ffi::compress_column_f64(btr_path_str, &data, btr_col_index as u32)
                            .map_err(|err| BtrBlocksError::BtrBlocksLibWrapper(err.to_string()))
                    });

                    type_and_thread_handles.push((1, handle));
                }
                ColumnType::String => {
                    let mut data = vec![];
                    for record_res in &records {
                        let record = record_res
                            .as_ref()
                            .map_err(|err| BtrBlocksError::Custom(err.to_string()))?;

                        data.push(record.get(btr_col_index).unwrap_or("NULL").to_string());
                    }

                    let bin_temp_dir = TempDir::new().map_err(|err| {
                        BtrBlocksError::Custom(
                            format!("failed to create a temp dir for binary data: {}", err)
                                .to_string(),
                        )
                    })?;

                    let bin_temp_dir_str = bin_temp_dir
                        .path()
                        .to_str()
                        .ok_or(BtrBlocksError::Path("must be a valid path".to_string()))?
                        .to_string();

                    let btr_path_str = btr_path_str.clone();
                    let handle = tokio::spawn(async move {
                        ffi::compress_column_string(
                            btr_path_str,
                            &data,
                            btr_col_index as u32,
                            bin_temp_dir_str,
                        )
                        .map_err(|err| BtrBlocksError::BtrBlocksLibWrapper(err.to_string()))
                    });

                    type_and_thread_handles.push((2, handle));
                }
                _ => {
                    unimplemented!()
                }
            };
        }

        // Construct the raw parts data
        for (r#type, handle) in type_and_thread_handles {
            raw_parts_data.push(r#type);

            match handle.await {
                Ok(parts_count) => raw_parts_data.push(parts_count?),
                Err(e) => return Err(BtrBlocksError::Custom(format!("tokio join error: {}", e))),
            }
        }

        // Finally write the metadata to the btr dir
        let num_chunks = ffi::get_num_chunks(records.len() as u64)
            .map_err(|err| BtrBlocksError::BtrBlocksLibWrapper(err.to_string()))?;

        let metadata_bytes =
            ffi::get_file_metadata_bytes(schema.columns.len() as u32, num_chunks, raw_parts_data)
                .map_err(|err| BtrBlocksError::BtrBlocksLibWrapper(err.to_string()))?;

        let mut file = File::create(format!("{btr_path_str}/metadata"))
            .map_err(|err| BtrBlocksError::Custom(err.to_string()))?;

        file.write_all(&metadata_bytes)
            .map_err(|err| BtrBlocksError::Custom(err.to_string()))?;

        Btr::from_url(btr_path_str.clone()).await
    }

    /// Decompressed the btr file and writes the resulting CSV to the
    /// given `target_url`
    pub async fn write_to_csv(&self, target_url: String, num_rows_per_poll: usize) -> Result<()> {
        let mut target_url = target_url.clone();
        ensure_protocol(&mut target_url);
        let target_url =
            Url::parse(&target_url).map_err(|err| BtrBlocksError::Url(err.to_string()))?;

        // Create the ChunkedStream to read decompresssed data by parts
        let mut stream =
            CsvDecompressionStream::new(self.clone(), num_rows_per_poll).await?;

        let (store, path) =
            parse_generic_url(&target_url).map_err(|err| BtrBlocksError::Url(err.to_string()))?;

        let upload = store.put_multipart(&path).await.unwrap();
        let mut write = WriteMultipart::new(upload);

        // Write the rows data in batches
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|err| BtrBlocksError::Custom(err.to_string()))?;
            write.write(batch.as_bytes());
        }

        // Finish writing
        write
            .finish()
            .await
            .map_err(|err| BtrBlocksError::Custom(err.to_string()))?;

        Ok(())
    }

    async fn csv_header(&self) -> Result<String> {
        let file_metadata = self.file_metadata().await?;
        let column_count = max(file_metadata.parts.len(), 1);
        let mut header_str = String::new();

        for counter in 0..file_metadata.parts.len() {
            let field_name = format!("column_{counter}");
            header_str.push_str(field_name.as_str());

            if counter + 1 < column_count {
                header_str.push(',');
            }
        }

        Ok(header_str)
    }

    /// Create a mount point and make the decompresssed CSV file accessible there
    pub async fn mount_csv_one_shot(
        &self,
        mount_point: String,
        mount_options: &mut Vec<MountOption>,
    ) -> Result<BackgroundSession> {
        // Decompress into csv and keep the result in memory
        let mut raw_csv_data = String::new();

        // Create the csv header text
        let file_metadata = self.file_metadata().await?;
        let column_count = max(file_metadata.parts.len(), 1);

        // Create the ChunkedStream to read decompresssed data by parts
        let mut stream =
            CsvDecompressionStream::new(self.clone(), 1_000_000 / column_count).await?;

        // Write the rows data in batches
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|err| BtrBlocksError::Custom(err.to_string()))?;
            raw_csv_data.push_str(&batch);
        }

        let btr_fs = BtrBlocksOneShotFs::new(raw_csv_data);
        btr_fs.mount(mount_point, mount_options)
    }

    pub async fn mount_csv_realtime(
        &self,
        mount_point: String,
        mount_options: &mut Vec<MountOption>,
        cache_limit: usize,
        precompute_csv_size: bool,
    ) -> Result<BackgroundSession> {

        let (store, path) =
            parse_generic_url(&self.btr_url).map_err(|err| BtrBlocksError::Url(err.to_string()))?;

        let csv_size = if precompute_csv_size {
            // Add the header size
            let mut size = 0;

            // Create the ChunkedStream to calculate the correct decompressed csv size
            let mut stream = CsvDecompressionStream::new(self.clone(), 10_000).await?;

            // Write the rows data in batches
            while let Some(batch) = stream.next().await {
                let batch = batch.map_err(|err| BtrBlocksError::Custom(err.to_string()))?;
                size += batch.len();
            }

            size
        } else {
            let mut compressed_size = 0;

            // List all items under the directory
            let mut stream = store.list(Some(&path));

            while let Some(object_metadata) = stream.try_next().await.map_err(|err| {
                BtrBlocksError::Custom(format!("failed to iterate over directory items: {}", err))
            })? {
                compressed_size += object_metadata.size;
            }

            compressed_size * 8
        };

        let btr_fs = BtrBlocksRealtimeFs::new(
            self.clone(),
            csv_size,
            cache_limit,
        )
        .await?;
        btr_fs.mount(mount_point, mount_options)
    }

    pub async fn file_metadata(&self) -> Result<FileMetadata> {
        FileMetadata::from_btr_url(self.btr_url.clone()).await
    }

    fn file_metadata_bytes(&self) -> Vec<u8> {
        self.metadata_bytes_cache.clone()
    }

    async fn column_part_bytes(&self, column_index: u32, part_index: u32) -> Result<Vec<u8>> {
        let column_part_url = self
            .btr_url
            .join(format!("column{}_part{}", column_index, part_index).as_str())
            .map_err(|err| BtrBlocksError::Url(err.to_string()))?;

        let (store, path) = parse_generic_url(&column_part_url)
            .map_err(|err| BtrBlocksError::Url(err.to_string()))?;

        Ok(store
            .get(&path)
            .await
            .map_err(|err| BtrBlocksError::Custom(err.to_string()))?
            .bytes()
            .await
            .map_err(|err| BtrBlocksError::Custom(err.to_string()))?
            .to_vec())
    }

    pub async fn decompress_column_i32(&self, column_index: u32) -> Result<Vec<i32>> {
        let metadata = self.file_metadata().await?;

        let mut compressed_data = vec![];
        let mut part_ending_indexes = vec![];

        for part_index in 0..metadata
            .parts
            .get(column_index as usize)
            .ok_or(BtrBlocksError::Custom(format!(
                "Column with index {column_index} does not exist"
            )))?
            .num_parts
        {
            let compressed_part = self.column_part_bytes(column_index, part_index).await?;
            compressed_data.extend(compressed_part);
            part_ending_indexes.push(compressed_data.len());
        }

        match ffi::decompress_column_i32(
            &compressed_data,
            &part_ending_indexes,
            metadata.num_chunks,
        ) {
            Ok(vec) => Ok(vec),
            Err(err) => Err(BtrBlocksError::BtrBlocksLibWrapper(err.to_string())),
        }
    }

    pub async fn decompress_column_part_i32(
        &self,
        column_index: u32,
        part_index: u32,
    ) -> Result<Vec<i32>> {
        match ffi::decompress_column_part_i32(
            &self.column_part_bytes(column_index, part_index).await?,
            &self.file_metadata_bytes(),
            column_index,
            part_index,
        ) {
            Ok(vec) => Ok(vec),
            Err(err) => Err(BtrBlocksError::BtrBlocksLibWrapper(err.to_string())),
        }
    }

    pub async fn decompress_column_string(&self, column_index: u32) -> Result<Vec<String>> {
        let metadata = self.file_metadata().await?;

        let mut compressed_data = vec![];
        let mut part_ending_indexes = vec![];

        for part_index in 0..metadata
            .parts
            .get(column_index as usize)
            .ok_or(BtrBlocksError::Custom(format!(
                "Column with index {column_index} does not exist"
            )))?
            .num_parts
        {
            let compressed_part = self.column_part_bytes(column_index, part_index).await?;
            compressed_data.extend(compressed_part);
            part_ending_indexes.push(compressed_data.len());
        }

        match ffi::decompress_column_string(
            &compressed_data,
            &part_ending_indexes,
            metadata.num_chunks,
        ) {
            Ok(vec) => Ok(vec),
            Err(err) => Err(BtrBlocksError::BtrBlocksLibWrapper(err.to_string())),
        }
    }

    pub async fn decompress_column_part_string(
        &self,
        column_index: u32,
        part_index: u32,
    ) -> Result<Vec<String>> {
        match ffi::decompress_column_part_string(
            &self.column_part_bytes(column_index, part_index).await?,
            &self.file_metadata_bytes(),
            column_index,
            part_index,
        ) {
            Ok(vec) => Ok(vec),
            Err(err) => Err(BtrBlocksError::BtrBlocksLibWrapper(err.to_string())),
        }
    }

    pub async fn decompress_column_f64(&self, column_index: u32) -> Result<Vec<f64>> {
        let metadata = self.file_metadata().await?;

        let mut compressed_data = vec![];
        let mut part_ending_indexes = vec![];

        for part_index in 0..metadata
            .parts
            .get(column_index as usize)
            .ok_or(BtrBlocksError::Custom(format!(
                "Column with index {column_index} does not exist"
            )))?
            .num_parts
        {
            let compressed_part = self.column_part_bytes(column_index, part_index).await?;
            compressed_data.extend(compressed_part);
            part_ending_indexes.push(compressed_data.len());
        }

        match ffi::decompress_column_f64(
            &compressed_data,
            &part_ending_indexes,
            metadata.num_chunks,
        ) {
            Ok(vec) => Ok(vec),
            Err(err) => Err(BtrBlocksError::BtrBlocksLibWrapper(err.to_string())),
        }
    }

    pub async fn decompress_column_part_f64(
        &self,
        column_index: u32,
        part_index: u32,
    ) -> Result<Vec<f64>> {
        match ffi::decompress_column_part_f64(
            &self.column_part_bytes(column_index, part_index).await?,
            &self.file_metadata_bytes(),
            column_index,
            part_index,
        ) {
            Ok(vec) => Ok(vec),
            Err(err) => Err(BtrBlocksError::BtrBlocksLibWrapper(err.to_string())),
        }
    }
}
