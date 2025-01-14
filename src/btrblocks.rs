use crate::{
    datafusion::BtrChunkedStream,
    error::BtrBlocksError,
    util::{ensure_protocol, extract_value_as_string, string_to_btr_url},
    Result,
};
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::arrow::{array::Array, datatypes::Schema as ArrowSchema};
use futures::StreamExt;
use object_store::{parse_url, ObjectStore, WriteMultipart};
use serde::Deserialize;
use std::{cmp::max, path::PathBuf};
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
            parse_url(&metadata_url).map_err(|err| BtrBlocksError::Url(err.to_string()))?;

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
}

impl Btr {
    /// Construct a Btr object from an existing BtrBlocks compressed file
    pub fn from_url(btr_url: String) -> Result<Self> {
        let btr_url = string_to_btr_url(&mut btr_url.clone())?;
        Ok(Self { btr_url })
    }

    /// Construct a Btr object from an existing CSV file by compressing it
    /// `btr_path` is the target path for the BtrBlocks compressed file output
    /// Currently this function is only supported for local filesystem
    pub fn from_csv(csv_path: PathBuf, btr_path: PathBuf, schema: Schema) -> Result<Self> {
        // TODO: refactor this to use object store as well, need to read csv here and pass in data
        // to wrapper, (perhaps lines as string vector?)
        let bin_temp_dir = TempDir::new().map_err(|err| {
            BtrBlocksError::Custom(
                format!("failed to create a temp dir for binary data: {}", err).to_string(),
            )
        })?;

        let mut schema_data_vec = vec![];
        for column in schema.columns {
            schema_data_vec.push(column.name);
            schema_data_vec.push(column.r#type.into());
        }

        let btr_path_str = btr_path
            .to_str()
            .ok_or(BtrBlocksError::Path("must be a valid path".to_string()))?
            .to_string();

        match ffi::csv_to_btr(
            csv_path
                .to_str()
                .ok_or(BtrBlocksError::Path("must be a valid path".to_string()))?
                .to_string(),
            btr_path_str.clone(),
            format!(
                "{}/",
                bin_temp_dir
                    .path()
                    .to_str()
                    .ok_or(BtrBlocksError::Path("must be a valid path".to_string()))?
            ),
            schema_data_vec,
        ) {
            Ok(_) => Btr::from_url(btr_path_str),
            Err(err) => Err(BtrBlocksError::BtrBlocksLibWrapper(err.to_string())),
        }
    }

    /// Decompressed the btr file and writes the resulting CSV to the
    /// given `target_url`
    pub async fn write_to_csv(&self, target_url: String) -> Result<()> {
        let mut target_url = target_url.clone();
        ensure_protocol(&mut target_url);
        let target_url =
            Url::parse(&target_url).map_err(|err| BtrBlocksError::Url(err.to_string()))?;

        // Create the csv header text
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

        // Create the schema ref for the chunked stream
        let schema_ref = file_metadata.to_schema_ref()?;

        // Create the ChunkedStream to read decompresssed data by parts
        let mut stream =
            BtrChunkedStream::new(schema_ref, self.clone(), 1_000_000 / column_count).await?;

        let (store, path) =
            parse_url(&target_url).map_err(|err| BtrBlocksError::Url(err.to_string()))?;

        let upload = store.put_multipart(&path).await.unwrap();
        let mut write = WriteMultipart::new(upload);

        // Write the header row to the target
        write.write(header_str.as_bytes());

        // Write the rows data in batches
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|err| BtrBlocksError::Custom(err.to_string()))?;
            let num_rows = batch.num_rows();
            let num_columns = batch.num_columns();

            let columns: Vec<&dyn Array> = (0..num_columns)
                .map(|col_index| batch.column(col_index).as_ref())
                .collect();

            for row_index in 0..num_rows {
                write.write("\n".as_bytes());

                for (counter, column) in columns.iter().enumerate() {
                    let value = extract_value_as_string(*column, row_index);
                    write.write(value.as_bytes());

                    if counter + 1 < column_count {
                        write.write(",".as_bytes());
                    }
                }
            }
        }

        // Finish writing
        write
            .finish()
            .await
            .map_err(|err| BtrBlocksError::Custom(err.to_string()))?;

        Ok(())
    }

    pub async fn file_metadata(&self) -> Result<FileMetadata> {
        FileMetadata::from_btr_url(self.btr_url.clone()).await
    }

    async fn file_metadata_bytes(&self) -> Result<Vec<u8>> {
        FileMetadata::bytes_from_btr_url(self.btr_url.clone()).await
    }

    async fn column_part_bytes(&self, column_index: u32, part_index: u32) -> Result<Vec<u8>> {
        let column_part_url = self
            .btr_url
            .join(format!("column{}_part{}", column_index, part_index).as_str())
            .map_err(|err| BtrBlocksError::Url(err.to_string()))?;

        let (store, path) =
            parse_url(&column_part_url).map_err(|err| BtrBlocksError::Url(err.to_string()))?;

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
            &self.file_metadata_bytes().await?,
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
            &self.file_metadata_bytes().await?,
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
            &self.file_metadata_bytes().await?,
            column_index,
            part_index,
        ) {
            Ok(vec) => Ok(vec),
            Err(err) => Err(BtrBlocksError::BtrBlocksLibWrapper(err.to_string())),
        }
    }
}
