use core::fmt;
use std::any::Any;
use std::cmp::{max, min};
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::task::Poll;

use crate::error::BtrBlocksError;
use crate::Btr;
use crate::ColumnType;
use async_trait::async_trait;
use datafusion::arrow::array::{Array, Float64Builder, Int32Builder, RecordBatch, StringBuilder};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{RecordBatchStream, TaskContext};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_expr::Expr;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use futures::stream::Stream;

#[derive(Debug, Clone)]
pub struct BtrBlocksDataSource {
    pub btr: Btr,
}

impl BtrBlocksDataSource {
    pub async fn new(btr_url: String) -> Self {
        Self {
            btr: Btr::from_url(btr_url)
                .await
                .expect("the URL should be a valid URL pointing to a btr compressed file"),
        }
    }
    pub(crate) async fn create_physical_plan(&self) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(BtrBlocksExec::new(self.clone())))
    }
}

#[async_trait]
impl TableProvider for BtrBlocksDataSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let file_metadata = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(self.btr.file_metadata())
                .expect("get file metadata from given btr path")
        });

        file_metadata
            .to_schema_ref()
            .expect("get schema ref from file metadata")
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.create_physical_plan().await
    }
}

#[derive(Debug, Clone)]
struct BtrBlocksExec {
    pub data_source: BtrBlocksDataSource,
    properties: PlanProperties,
}

impl BtrBlocksExec {
    fn new(data_source: BtrBlocksDataSource) -> Self {
        Self {
            data_source: data_source.clone(),
            properties: PlanProperties::new(
                EquivalenceProperties::new(data_source.schema()),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
        }
    }
}

impl DisplayAs for BtrBlocksExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "BtrBlocksExec")
    }
}

impl ExecutionPlan for BtrBlocksExec {
    fn name(&self) -> &'static str {
        "BtrBlocksExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let meta = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(self.data_source.btr.file_metadata())
                .expect("get file metadata from given btr path")
        });

        let column_count = max(meta.parts.len(), 1);

        // Return a chunked stream to keep memory usage low
        Ok(Box::pin(
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(BtrChunkedStream::new(
                    self.schema().clone(),
                    self.data_source.btr.clone(),
                    1_000_000 / column_count,
                ))
            })
            .map_err(|err| DataFusionError::External(Box::new(err)))?,
        ))
    }
}

enum TypedCache {
    Int(Vec<i32>),
    Float(Vec<f64>),
    String(Vec<String>),
}

struct DecompressedColumnCache {
    column_index: usize,
    num_parts: usize,
    next_part_index_to_read: usize,
    cached_data: TypedCache,
}

impl DecompressedColumnCache {
    /// Checks if all data is consumed from this cache
    fn finished(&self) -> bool {
        // Check if there is more data on the existing cache
        let empty = match &self.cached_data {
            TypedCache::Int(vec) => vec.is_empty(),
            TypedCache::Float(vec) => vec.is_empty(),
            TypedCache::String(vec) => vec.is_empty(),
        };

        if !empty {
            return false;
        }

        // Check if there is more partitions to read from
        self.done_reading_all_parts()
    }

    async fn read_next_part(&mut self, btr: &Btr) -> crate::Result<()> {
        if !self.done_reading_all_parts() {
            match &mut self.cached_data {
                TypedCache::Int(vec) => {
                    let mut new_data = btr
                        .decompress_column_part_i32(
                            self.column_index as u32,
                            self.next_part_index_to_read as u32,
                        )
                        .await?;

                    vec.append(&mut new_data);
                }
                TypedCache::Float(vec) => {
                    let mut new_data = btr
                        .decompress_column_part_f64(
                            self.column_index as u32,
                            self.next_part_index_to_read as u32,
                        )
                        .await?;

                    vec.append(&mut new_data);
                }
                TypedCache::String(vec) => {
                    let mut new_data = btr
                        .decompress_column_part_string(
                            self.column_index as u32,
                            self.next_part_index_to_read as u32,
                        )
                        .await?;

                    vec.append(&mut new_data);
                }
            };

            self.next_part_index_to_read += 1;
        }

        Ok(())
    }

    async fn read_until_enough_cache(&mut self, btr: &Btr, nrpp: usize) -> crate::Result<()> {
        while self.current_cache_len() < nrpp && !self.done_reading_all_parts() {
            self.read_next_part(btr).await?;
        }
        Ok(())
    }

    fn current_cache_len(&self) -> usize {
        match &self.cached_data {
            TypedCache::Int(vec) => vec.len(),
            TypedCache::Float(vec) => vec.len(),
            TypedCache::String(vec) => vec.len(),
        }
    }

    fn done_reading_all_parts(&self) -> bool {
        self.next_part_index_to_read > self.num_parts - 1
    }
}

/// A `stream` that reads btr and decompresses data part by part on each poll
pub struct BtrChunkedStream {
    btr: Btr,
    schema_ref: SchemaRef,
    column_caches: Vec<DecompressedColumnCache>,
    num_rows_per_poll: usize,
}

impl BtrChunkedStream {
    pub async fn new(
        schema_ref: SchemaRef,
        btr: Btr,
        num_rows_per_poll: usize,
    ) -> crate::Result<Self> {
        let mut column_caches = vec![];

        for (counter, field) in btr.file_metadata().await?.parts.into_iter().enumerate() {
            let vec = match field.r#type {
                ColumnType::Integer => TypedCache::Int(vec![]),
                ColumnType::Double => TypedCache::Float(vec![]),
                ColumnType::String => TypedCache::String(vec![]),
                _ => {
                    return Err(BtrBlocksError::UnexpectedType(Into::<String>::into(
                        field.r#type,
                    )));
                }
            };

            let cache = DecompressedColumnCache {
                column_index: counter,
                num_parts: field.num_parts as usize,
                next_part_index_to_read: 0,
                cached_data: vec,
            };
            column_caches.push(cache);
        }

        Ok(Self {
            btr,
            schema_ref,
            column_caches,
            num_rows_per_poll,
        })
    }
}

impl RecordBatchStream for BtrChunkedStream {
    fn schema(&self) -> SchemaRef {
        self.schema_ref.clone()
    }
}

impl Stream for BtrChunkedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut data_vec: Vec<Arc<dyn Array>> = vec![];
        let nrpp = self.num_rows_per_poll;
        let btr = self.btr.clone();

        for column in &mut self.column_caches {
            if column.finished() {
                for inner_column in &mut self.column_caches {
                    if !inner_column.finished() {
                        // The columns should be all finished at the same time, if there is a
                        // mismatch it means the data is corrupted  (some columns have different
                        // number of elements), so return an execution error
                        return Poll::Ready(Some(Err(DataFusionError::Execution(format!("A columns is finished and all elements are consumed, however the column with index {} is not finished, most likely the data is corrupted.", inner_column.column_index).to_string()))));
                    }
                }

                // All columns are finished, return None to hint the strem is done and shold not be
                // polled anymore
                return Poll::Ready(None);
            }

            if let Err(e) = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(column.read_until_enough_cache(&btr, nrpp))
            }) {
                println!("failed to decompress part: {e}");
                return Poll::Pending;
            }

            // Now consume the data for the stream
            let num_elements_to_consume = min(column.current_cache_len(), nrpp);

            match &mut column.cached_data {
                TypedCache::Int(vec) => {
                    let mut builder = Int32Builder::new();
                    let to_consumed = vec.drain(0..num_elements_to_consume).collect::<Vec<i32>>();
                    vec.shrink_to_fit();
                    for el in to_consumed {
                        builder.append_value(el);
                    }
                    data_vec.push(Arc::new(builder.finish()));
                }
                TypedCache::Float(vec) => {
                    let mut builder = Float64Builder::new();
                    let to_consumed = vec.drain(0..num_elements_to_consume).collect::<Vec<f64>>();
                    vec.shrink_to_fit();
                    for el in to_consumed {
                        builder.append_value(el);
                    }
                    data_vec.push(Arc::new(builder.finish()));
                }
                TypedCache::String(vec) => {
                    let mut builder = StringBuilder::new();
                    let to_consumed = vec
                        .drain(0..num_elements_to_consume)
                        .collect::<Vec<String>>();
                    vec.shrink_to_fit();
                    for el in to_consumed {
                        builder.append_value(el);
                    }
                    data_vec.push(Arc::new(builder.finish()));
                }
            }
        }

        let batch = RecordBatch::try_new(self.schema().clone(), data_vec)?;
        Poll::Ready(Some(Ok(batch)))
    }
}
