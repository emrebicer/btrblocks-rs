use datafusion::arrow::datatypes::SchemaRef;
use std::cmp::min;
use std::sync::Arc;
use std::task::Poll;

use crate::error::BtrBlocksError;
use crate::Btr;
use crate::ColumnType;
use datafusion::arrow::array::{Array, Float64Builder, Int32Builder, RecordBatch, StringBuilder};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::RecordBatchStream;
use futures::stream::Stream;

/// A `stream` that reads btr and decompresses data part by part on each poll
pub struct ChunkedDecompressionStream {
    btr: Btr,
    schema_ref: SchemaRef,
    column_caches: Vec<DecompressedColumnCache>,
    num_rows_per_poll: usize,
}

impl ChunkedDecompressionStream {
    pub async fn new(
        btr: Btr,
        num_rows_per_poll: usize,
    ) -> crate::Result<Self> {
        let mut column_caches = vec![];

        let metadata = btr.file_metadata().await?;
        let schema_ref = metadata.to_schema_ref()?;

        for (counter, field) in metadata.parts.into_iter().enumerate() {
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

impl RecordBatchStream for ChunkedDecompressionStream {
    fn schema(&self) -> SchemaRef {
        self.schema_ref.clone()
    }
}

impl Stream for ChunkedDecompressionStream {
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
