use datafusion::arrow::datatypes::SchemaRef;
use std::cmp::min;
use std::sync::Arc;
use std::task::Poll;

use crate::error::BtrBlocksError;
use crate::Btr;
use crate::ColumnType;
use datafusion::arrow::array::{Array, Float64Builder, Int32Builder, RecordBatch, StringBuilder};
use datafusion::error::Result;
use datafusion::execution::RecordBatchStream;
use futures::stream::Stream;
use tokio::sync::Mutex;

/// A `stream` that reads btr and decompresses data part by part on each poll
pub struct ChunkedDecompressionStream {
    btr: Btr,
    schema_ref: SchemaRef,
    column_caches: Vec<Arc<Mutex<DecompressedColumnCache>>>,
    num_rows_per_poll: usize,
}

impl ChunkedDecompressionStream {
    pub async fn new(btr: Btr, num_rows_per_poll: usize) -> crate::Result<Self> {
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
            column_caches.push(Arc::new(Mutex::new(cache)));
        }

        Ok(Self {
            btr,
            schema_ref,
            column_caches,
            num_rows_per_poll,
        })
    }

    async fn is_decompression_finished(&mut self) -> bool {
        let mut finished_count = 0;
        for column in &mut self.column_caches {
            let column = column.lock().await;
            if column.finished() {
                finished_count += 1;
            }
        }

        return finished_count == self.column_caches.len();
    }

    // Keep decompressing and reading data until there is enough cache
    // to satisfy the num_rows_per_poll
    //
    // Spawns new threads for individual column decompressions
    async fn decompress_until_enough_cache(&mut self) -> crate::Result<()> {
        let mut handles = vec![];
        let nrpp = self.num_rows_per_poll.to_owned();
        for index in 0..self.column_caches.len() {
            let col = Arc::clone(&self.column_caches[index]);
            let btr = self.btr.clone();
            handles.push(tokio::spawn(async move {
                let col_guard = col.lock();
                col_guard.await.read_until_enough_cache(&btr, nrpp).await
            }));
        }

        for handle in handles {
            match handle.await {
                Ok(_res) => {}
                Err(err) => {
                    return Err(BtrBlocksError::Custom(format!(
                        "failed to read until enough cache: {}",
                        err
                    )))
                }
            }
        }

        Ok(())
    }

    // Populate the data for the datafusion stream from the existing cache
    async fn populate_data_vec(&mut self) -> Vec<Arc<dyn Array>> {
        let mut data_vec: Vec<Arc<dyn Array>> = vec![];
        for column in &mut self.column_caches {
            let mut column = column.lock().await;
            let num_elements_to_consume = min(column.current_cache_len(), self.num_rows_per_poll);

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

        return data_vec;
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
        // Check if the decompression is finished
        if tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.is_decompression_finished())
        }) {
            return Poll::Ready(None);
        }

        // Decompress more parts until we have enough decompressed cache
        if let Err(e) = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.decompress_until_enough_cache())
        }) {
            println!("failed to decompress: {e}");
            return Poll::Pending;
        }

        // Read the data from the decompressed cache
        let data_vec = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.populate_data_vec())
        });

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
