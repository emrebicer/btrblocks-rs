use std::task::Poll;

use datafusion::arrow::array::Array;
use futures::{Stream, StreamExt};

use crate::{error::BtrBlocksError, util::extract_value_as_string, Btr, Result};

use super::chunked_decompression::ChunkedDecompressionStream;

/// A `stream` that decompresses btr into csv, returns `num_rows_per_poll` rows on each poll
pub struct CsvDecompressionStream {
    column_count: usize,
    chunked_decompression_stream: ChunkedDecompressionStream,
    csv_header: String,
    first_poll: bool,
}

impl CsvDecompressionStream {
    pub async fn new(btr: Btr, num_rows_per_poll: usize) -> crate::Result<Self> {
        let file_metadata = btr.file_metadata().await?;

        let column_count = file_metadata.num_columns as usize;

        let chunked_decompression_stream =
            ChunkedDecompressionStream::new(btr, num_rows_per_poll).await?;

        let mut csv_header = String::new();

        for counter in 0..file_metadata.parts.len() {
            let field_name = format!("column_{counter}");
            csv_header.push_str(field_name.as_str());

            if counter + 1 < column_count {
                csv_header.push(',');
            }
        }

        Ok(Self {
            column_count,
            chunked_decompression_stream,
            csv_header,
            first_poll: true,
        })
    }
}

impl Stream for CsvDecompressionStream {
    type Item = Result<String>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut res = String::new();

        if self.first_poll {
            res.push_str(&self.csv_header);
            self.first_poll = false;
        }

        let batch_res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.chunked_decompression_stream.next())
        });

        if let Some(batch) = batch_res {
            let batch = batch.map_err(|err| BtrBlocksError::Custom(err.to_string()))?;
            let num_rows = batch.num_rows();
            let num_columns = batch.num_columns();

            let columns: Vec<&dyn Array> = (0..num_columns)
                .map(|col_index| batch.column(col_index).as_ref())
                .collect();

            for row_index in 0..num_rows {
                res.push('\n');

                for (counter, column) in columns.iter().enumerate() {
                    let value = extract_value_as_string(*column, row_index);
                    res.push_str(&value);

                    if counter + 1 < self.column_count {
                        res.push(',');
                    }
                }
            }

            Poll::Ready(Some(Ok(res)))
        } else {
            Poll::Ready(None)
        }
    }
}
