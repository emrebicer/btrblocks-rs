use core::fmt;
use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::path::PathBuf;
use std::sync::Arc;

use crate::Btr;
use crate::ColumnType;
use datafusion::arrow::array::Array;
use datafusion::arrow::array::Float64Builder;
use datafusion::arrow::array::Int32Builder;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion_expr::Expr;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};

#[derive(Debug, Clone)]
pub struct BtrBlocksDataSource {
    pub btr: Btr,
}

impl BtrBlocksDataSource {
    pub fn new(btr_path: PathBuf) -> Self {
        Self {
            btr: Btr::from_path(btr_path),
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
        let file_metadata = self.btr.file_metadata();
        let mut fields = vec![];

        let mut counter = 0;
        for column in file_metadata.parts {
            let data_type = match column.r#type {
                ColumnType::Integer => DataType::Int32,
                ColumnType::Double => DataType::Float64,
                ColumnType::String => DataType::Utf8,
                _ => DataType::Null,
            };

            // NOTE: there is no way to get the actual column name, it does not exist in the
            // metadata
            fields.push(Field::new(format!("column_{counter}"), data_type, true));
            counter += 1;
        }

        SchemaRef::new(Schema::new(fields))
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
        // Get the metadata
        let meta = self.data_source.btr.file_metadata();

        // Vector to hold Arrow arrays for the columns
        let mut data_vec: Vec<Arc<dyn Array>> = vec![];

        for (col_index, part_info) in meta.parts.iter().enumerate() {
            match part_info.r#type {
                ColumnType::Integer => {
                    let mut vec = Vec::new();
                    for part_index in 0..part_info.num_parts {
                        vec.append(
                            &mut self
                                .data_source
                                .btr
                                .decompress_column_part_i32(col_index as u32, part_index)
                                .expect("decompression should not fail"),
                        );
                    }

                    // Read the decompressed values into an Arrow array
                    let mut builder = Int32Builder::new();

                    for val in vec {
                        builder.append_value(val);
                    }

                    // Finalize the array and add it to the vector
                    data_vec.push(Arc::new(builder.finish()));
                }
                ColumnType::Double => {
                    let mut vec = Vec::new();
                    for part_index in 0..part_info.num_parts {
                        vec.append(
                            &mut self
                                .data_source
                                .btr
                                .decompress_column_part_f64(col_index as u32, part_index)
                                .expect("decompression should not fail"),
                        );
                    }

                    // Read the decompressed values into an Arrow array
                    let mut builder = Float64Builder::new(); // For UInt32 column

                    for val in vec {
                        builder.append_value(val);
                    }

                    // Finalize the array and add it to the vector
                    data_vec.push(Arc::new(builder.finish()));
                }
                ColumnType::String => {
                    let mut vec = Vec::new();
                    for part_index in 0..part_info.num_parts {
                        vec.append(
                            &mut self
                                .data_source
                                .btr
                                .decompress_column_part_string(col_index as u32, part_index)
                                .expect("decompression should not fail"),
                        );
                    }

                    // Read the decompressed values into an Arrow array
                    let mut builder = StringBuilder::new(); // For UInt32 column

                    for val in vec {
                        builder.append_value(val);
                    }

                    // Finalize the array and add it to the vector
                    data_vec.push(Arc::new(builder.finish()));
                }
                _ => {
                    unimplemented!("Column type not supported");
                }
            }
        }

        // Create a single RecordBatch from the data and schema
        let schema: SchemaRef = self.schema();

        let batch = RecordBatch::try_new(schema.clone(), data_vec)?;

        // Wrap the batch in a MemoryStream
        Ok(Box::pin(MemoryStream::try_new(vec![batch], schema, None)?))
    }
}
