use core::fmt;
use std::any::Any;
use std::cmp::max;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::stream::ChunkedDecompressionStream;
use crate::Btr;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_expr::Expr;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};

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
                tokio::runtime::Handle::current().block_on(ChunkedDecompressionStream::new(
                    self.data_source.btr.clone(),
                    1_000_000 / column_count,
                ))
            })
            .map_err(|err| DataFusionError::External(Box::new(err)))?,
        ))
    }
}
