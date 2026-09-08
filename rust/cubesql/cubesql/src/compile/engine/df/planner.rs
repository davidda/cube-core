use std::sync::Arc;

use crate::config::ConfigObj;
use async_trait::async_trait;
use datafusion::{
    arrow::record_batch::{RecordBatch, RecordBatchOptions},
    error::Result,
    execution::context::{QueryPlanner, SessionState},
    logical_plan::LogicalPlan,
    physical_plan::{
        empty::EmptyExec, memory::MemoryExec, planner::DefaultPhysicalPlanner,
        subquery::SubqueryExec, ExecutionPlan, PhysicalPlanner,
    },
};

use crate::transport::{LoadRequestMeta, TransportService};

use super::scan::CubeScanExtensionPlanner;

pub struct CubeQueryPlanner {
    pub transport: Arc<dyn TransportService>,
    pub meta: LoadRequestMeta,
    pub config_obj: Arc<dyn ConfigObj>,
}

impl CubeQueryPlanner {
    pub fn new(
        transport: Arc<dyn TransportService>,
        meta: LoadRequestMeta,
        config_obj: Arc<dyn ConfigObj>,
    ) -> Self {
        Self {
            transport,
            meta,
            config_obj,
        }
    }
}

#[async_trait]
impl QueryPlanner for CubeQueryPlanner {
    /// Given a `LogicalPlan` created from above, create an
    /// `ExecutionPlan` suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let physical_planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            CubeScanExtensionPlanner {
                transport: self.transport.clone(),
                meta: self.meta.clone(),
                config_obj: self.config_obj.clone(),
            },
        )]);
        // Delegate most work of physical planning to the default physical planner
        let plan = physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await?;
        align_subquery_input(plan)
    }
}

// The pinned DataFusion EmptyExec emits a placeholder column even when its schema
// is empty. SubqueryExec copies those batch columns before appending scalar results,
// so its batch no longer matches its schema. Supply the declared zero-column row
// at this boundary; the outer projection still selects and names the real results.
fn align_subquery_input(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let original = plan.children();
    let mut children = original
        .iter()
        .cloned()
        .map(align_subquery_input)
        .collect::<Result<Vec<_>>>()?;
    if plan.as_any().is::<SubqueryExec>() {
        if let Some(input) = children.first_mut() {
            if let Some(empty) = input.as_any().downcast_ref::<EmptyExec>() {
                if empty.produce_one_row() && input.schema().fields().is_empty() {
                    let schema = input.schema();
                    let mut options = RecordBatchOptions::default();
                    options.row_count = Some(1);
                    let batch =
                        RecordBatch::try_new_with_options(schema.clone(), vec![], &options)?;
                    let partitions =
                        vec![vec![batch]; input.output_partitioning().partition_count()];
                    *input = Arc::new(MemoryExec::try_new(&partitions, schema, None)?);
                }
            }
        }
    }
    if children
        .iter()
        .zip(&original)
        .any(|(a, b)| !Arc::ptr_eq(a, b))
    {
        plan.with_new_children(children)
    } else {
        Ok(plan)
    }
}
