use itertools::Itertools;

use super::*;
use crate::catalog::TableRefId;
use crate::logical_planner::LogicalGet;
use crate::types::ColumnId;

/// The physical plan of sequential scan operation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalSeqScan {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub with_row_handler: bool,
    pub is_sorted: bool,
}

impl PhysicalPlaner {
    pub fn plan_get(&self, plan: &LogicalGet) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::SeqScan(PhysicalSeqScan {
            table_ref_id: plan.table_ref_id,
            column_ids: plan.column_ids.clone(),
            with_row_handler: plan.with_row_handler,
            is_sorted: plan.is_sorted,
        }))
    }
}

impl PlanExplainable for PhysicalSeqScan {
    fn explain_inner(&self, _level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "SeqScan: table #{}, columns [{}], with_row_handler: {}, is_sorted: {}",
            self.table_ref_id.table_id,
            self.column_ids.iter().map(ToString::to_string).join(", "),
            self.with_row_handler,
            self.is_sorted
        )
    }
}
