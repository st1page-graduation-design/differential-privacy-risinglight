use super::*;
use crate::binder::{BoundDelete, BoundTableRef};
use crate::catalog::TableRefId;

/// The logical plan of `delete`.
#[derive(Debug, Clone)]
pub struct LogicalDelete {
    pub table_ref_id: TableRefId,
    pub child: LogicalPlanRef,
}

impl_logical_plan!(LogicalDelete, [child]);

impl LogicalPlaner {
    pub fn plan_delete(&self, stmt: BoundDelete) -> Result<LogicalPlanRef, LogicalPlanError> {
        if let BoundTableRef::BaseTableRef { ref ref_id, .. } = stmt.from_table {
            if let Some(expr) = stmt.where_clause {
                let child = self.plan_table_ref(&stmt.from_table, true, false)?;
                Ok(LogicalDelete {
                    table_ref_id: *ref_id,
                    child: LogicalFilter { expr, child }.into(),
                }
                .into())
            } else {
                panic!("delete whole table is not supported yet")
            }
        } else {
            panic!("unsupported table")
        }
    }
}
