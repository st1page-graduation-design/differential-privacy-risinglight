use super::*;
use crate::binder::{BoundExpr, BoundInsert};
use crate::catalog::TableRefId;
use crate::types::{ColumnId, DataType};

/// The logical plan of `INSERT`.
#[derive(Debug, Clone)]
pub struct LogicalInsert {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub child: LogicalPlanRef,
}

impl_logical_plan!(LogicalInsert, [child]);

/// The logical plan of `VALUES`.
#[derive(Debug, Clone)]
pub struct LogicalValues {
    pub column_types: Vec<DataType>,
    pub values: Vec<Vec<BoundExpr>>,
}

impl_logical_plan!(LogicalValues);

impl LogicalPlaner {
    pub fn plan_insert(&self, stmt: BoundInsert) -> Result<LogicalPlanRef, LogicalPlanError> {
        Ok(LogicalInsert {
            table_ref_id: stmt.table_ref_id,
            column_ids: stmt.column_ids,
            child: LogicalValues {
                column_types: stmt.column_types,
                values: stmt.values,
            }
            .into(),
        }
        .into())
    }
}
