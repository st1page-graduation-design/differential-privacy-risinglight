use super::*;
use crate::binder::{BoundDrop, Object};

/// The logical plan of `drop`.
#[derive(Debug, Clone)]
pub struct LogicalDrop {
    pub object: Object,
}

impl_logical_plan!(LogicalDrop);

impl LogicalPlaner {
    pub fn plan_drop(&self, stmt: BoundDrop) -> Result<LogicalPlanRef, LogicalPlanError> {
        Ok(LogicalDrop {
            object: stmt.object,
        }
        .into())
    }
}
