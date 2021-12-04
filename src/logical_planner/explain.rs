use super::*;

/// The logical plan of `explain`.
#[derive(Debug, Clone)]
pub struct LogicalExplain {
    pub child: LogicalPlanRef,
}

impl_logical_plan!(LogicalExplain, [child]);

impl LogicalPlaner {
    pub fn plan_explain(&self, stmt: BoundStatement) -> Result<LogicalPlanRef, LogicalPlanError> {
        Ok(LogicalExplain {
            child: self.plan(stmt)?,
        }
        .into())
    }
}
