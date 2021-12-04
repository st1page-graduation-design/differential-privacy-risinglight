use crate::{
    logical_optimizer::PlanRewriter,
    logical_planner::{LogicalPlan, LogicalPlanRef},
};
use enum_dispatch::enum_dispatch;

mod aggregate;
mod copy;
mod create;
mod delete;
mod drop;
mod dummy;
mod explain;
mod filter;
mod input_ref_resolver;
mod insert;
mod join;
mod limit;
mod order;
mod projection;
mod seq_scan;

pub use aggregate::*;
pub use copy::*;
pub use create::*;
pub use delete::*;
pub use drop::*;
pub use dummy::*;
pub use explain::*;
pub use filter::*;
pub use input_ref_resolver::*;
pub use insert::*;
pub use join::*;
pub use limit::*;
pub use order::*;
pub use projection::*;
pub use seq_scan::*;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum PhysicalPlanError {
    #[error("invalid SQL")]
    InvalidLogicalPlan,
}

#[enum_dispatch(Explain)]
#[derive(Debug, PartialEq, Clone)]
pub enum PhysicalPlan {
    PhysicalDummy,
    PhysicalSeqScan,
    PhysicalInsert,
    PhysicalValues,
    PhysicalCreateTable,
    PhysicalDrop,
    PhysicalProjection,
    PhysicalFilter,
    PhysicalExplain,
    PhysicalJoin,
    PhysicalSimpleAgg,
    PhysicalHashAgg,
    PhysicalOrder,
    PhysicalLimit,
    PhysicalDelete,
    PhysicalCopyFromFile,
    PhysicalCopyToFile,
}

#[derive(Default)]
pub struct PhysicalPlaner;

impl PhysicalPlaner {
    fn plan_inner(&self, plan: &LogicalPlan) -> Result<PhysicalPlan, PhysicalPlanError> {
        match plan {
            LogicalPlan::LogicalDummy(plan) => self.plan_dummy(plan),
            LogicalPlan::LogicalCreateTable(plan) => self.plan_create_table(plan),
            LogicalPlan::LogicalDrop(plan) => self.plan_drop(plan),
            LogicalPlan::LogicalInsert(plan) => self.plan_insert(plan),
            LogicalPlan::LogicalValues(plan) => self.plan_values(plan),
            LogicalPlan::LogicalJoin(plan) => self.plan_join(plan),
            LogicalPlan::LogicalGet(plan) => self.plan_get(plan),
            LogicalPlan::LogicalProjection(plan) => self.plan_projection(plan),
            LogicalPlan::LogicalFilter(plan) => self.plan_filter(plan),
            LogicalPlan::LogicalOrder(plan) => self.plan_order(plan),
            LogicalPlan::LogicalLimit(plan) => self.plan_limit(plan),
            LogicalPlan::LogicalExplain(plan) => self.plan_explain(plan),
            LogicalPlan::LogicalAggregate(plan) => self.plan_aggregate(plan),
            LogicalPlan::LogicalDelete(plan) => self.plan_delete(plan),
            LogicalPlan::LogicalCopyFromFile(plan) => self.plan_copy_from_file(plan),
            LogicalPlan::LogicalCopyToFile(plan) => self.plan_copy_to_file(plan),
        }
    }

    pub fn plan(&self, plan: LogicalPlanRef) -> Result<PhysicalPlan, PhysicalPlanError> {
        // Resolve input reference
        let plan = InputRefResolver::default().rewrite_plan(plan);

        // Create physical plan
        self.plan_inner(&plan)
    }
}

#[enum_dispatch]
pub trait Explain {
    fn explain_inner(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result;

    fn explain(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", " ".repeat(level * 2))?;
        self.explain_inner(level, f)
    }
}

impl std::fmt::Display for PhysicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.explain(0, f)
    }
}
