use super::*;
use crate::binder::{BoundAggCall, BoundExpr};
use crate::logical_planner::LogicalAggregate;

/// The physical plan of simple aggregation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalSimpleAgg {
    pub agg_calls: Vec<BoundAggCall>,
    pub child: Box<PhysicalPlan>,
}

/// The physical plan of hash aggregation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalHashAgg {
    pub agg_calls: Vec<BoundAggCall>,
    pub group_keys: Vec<BoundExpr>,
    pub child: Box<PhysicalPlan>,
}

impl PhysicalPlaner {
    pub fn plan_aggregate(
        &self,
        plan: &LogicalAggregate,
    ) -> Result<PhysicalPlan, PhysicalPlanError> {
        if plan.group_keys.is_empty() {
            Ok(PhysicalSimpleAgg {
                agg_calls: plan.agg_calls.clone(),
                child: self.plan_inner(&plan.child)?.into(),
            }
            .into())
        } else {
            Ok(PhysicalHashAgg {
                agg_calls: plan.agg_calls.clone(),
                group_keys: plan.group_keys.clone(),
                child: self.plan_inner(&plan.child)?.into(),
            }
            .into())
        }
    }
}

impl Explain for PhysicalSimpleAgg {
    fn explain_inner(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "SimpleAgg: {:?}", self.agg_calls)?;
        self.child.explain(level + 1, f)
    }
}

impl Explain for PhysicalHashAgg {
    fn explain_inner(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "HashAgg: {} agg calls", self.agg_calls.len(),)?;
        self.child.explain(level + 1, f)
    }
}
