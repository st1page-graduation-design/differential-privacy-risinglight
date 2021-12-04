use crate::{binder::BoundStatement, types::ConvertError};
use enum_dispatch::enum_dispatch;
use smallvec::SmallVec;
use std::fmt::Debug;
use std::rc::Rc;

/// Macro to implement `LogicalPlanTrait` for logical plan structure.
macro_rules! impl_logical_plan {
    ($type:ident) => {
        impl_logical_plan!($type,[]);
    };
    ($type:ident,[$($child:ident),*]) => {
        impl LogicalPlanTrait for $type {
            fn children(&self) -> SmallVec<[LogicalPlanRef; 2]> {
                smallvec::smallvec![$(self.$child.clone()),*]
            }
            #[allow(unused_mut)]
            fn clone_with_children(&self, children: impl IntoIterator<Item = LogicalPlanRef>) -> LogicalPlanRef {
                let mut iter = children.into_iter();
                let mut new = self.clone();
                $(
                    new.$child = iter.next().expect("invalid children number");
                )*
                assert!(iter.next().is_none(), "invalid children number");
                new.into()
            }
        }
        impl From<$type> for LogicalPlanRef {
            fn from(plan: $type) -> LogicalPlanRef {
                Rc::new(plan.into())
            }
        }
    };
}

mod copy;
mod create;
mod delete;
mod drop;
mod explain;
mod insert;
mod select;

pub use copy::*;
pub use create::*;
pub use delete::*;
pub use drop::*;
pub use explain::*;
pub use insert::*;
pub use select::*;

/// The error type of logical planner.
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum LogicalPlanError {
    #[error("conversion error: {0}")]
    Convert(#[from] ConvertError),
}

/// The trait of logical plan.
#[enum_dispatch]
pub trait LogicalPlanTrait: Debug {
    fn children(&self) -> SmallVec<[LogicalPlanRef; 2]>;
    fn clone_with_children(
        &self,
        children: impl IntoIterator<Item = LogicalPlanRef>,
    ) -> LogicalPlanRef;
}

/// The reference type to a logical plan.
pub type LogicalPlanRef = Rc<LogicalPlan>;

#[enum_dispatch(LogicalPlanTrait)]
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    LogicalDummy,
    LogicalGet,
    LogicalInsert,
    LogicalValues,
    LogicalCreateTable,
    LogicalDrop,
    LogicalProjection,
    LogicalFilter,
    LogicalExplain,
    LogicalJoin,
    LogicalAggregate,
    LogicalOrder,
    LogicalLimit,
    LogicalDelete,
    LogicalCopyFromFile,
    LogicalCopyToFile,
}

#[derive(Default)]
pub struct LogicalPlaner;

impl LogicalPlaner {
    /// Generate the logical plan from a bound statement.
    pub fn plan(&self, stmt: BoundStatement) -> Result<LogicalPlanRef, LogicalPlanError> {
        match stmt {
            BoundStatement::CreateTable(stmt) => self.plan_create_table(stmt),
            BoundStatement::Drop(stmt) => self.plan_drop(stmt),
            BoundStatement::Insert(stmt) => self.plan_insert(stmt),
            BoundStatement::Copy(stmt) => self.plan_copy(stmt),
            BoundStatement::Select(stmt) => self.plan_select(stmt),
            BoundStatement::Explain(stmt) => self.plan_explain(*stmt),
            BoundStatement::Delete(stmt) => self.plan_delete(*stmt),
        }
    }
}
