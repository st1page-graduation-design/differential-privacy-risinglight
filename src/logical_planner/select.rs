//! Logical planner of `select` statement.
//!
//! A `select` statement will be planned to a compose of:
//!
//! - [`LogicalGet`] (from *) or [`LogicalDummy`] (no from)
//! - [`LogicalJoin`] (from * join *)
//! - [`LogicalFilter`] (where *)
//! - [`LogicalProjection`] (select *)
//! - [`LogicalAggregate`] (count(*))
//! - [`LogicalOrder`] (order by *)
//! - [`LogicalLimit`] (limit *)

use super::*;
use crate::{
    binder::{
        BoundAggCall, BoundExpr, BoundInputRef, BoundJoinOperator, BoundOrderBy, BoundSelect,
        BoundTableRef,
    },
    catalog::TableRefId,
    types::ColumnId,
};

/// The logical plan of dummy get.
#[derive(Debug, Clone)]
pub struct LogicalDummy;

/// The logical plan of get.
#[derive(Debug, Clone)]
pub struct LogicalGet {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub with_row_handler: bool,
    pub is_sorted: bool,
}

/// The logical plan of join, it only records join tables and operators.
/// The query optimizer should decide the join orders and specific algorithms (hash join, nested
/// loop join or index join).
#[derive(Debug, Clone)]
pub struct LogicalJoin {
    pub left_plan: LogicalPlanRef,
    pub right_plan: LogicalPlanRef,
    pub join_op: BoundJoinOperator,
}

/// The logical plan of filter operation.
#[derive(Debug, Clone)]
pub struct LogicalFilter {
    pub expr: BoundExpr,
    pub child: LogicalPlanRef,
}

/// The logical plan of project operation.
#[derive(Debug, Clone)]
pub struct LogicalProjection {
    pub project_expressions: Vec<BoundExpr>,
    pub child: LogicalPlanRef,
}

/// The logical plan of hash aggregate operation.
#[derive(Debug, Clone)]
pub struct LogicalAggregate {
    pub agg_calls: Vec<BoundAggCall>,
    /// Group keys in hash aggregation (optional)
    pub group_keys: Vec<BoundExpr>,
    pub child: LogicalPlanRef,
}

/// The logical plan of order.
#[derive(Debug, Clone)]
pub struct LogicalOrder {
    pub comparators: Vec<BoundOrderBy>,
    pub child: LogicalPlanRef,
}

/// The logical plan of limit operation.
#[derive(Debug, Clone)]
pub struct LogicalLimit {
    pub offset: usize,
    pub limit: usize,
    pub child: LogicalPlanRef,
}

impl_logical_plan!(LogicalDummy);
impl_logical_plan!(LogicalGet);
impl_logical_plan!(LogicalJoin, [left_plan, right_plan]);
impl_logical_plan!(LogicalFilter, [child]);
impl_logical_plan!(LogicalProjection, [child]);
impl_logical_plan!(LogicalAggregate, [child]);
impl_logical_plan!(LogicalOrder, [child]);
impl_logical_plan!(LogicalLimit, [child]);

impl LogicalPlaner {
    pub fn plan_select(
        &self,
        mut stmt: Box<BoundSelect>,
    ) -> Result<LogicalPlanRef, LogicalPlanError> {
        let mut plan: LogicalPlanRef = LogicalDummy.into();
        let mut is_sorted = false;

        if let Some(table_ref) = stmt.from_table.get(0) {
            // use `sorted` mode from the storage engine if the order by column is the primary key
            if stmt.orderby.len() == 1 && !stmt.orderby[0].descending {
                if let BoundExpr::ColumnRef(col_ref) = &stmt.orderby[0].expr {
                    if col_ref.is_primary_key {
                        is_sorted = true;
                    }
                }
            }
            plan = self.plan_table_ref(table_ref, false, is_sorted)?;
        }

        if let Some(expr) = stmt.where_clause {
            plan = LogicalFilter { expr, child: plan }.into();
        }

        let mut agg_extractor = AggExtractor::new(stmt.group_by.len());
        for expr in &mut stmt.select_list {
            agg_extractor.visit_expr(expr);
        }
        if !agg_extractor.agg_calls.is_empty() {
            plan = LogicalAggregate {
                agg_calls: agg_extractor.agg_calls,
                group_keys: stmt.group_by,
                child: plan,
            }
            .into();
        }

        // TODO: support the following clauses
        assert!(!stmt.select_distinct, "TODO: plan distinct");

        if !stmt.select_list.is_empty() {
            plan = LogicalProjection {
                project_expressions: stmt.select_list,
                child: plan,
            }
            .into();
        }
        if !stmt.orderby.is_empty() && !is_sorted {
            plan = LogicalOrder {
                comparators: stmt.orderby,
                child: plan,
            }
            .into();
        }
        if stmt.limit.is_some() || stmt.offset.is_some() {
            let limit = match stmt.limit {
                Some(limit) => match limit {
                    BoundExpr::Constant(v) => v.as_usize()?.unwrap_or(usize::MAX / 2),
                    _ => panic!("limit only support constant expression"),
                },
                None => usize::MAX / 2, // avoid 'offset + limit' overflow
            };
            let offset = match stmt.offset {
                Some(offset) => match offset {
                    BoundExpr::Constant(v) => v.as_usize()?.unwrap_or(0),
                    _ => panic!("offset only support constant expression"),
                },
                None => 0,
            };
            plan = LogicalLimit {
                offset,
                limit,
                child: plan,
            }
            .into();
        }
        Ok(plan)
    }

    pub fn plan_table_ref(
        &self,
        table_ref: &BoundTableRef,
        with_row_handler: bool,
        is_sorted: bool,
    ) -> Result<LogicalPlanRef, LogicalPlanError> {
        match table_ref {
            BoundTableRef::BaseTableRef {
                ref_id,
                table_name: _,
                column_ids,
            } => Ok(LogicalGet {
                table_ref_id: *ref_id,
                column_ids: column_ids.to_vec(),
                with_row_handler,
                is_sorted,
            }
            .into()),
            BoundTableRef::JoinTableRef {
                relation,
                join_tables,
            } => {
                let mut plan = self.plan_table_ref(relation, with_row_handler, is_sorted)?;
                for join_table in join_tables.iter() {
                    let table_plan =
                        self.plan_table_ref(&join_table.table_ref, with_row_handler, is_sorted)?;
                    plan = LogicalJoin {
                        left_plan: plan,
                        right_plan: table_plan,
                        join_op: join_table.join_op.clone(),
                    }
                    .into();
                }
                Ok(plan)
            }
        }
    }
}

/// An expression visitor that extracts aggregation nodes and replaces them with `InputRef`.
///
/// For example:
/// In SQL: `select sum(b) + a * count(a) from t group by a;`
/// The expression `sum(b) + a * count(a)` will be rewritten to `InputRef(1) + a * InputRef(2)`,
/// because the underlying aggregate plan will output `(a, sum(b), count(a))`. The group keys appear
/// before aggregations.
#[derive(Default)]
struct AggExtractor {
    agg_calls: Vec<BoundAggCall>,
    index: usize,
}

impl AggExtractor {
    fn new(group_key_count: usize) -> Self {
        AggExtractor {
            agg_calls: vec![],
            index: group_key_count,
        }
    }

    fn visit_expr(&mut self, expr: &mut BoundExpr) {
        use BoundExpr::*;
        match expr {
            AggCall(agg) => {
                let input_ref = InputRef(BoundInputRef {
                    index: self.index,
                    return_type: agg.return_type.clone(),
                });
                match std::mem::replace(expr, input_ref) {
                    AggCall(agg) => self.agg_calls.push(agg),
                    _ => unreachable!(),
                }
                self.index += 1;
            }
            BinaryOp(bin_op) => {
                self.visit_expr(&mut bin_op.left_expr);
                self.visit_expr(&mut bin_op.right_expr);
            }
            UnaryOp(unary_op) => self.visit_expr(&mut unary_op.expr),
            TypeCast(type_cast) => self.visit_expr(&mut type_cast.expr),
            IsNull(is_null) => self.visit_expr(&mut is_null.expr),
            Constant(_) | ColumnRef(_) | InputRef(_) => {}
        }
    }
}
