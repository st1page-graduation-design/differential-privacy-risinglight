// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::binder::*;
use crate::catalog::ColumnRefId;

/// Resolves column references into physical indices into the `DataChunk`.
///
/// This will rewrite all `BoundExpr` expressions to `InputRef`.
#[derive(Default)]
pub struct InputRefResolver {
    /// The output columns of the last visited plan.
    ///
    /// For those plans that don't change columns (e.g. Order, Filter), this variable should
    /// not be touched. For other plans that change columns (e.g. SeqScan, Join, Projection,
    /// Aggregate), this variable should be set before the function returns.
    bindings: Vec<Option<BoundExpr>>,
}

impl ExprRewriter for InputRefResolver {
    fn rewrite_column_ref(&self, expr: &mut BoundExpr) {
        use BoundExpr::*;
        match expr {
            BoundExpr::ColumnRef(_) => {
                if let Some(idx) = self
                    .bindings
                    .iter()
                    .position(|col| *col == Some(expr.clone()))
                {
                    *expr = InputRef(BoundInputRef {
                        index: idx,
                        return_type: expr.return_type().unwrap(),
                    });
                }
            }
            _ => unreachable!(),
        }
    }
}

impl PlanRewriter for InputRefResolver {
    fn rewrite_logical_join(&mut self, join: &LogicalJoin) -> PlanRef {
        let left = self.rewrite(join.left());
        let mut resolver = Self::default();
        let right = resolver.rewrite(join.right());
        self.bindings.append(&mut resolver.bindings);
        Arc::new(join.clone_with_rewrite_expr(left, right, self))
    }

    fn rewrite_logical_table_scan(&mut self, plan: &LogicalTableScan) -> PlanRef {
        self.bindings = plan
            .column_ids()
            .iter()
            .zip(plan.column_descs())
            .map(|(col_id, col_desc)| {
                Some(BoundExpr::ColumnRef(BoundColumnRef {
                    column_ref_id: ColumnRefId::from_table(plan.table_ref_id(), *col_id),
                    is_primary_key: col_desc.is_primary(),
                    desc: col_desc.clone(),
                }))
            })
            .collect();
        Arc::new(plan.clone())
    }

    fn rewrite_internal(&mut self, plan: &Internal) -> PlanRef {
        self.bindings = plan
            .column_ids()
            .iter()
            .zip(plan.column_descs())
            .map(|(col_id, col_desc)| {
                Some(BoundExpr::ColumnRef(BoundColumnRef {
                    column_ref_id: ColumnRefId::from_table(plan.table_ref_id(), *col_id),
                    is_primary_key: col_desc.is_primary(),
                    desc: col_desc.clone(),
                }))
            })
            .collect();
        Arc::new(plan.clone())
    }

    fn rewrite_logical_projection(&mut self, proj: &LogicalProjection) -> PlanRef {
        let new_child = self.rewrite(proj.child());
        let bindings = proj
            .project_expressions()
            .iter()
            .map(|e| Some(e.clone()))
            .collect();

        let ret = match new_child.node_type() {
            PlanNodeType::LogicalAggregate => {
                let group_keys = self
                    .bindings
                    .iter()
                    .map(|it| it.clone().unwrap())
                    .collect::<Vec<_>>();
                let mut resolver = AggInputRefResolver::new(group_keys.len());
                let mut select_list = proj.project_expressions().to_vec();
                for expr in &mut select_list {
                    resolver.resolve_select_expr(expr, &group_keys);
                }
                let new_proj = LogicalProjection::new(select_list, new_child.clone());
                Arc::new(new_proj.clone_with_rewrite_expr(new_child, self))
            }
            _ => Arc::new(proj.clone_with_rewrite_expr(new_child, self)),
        };

        self.bindings = bindings;
        ret
    }

    fn rewrite_logical_aggregate(&mut self, agg: &LogicalAggregate) -> PlanRef {
        let new_child = self.rewrite(agg.child());
        let bindings = agg.group_keys().iter().map(|e| Some(e.clone())).collect();
        let ret = Arc::new(agg.clone_with_rewrite_expr(new_child, self));
        self.bindings = bindings;
        ret
    }
    fn rewrite_logical_filter(&mut self, plan: &LogicalFilter) -> PlanRef {
        let child = self.rewrite(plan.child());
        Arc::new(plan.clone_with_rewrite_expr(child, self))
    }
    fn rewrite_logical_order(&mut self, plan: &LogicalOrder) -> PlanRef {
        let child = self.rewrite(plan.child());
        Arc::new(plan.clone_with_rewrite_expr(child, self))
    }
    fn rewrite_logical_values(&mut self, plan: &LogicalValues) -> PlanRef {
        Arc::new(plan.clone_with_rewrite_expr(self))
    }
}

/// Resolves select expression into `InputRef` using group by expressions
/// for parent node of `LogicalAggregate`.
#[derive(Default)]
struct AggInputRefResolver {
    agg_start_index: usize,
}

impl AggInputRefResolver {
    fn new(group_key_count: usize) -> Self {
        AggInputRefResolver {
            agg_start_index: group_key_count,
        }
    }

    /// using group by exprs to resolve select expr into `InputRef` which include two cases:
    /// 1. found identical select expr in group by exprs and replace it with `InputRef`
    /// 2. found aggregate function in select expr and replace it with `InputRef`
    fn resolve_select_expr(&mut self, expr: &mut BoundExpr, group_keys: &Vec<BoundExpr>) {
        use BoundExpr::*;

        // if found identical select expr in group by exprs, replace select expr with `InputRef`
        if let Some(i) = group_keys.iter().position(|e| e == expr) {
            *expr = InputRef(BoundInputRef {
                index: i,
                return_type: expr.return_type().unwrap(),
            });
            return;
        }

        match expr {
            // due to aggregate exprs are behind group by exprs, so we used group by exprs count as
            // InputRef's based index
            AggCall(agg) => {
                *expr = InputRef(BoundInputRef {
                    index: self.agg_start_index,
                    return_type: agg.return_type.clone(),
                });
                self.agg_start_index += 1;
            }
            BinaryOp(bin_op) => {
                self.resolve_select_expr(&mut bin_op.left_expr, group_keys);
                self.resolve_select_expr(&mut bin_op.right_expr, group_keys);
            }
            UnaryOp(unary_op) => self.resolve_select_expr(&mut unary_op.expr, group_keys),
            TypeCast(type_cast) => self.resolve_select_expr(&mut type_cast.expr, group_keys),
            ExprWithAlias(expr_with_alias) => {
                self.resolve_select_expr(&mut expr_with_alias.expr, group_keys)
            }
            IsNull(isnull) => self.resolve_select_expr(&mut isnull.expr, group_keys),
            Constant(_) | ColumnRef(_) | InputRef(_) | Alias(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::BinaryOperator;

    use super::*;
    use crate::binder::AggKind;
    use crate::types::{DataTypeExt, DataTypeKind, DataValue};

    #[test]
    /// To be resolved SQL:
    /// ```sql
    /// select v2 + 1, sum(v1) from t group by v2 + 1
    /// ```
    /// After resolved select list: `[InputRef #0, InputRef #1]`
    fn test_resolve_select_expr() {
        let sum_v1_call = BoundExpr::AggCall(BoundAggCall {
            kind: AggKind::Sum,
            args: vec![BoundExpr::ColumnRef(BoundColumnRef {
                column_ref_id: ColumnRefId::new(0, 0, 0, 0),
                is_primary_key: false,
                desc: DataTypeKind::Int(None).not_null().to_column("v1".into()),
            })],
            return_type: DataTypeKind::Int(None).not_null(),
        });
        let v2_plus_1_expr = BoundExpr::BinaryOp(BoundBinaryOp {
            op: BinaryOperator::Plus,
            left_expr: BoundExpr::ColumnRef(BoundColumnRef {
                column_ref_id: ColumnRefId::new(0, 0, 0, 1),
                is_primary_key: false,
                desc: DataTypeKind::Int(None).not_null().to_column("v2".into()),
            })
            .into(),
            right_expr: BoundExpr::Constant(DataValue::Int32(1)).into(),
            return_type: Some(DataTypeKind::Int(None).not_null()),
        });
        let group_keys = vec![v2_plus_1_expr.clone()];
        let mut select_list = vec![v2_plus_1_expr, sum_v1_call];

        let mut resolver = AggInputRefResolver::new(group_keys.len());
        for expr in &mut select_list {
            resolver.resolve_select_expr(expr, &group_keys);
        }

        assert_eq!(
            select_list[0],
            BoundExpr::InputRef(BoundInputRef {
                index: 0,
                return_type: DataTypeKind::Int(None).not_null(),
            })
        );
        assert_eq!(
            select_list[1],
            BoundExpr::InputRef(BoundInputRef {
                index: 1,
                return_type: DataTypeKind::Int(None).not_null(),
            })
        );
    }

    #[test]
    /// To be resolved SQL:
    /// ```sql
    /// select v2 + 1 + sum(v1) from t group by v2 + 1
    /// ```
    /// After resolved select list: `[Plus(InputRef #0, InputRef #1)]`
    fn test_resolve_select_expr_plus_agg_call() {
        let sum_v1_call = BoundExpr::AggCall(BoundAggCall {
            kind: AggKind::Sum,
            args: vec![BoundExpr::ColumnRef(BoundColumnRef {
                column_ref_id: ColumnRefId::new(0, 0, 0, 0),
                is_primary_key: false,
                desc: DataTypeKind::Int(None).not_null().to_column("v1".into()),
            })],
            return_type: DataTypeKind::Int(None).not_null(),
        });
        let v2_expr = BoundExpr::ColumnRef(BoundColumnRef {
            column_ref_id: ColumnRefId::new(0, 0, 0, 1),
            is_primary_key: false,
            desc: DataTypeKind::Int(None).not_null().to_column("v2".into()),
        });
        let v2_plus_1_expr = BoundExpr::BinaryOp(BoundBinaryOp {
            op: BinaryOperator::Plus,
            left_expr: v2_expr.into(),
            right_expr: BoundExpr::Constant(DataValue::Int32(1)).into(),
            return_type: Some(DataTypeKind::Int(None).not_null()),
        });
        let v2_plus_1_plus_sum_expr = BoundExpr::BinaryOp(BoundBinaryOp {
            op: BinaryOperator::Plus,
            left_expr: v2_plus_1_expr.clone().into(),
            right_expr: sum_v1_call.into(),
            return_type: Some(DataTypeKind::Int(None).not_null()),
        });
        let group_keys = vec![v2_plus_1_expr];
        let mut select_list = vec![v2_plus_1_plus_sum_expr];

        let mut resolver = AggInputRefResolver::new(group_keys.len());
        for expr in &mut select_list {
            resolver.resolve_select_expr(expr, &group_keys);
        }

        assert_eq!(
            select_list[0],
            BoundExpr::BinaryOp(BoundBinaryOp {
                op: BinaryOperator::Plus,
                left_expr: BoundExpr::InputRef(BoundInputRef {
                    index: 0,
                    return_type: DataTypeKind::Int(None).not_null(),
                })
                .into(),
                right_expr: BoundExpr::InputRef(BoundInputRef {
                    index: 1,
                    return_type: DataTypeKind::Int(None).not_null(),
                })
                .into(),
                return_type: Some(DataTypeKind::Int(None).not_null()),
            })
        );
    }
}
