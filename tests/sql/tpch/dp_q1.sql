explain select
    l_returnflag,
    l_linestatus,
    dp_sum(l_quantity                                      , 0.5) as sum_qty,
    dp_sum(l_extendedprice * (1 - l_discount) * (1 + l_tax), 0.5) as sum_charge,
    dp_count(1                                             , 0.5) as count_order
from
    lineitem
where
    l_shipdate <= date '1998-12-01' - interval '71' day
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;

PhysicalOrder:
    [InputRef #0 (asc), InputRef #1 (asc)]
  PhysicalProjection:
      InputRef #0
      InputRef #1
      InputRef #2 (alias to sum_qty)
      InputRef #3 (alias to sum_charge)
      InputRef #4 (alias to count_order)
    PhysicalHashAgg:
        InputRef #1
        InputRef #2
        dp_sum(epsilon=0.5)(InputRef #3) -> FLOAT
        dp_sum(epsilon=0.5)(((InputRef #4 * (1 - InputRef #5)) * (1 + InputRef #6))) -> FLOAT
        dp_count(epsilon=0.5)(1) -> FLOAT
      PhysicalTableScan:
          table #9,
          columns [10, 8, 9, 4, 5, 6, 7],
          with_row_handler: false,
          is_sorted: false,
          expr: LtEq(InputRef #0, Date(Date(10490)) (const))