explain select
    l_returnflag,
    l_linestatus,
    sum(l_quantity                                      ) as sum_qty,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    count(1                                             ) as count_order
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
        sum(InputRef #3) -> NUMERIC(15,2)
        sum(((InputRef #4 * (1 - InputRef #5)) * (1 + InputRef #6))) -> NUMERIC(15,2) (null)
        count(1) -> INT
      PhysicalTableScan:
          table #9,
          columns [10, 8, 9, 4, 5, 6, 7],
          with_row_handler: false,
          is_sorted: false,
          expr: LtEq(InputRef #0, Date(Date(10490)) (const))