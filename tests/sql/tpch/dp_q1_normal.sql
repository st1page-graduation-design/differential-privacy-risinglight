select
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
