select
    l_returnflag,
    l_linestatus,
    1 - ( dp_sum(l_quantity                                      , 0.5) / sum(l_quantity) ) as sum_qty_d,
    1 - ( dp_sum(l_extendedprice * (1 - l_discount) * (1 + l_tax), 0.5) / sum(l_extendedprice * (1 - l_discount) * (1 + l_tax), 0.5)) as sum_charge,
    1 - ( dp_count(1                                             , 0.5) / count(1)) as count_order
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
