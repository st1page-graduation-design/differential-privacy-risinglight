select
    l_returnflag,
    l_linestatus,
    dp_sum(l_quantity                                      , 0.5) as sum_qty,
    dp_sum(l_extendedprice                                 , 0.5) as sum_base_price,
    dp_sum(l_extendedprice * (1 - l_discount)              , 0.5) as sum_disc_price,
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
