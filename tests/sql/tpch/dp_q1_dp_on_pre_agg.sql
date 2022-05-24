select 
    l_returnflag,
    l_linestatus,
    sum(sum_qty), 
    sum(sum_base_price),
    sum(sum_disc_price),
    sum(sum_charge),
    sum(count_order),
from(
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        count(*) as count_order
    from
        lineitem,
        orders,
        customer
    where
        l_shipdate <= date '1998-12-01' - interval '71' day
        and c_custkey = o_custkey
        and l_orderkey = o_orderkey
    group by
        l_returnflag,
        l_linestatus,
        c_custkey
    ) pre_agg
group by 
    pre_agg.l_returnflag,
    pre_agg.l_linestatus;