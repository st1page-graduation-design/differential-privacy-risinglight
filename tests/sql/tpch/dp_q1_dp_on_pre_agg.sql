select 
    l_returnflag,
    l_linestatus,
    sum(sum_qty), 
    sum(sum_charge),
    sum(count_order)
from(
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
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

PhysicalHashAgg:                                                                                                                             
     InputRef #0                                                                                                                              
     InputRef #1                                                                                                                              
     dp_sum(epsilon=0.4)(InputRef #2) -> NUMERIC(15,2)                                                                                        
     dp_sum(epsilon=0.4)(InputRef #3) -> NUMERIC(15,2)                                                                                        
     dp_sum(epsilon=0.4)(InputRef #4) -> NUMERIC(15,2)                                                                                        
   PhysicalProjection:                                                                                                                        
       InputRef #0                                                                                                                            
       InputRef #1                                                                                                                            
       InputRef #3 (alias to sum_qty)                                                                                                         
       InputRef #4 (alias to sum_charge)                                                                                                      
       InputRef #5 (alias to count_order)                                                                                                     
     PhysicalHashAgg:                                                                                                                         
         InputRef #2                                                                                                                          
         InputRef #3                                                                                                                          
         InputRef #10                                                                                                                         
         sum(InputRef #4) -> NUMERIC(15,2)                                                                                                    
         sum(((InputRef #5 * (1 - InputRef #6)) * (1 + InputRef #7))) -> NUMERIC(15,2) (null)                                                 
         count(InputRef #0) -> INT                                                                                                            
       PhysicalHashJoin:                                                                                                                      
           op Inner,                                                                                                                          
           predicate: Eq(InputRef #8, InputRef #10)                                                                                           
         PhysicalHashJoin:                                                                                                                    
             op Inner,                                                                                                                        
             predicate: Eq(InputRef #1, InputRef #9)                                                                                          
           PhysicalTableScan:                                                                                                                 
               table #9,                                                                                                                      
               columns [10, 0, 8, 9, 4, 5, 6, 7],                                                                                             
               with_row_handler: false,                                                                                                       
               is_sorted: false,                                                                                                              
               expr: LtEq(InputRef #0, Date(Date(10490)) (const))                                                                             
           PhysicalTableScan:                                                                                                                 
               table #8,                                                                                                                      
               columns [1, 0],                                                                                                                
               with_row_handler: false,                                                                                                       
               is_sorted: false,                                                                                                              
               expr: None                                                                                                                     
         PhysicalTableScan:                                                                                                                   
             table #7,                                                                                                                        
             columns [0],                                                                                                                     
             with_row_handler: false,                                                                                                         
             is_sorted: false,                                                                                                                
             expr: None        