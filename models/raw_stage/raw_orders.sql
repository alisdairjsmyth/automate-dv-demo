select
    orders.l_orderkey as orderkey,
    orders.l_partkey as partkey,
    orders.l_suppkey as supplierkey,
    orders.l_linenumber as linenumber,
    orders.l_quantity as quantity,
    orders.l_extendedprice as extendedprice,
    orders.l_discount as discount,
    orders.l_tax as tax,
    orders.l_returnflag as returnflag,
    orders.l_linestatus as linestatus,
    orders.l_shipdate as shipdate,
    orders.l_commitdate as commitdate,
    orders.l_receiptdate as receiptdate,
    orders.l_shipinstruct as shipinstruct,
    orders.l_shipmode as shipmode,
    orders.l_comment as line_comment,
    lineitem.o_custkey as customerkey,
    lineitem.o_orderstatus as orderstatus,
    lineitem.o_totalprice as totalprice,
    lineitem.o_orderdate as orderdate,
    lineitem.o_orderpriority as orderpriority,
    lineitem.o_clerk as clerk,
    lineitem.o_shippriority as shippriority,
    lineitem.o_comment as order_comment,
    customer.c_name as customer_name,
    customer.c_address as customer_address,
    customer.c_nationkey as customer_nation_key,
    customer.c_phone as customer_phone,
    customer.c_acctbal as customer_accbal,
    customer.c_mktsegment as customer_mktsegment,
    customer.c_comment as customer_comment,
    customer_nation.n_name as customer_nation_name,
    customer_nation.n_regionkey as customer_region_key,
    customer_nation.n_comment as customer_nation_comment,
    customer_region.r_name as customer_region_name,
    customer_region.r_comment as customer_region_comment,
from {{ source("tpch_sample", "orders") }} as orders
left join
    {{ source("tpch_sample", "lineitem") }} as lineitem
    on orders.l_orderkey = lineitem.o_orderkey
left join
    {{ source("tpch_sample", "customer") }} as customer
    on lineitem.o_custkey = customer.c_custkey
left join
    {{ source("tpch_sample", "nation") }} as customer_nation
    on customer.c_nationkey = customer_nation.n_nationkey
left join
    {{ source("tpch_sample", "region") }} as customer_region
    on customer_nation.n_regionkey = customer_region.r_regionkey
left join
    {{ source("tpch_sample", "part") }} as part on orders.l_partkey = part.p_partkey
left join
    {{ source("tpch_sample", "supplier") }} as supplier
    on orders.l_suppkey = supplier.s_suppkey
left join
    {{ source("tpch_sample", "nation") }} as supplier_nation
    on supplier.s_nationkey = supplier_nation.n_nationkey
left join
    {{ source("tpch_sample", "region") }} as supplier_region
    on supplier_nation.n_regionkey = supplier_region.r_regionkey
where lineitem.o_orderdate = to_date('{{ var("load_date") }}')
