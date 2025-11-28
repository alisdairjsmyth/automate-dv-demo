SELECT
    a.l_orderkey AS orderkey,
    a.l_partkey AS partkey,
    a.l_suppkey AS supplierkey,
    a.l_linenumber AS linenumber,
    a.l_quantity AS quantity,
    a.l_extendedprice AS extendedprice,
    a.l_discount AS discount,
    a.l_tax AS tax,
    a.l_returnflag AS returnflag,
    a.l_linestatus AS linestatus,
    a.l_shipdate AS shipdate,
    a.l_commitdate AS commitdate,
    a.l_receiptdate AS receiptdate,
    a.l_shipinstruct AS shipinstruct,
    a.l_shipmode AS shipmode,
    a.l_comment AS line_comment,
    b.o_custkey AS customerkey,
    b.o_orderstatus AS orderstatus,
    b.o_totalprice AS totalprice,
    b.o_orderdate AS orderdate,
    b.o_orderpriority AS orderpriority,
    b.o_clerk AS clerk,
    b.o_shippriority AS shippriority,
    b.o_comment AS order_comment,
    c.c_name AS customer_name,
    c.c_address AS customer_address,
    c.c_nationkey AS customer_nation_key,
    c.c_phone AS customer_phone,
    c.c_acctbal AS customer_accbal,
    c.c_mktsegment AS customer_mktsegment,
    c.c_comment AS customer_comment,
    d.n_name AS customer_nation_name,
    d.n_regionkey AS customer_region_key,
    d.n_comment AS customer_nation_comment,
    e.r_name AS customer_region_name,
    e.r_comment AS customer_region_comment
FROM {{ source('tpch_sample', 'orders') }} AS b
LEFT JOIN {{ source('tpch_sample', 'lineitem') }} AS a
    ON b.o_orderkey = a.l_orderkey
LEFT JOIN {{ source('tpch_sample', 'customer') }} AS c
    ON b.o_custkey = c.c_custkey
LEFT JOIN {{ source('tpch_sample', 'nation') }} AS d
    ON c.c_nationkey = d.n_nationkey
LEFT JOIN {{ source('tpch_sample', 'region') }} AS e
    ON d.n_regionkey = e.r_regionkey
LEFT JOIN {{ source('tpch_sample', 'part') }} AS g
    ON a.l_partkey = g.p_partkey
LEFT JOIN {{ source('tpch_sample', 'supplier') }} AS h
    ON a.l_suppkey = h.s_suppkey
LEFT JOIN {{ source('tpch_sample', 'nation') }} AS j
    ON h.s_nationkey = j.n_nationkey
LEFT JOIN {{ source('tpch_sample', 'region') }} AS k
    ON j.n_regionkey = k.r_regionkey
WHERE b.o_orderdate = TO_DATE('{{ var('load_date') }}')
