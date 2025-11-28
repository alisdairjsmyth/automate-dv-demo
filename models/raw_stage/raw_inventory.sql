SELECT
    a.ps_partkey AS partkey,
    a.ps_suppkey AS supplierkey,
    a.ps_availqty AS availqty,
    a.ps_supplycost AS supplycost,
    a.ps_comment AS part_supply_comment,
    b.s_name AS supplier_name,
    b.s_address AS supplier_address,
    b.s_nationkey AS supplier_nation_key,
    b.s_phone AS supplier_phone,
    b.s_acctbal AS supplier_acctbal,
    b.s_comment AS supplier_comment,
    c.p_name AS part_name,
    c.p_mfgr AS part_mfgr,
    c.p_brand AS part_brand,
    c.p_type AS part_type,
    c.p_size AS part_size,
    c.p_container AS part_container,
    c.p_retailprice AS part_retailprice,
    c.p_comment AS part_comment,
    d.n_name AS supplier_nation_name,
    d.n_comment AS supplier_nation_comment,
    d.n_regionkey AS supplier_region_key,
    e.r_name AS supplier_region_name,
    e.r_comment AS supplier_region_comment
FROM {{ source('tpch_sample', 'partsupp') }} AS a
LEFT JOIN {{ source('tpch_sample', 'supplier') }} AS b
    ON a.ps_suppkey = b.s_suppkey
LEFT JOIN {{ source('tpch_sample', 'part') }} AS c
    ON a.ps_partkey = c.p_partkey
LEFT JOIN {{ source('tpch_sample', 'nation') }} AS d
    ON b.s_nationkey = d.n_nationkey
LEFT JOIN {{ source('tpch_sample', 'region') }} AS e
    ON d.n_regionkey = e.r_regionkey
INNER JOIN {{ ref('raw_orders') }} AS f
    ON a.ps_partkey = f.partkey AND a.ps_suppkey = f.supplierkey
ORDER BY
    partkey,
    supplierkey
