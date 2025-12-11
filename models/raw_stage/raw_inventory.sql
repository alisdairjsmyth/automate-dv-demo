select
    a.ps_partkey as partkey,
    a.ps_suppkey as supplierkey,
    a.ps_availqty as availqty,
    a.ps_supplycost as supplycost,
    a.ps_comment as part_supply_comment,
    b.s_name as supplier_name,
    b.s_address as supplier_address,
    b.s_nationkey as supplier_nation_key,
    b.s_phone as supplier_phone,
    b.s_acctbal as supplier_acctbal,
    b.s_comment as supplier_comment,
    c.p_name as part_name,
    c.p_mfgr as part_mfgr,
    c.p_brand as part_brand,
    c.p_type as part_type,
    c.p_size as part_size,
    c.p_container as part_container,
    c.p_retailprice as part_retailprice,
    c.p_comment as part_comment,
    d.n_name as supplier_nation_name,
    d.n_comment as supplier_nation_comment,
    d.n_regionkey as supplier_region_key,
    e.r_name as supplier_region_name,
    e.r_comment as supplier_region_comment
from {{ source("tpch_sample", "partsupp") }} as a
left join {{ source("tpch_sample", "supplier") }} as b on a.ps_suppkey = b.s_suppkey
left join {{ source("tpch_sample", "part") }} as c on a.ps_partkey = c.p_partkey
left join {{ source("tpch_sample", "nation") }} as d on b.s_nationkey = d.n_nationkey
left join {{ source("tpch_sample", "region") }} as e on d.n_regionkey = e.r_regionkey
inner join
    {{ ref("raw_orders") }} as f
    on a.ps_partkey = f.partkey
    and a.ps_suppkey = f.supplierkey
