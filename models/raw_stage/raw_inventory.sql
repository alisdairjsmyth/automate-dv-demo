select
    partsupp.ps_partkey as partkey,
    partsupp.ps_suppkey as supplierkey,
    partsupp.ps_availqty as availqty,
    partsupp.ps_supplycost as supplycost,
    partsupp.ps_comment as part_supply_comment,
    supplier.s_name as supplier_name,
    supplier.s_address as supplier_address,
    supplier.s_nationkey as supplier_nation_key,
    supplier.s_phone as supplier_phone,
    supplier.s_acctbal as supplier_acctbal,
    supplier.s_comment as supplier_comment,
    part.p_name as part_name,
    part.p_mfgr as part_mfgr,
    part.p_brand as part_brand,
    part.p_type as part_type,
    part.p_size as part_size,
    part.p_container as part_container,
    part.p_retailprice as part_retailprice,
    part.p_comment as part_comment,
    nation.n_name as supplier_nation_name,
    nation.n_comment as supplier_nation_comment,
    nation.n_regionkey as supplier_region_key,
    region.r_name as supplier_region_name,
    region.r_comment as supplier_region_comment,
from {{ source("tpch_sample", "partsupp") }} as partsupp
left join
    {{ source("tpch_sample", "supplier") }} as supplier
    on partsupp.ps_suppkey = supplier.s_suppkey
left join
    {{ source("tpch_sample", "part") }} as part on partsupp.ps_partkey = part.p_partkey
left join
    {{ source("tpch_sample", "nation") }} as nation
    on supplier.s_nationkey = nation.n_nationkey
left join
    {{ source("tpch_sample", "region") }} as region
    on nation.n_regionkey = region.r_regionkey
inner join
    {{ ref("raw_orders") }} as raw_orders
    on partsupp.ps_partkey = raw_orders.partkey
    and partsupp.ps_suppkey = raw_orders.supplierkey
