{%- set yaml_metadata -%}
source_model: 'raw_orders'
derived_columns:
  customer_key: 'customerkey'
  nation_key: 'customer_nation_key'
  region_key: 'customer_region_key'
  record_source: '!TPCH-ORDERS'
  effective_from: 'orderdate'
hashed_columns:
  customer_pk: 'customer_key'
  link_customer_nation_pk:
    - 'customer_key'
    - 'customer_nation_key'
  customer_nation_pk: 'customer_nation_key'
  customer_region_pk: 'customer_region_key'
  nation_pk: 'customer_nation_key'
  region_pk: 'customer_region_key'
  nation_region_pk:
    - 'customer_nation_key'
    - 'customer_region_key'
  order_pk: 'orderkey'
  order_customer_pk:
    - 'customer_key'
    - 'orderkey'
  lineitem_pk:
    - 'orderkey'
    - 'linenumber'
  link_lineitem_order_pk:
    - 'orderkey'
    - 'orderkey'
    - 'linenumber'
  part_pk: 'partkey'
  supplier_pk: 'supplierkey'
  inventory_pk:
    - 'partkey'
    - 'supplierkey'
  inventory_allocation_pk:
    - 'linenumber'
    - 'partkey'
    - 'supplierkey'
  customer_hashdiff:
    is_hashdiff: true
    columns:
      - 'customer_key'
      - 'customer_name'
      - 'customer_address'
      - 'customer_phone'
      - 'customer_accbal'
      - 'customer_mktsegment'
      - 'customer_comment'
  customer_nation_hashdiff:
    is_hashdiff: true
    columns:
      - 'customer_nation_name'
      - 'customer_nation_comment'
      - 'customer_nation_key'
  customer_region_hashdiff:
    is_hashdiff: true
    columns:
      - 'customer_region_name'
      - 'customer_region_comment'
      - 'customer_region_key'
  lineitem_hashdiff:
    is_hashdiff: true
    columns:
      - 'orderkey'
      - 'linenumber'
      - 'commitdate'
      - 'discount'
      - 'extendedprice'
      - 'linestatus'
      - 'line_comment'
      - 'quantity'
      - 'receiptdate'
      - 'returnflag'
      - 'shipdate'
      - 'shipinstruct'
      - 'shipmode'
      - 'tax'
  order_hashdiff:
    is_hashdiff: true
    columns:
      - 'orderkey'
      - 'clerk'
      - 'orderdate'
      - 'orderpriority'
      - 'orderstatus'
      - 'order_comment'
      - 'shippriority'
      - 'totalprice'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}
{% set source_model = metadata_dict["source_model"] %}
{% set derived_columns = metadata_dict["derived_columns"] %}
{% set hashed_columns = metadata_dict["hashed_columns"] %}

with
    staging as (
        {{
            automate_dv.stage(
                include_source_columns=true,
                source_model=source_model,
                derived_columns=derived_columns,
                hashed_columns=hashed_columns,
                ranked_columns=none,
            )
        }}
    )

select *, to_date('{{ var("load_date") }}') as load_date
from staging
