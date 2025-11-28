{%- set yaml_metadata -%}
source_model: 'raw_inventory'
derived_columns:
  nation_key: 'supplier_nation_key'
  region_key: 'supplier_region_key'
  record_source: '!TPCH-INVENTORY'
hashed_columns:
  supplier_pk: 'supplierkey'
  supplier_nation_pk: 'supplier_nation_key'
  supplier_region_pk: 'supplier_region_key'
  region_pk: 'supplier_region_key'
  nation_pk: 'supplier_nation_key'
  nation_region_pk:
    - 'supplier_nation_key'
    - 'supplier_region_key'
  link_supplier_nation_pk:
    - 'supplierkey'
    - 'supplier_nation_key'
  part_pk: 'partkey'
  inventory_pk:
    - 'partkey'
    - 'supplierkey'
  supplier_hashdiff:
    is_hashdiff: true
    columns:
      - 'supplierkey'
      - 'supplier_acctbal'
      - 'supplier_address'
      - 'supplier_phone'
      - 'supplier_comment'
      - 'supplier_name'
  part_hashdiff:
    is_hashdiff: true
    columns:
      - 'partkey'
      - 'part_brand'
      - 'part_comment'
      - 'part_container'
      - 'part_mfgr'
      - 'part_name'
      - 'part_retailprice'
      - 'part_size'
      - 'part_type'
  supplier_region_hashdiff:
    is_hashdiff: true
    columns:
      - 'supplier_region_key'
      - 'supplier_region_comment'
      - 'supplier_region_name'
  supplier_nation_hashdiff:
    is_hashdiff: true
    columns:
      - 'supplier_nation_key'
      - 'supplier_nation_comment'
      - 'supplier_nation_name'
  inventory_hashdiff:
    is_hashdiff: true
    columns:
      - 'partkey'
      - 'supplierkey'
      - 'availqty'
      - 'supplycost'
      - 'part_supply_comment'
{%- endset -%}

{% set metadata_dict   = fromyaml(yaml_metadata) %}
{% set source_model    = metadata_dict['source_model'] %}
{% set derived_columns = metadata_dict['derived_columns'] %}
{% set hashed_columns  = metadata_dict['hashed_columns'] %}

WITH staging AS (
{{ automate_dv.stage(
      include_source_columns=true,
      source_model=source_model,
      derived_columns=derived_columns,
      hashed_columns=hashed_columns,
      ranked_columns=none) }}
)

SELECT
    *,
    TO_DATE('{{ var('load_date') }}') AS load_date,
    TO_DATE('{{ var('load_date') }}') AS effective_from
FROM staging
