{%- set source_model = "v_stg_inventory" -%}
{%- set src_pk = "part_pk" -%}
{%- set src_hashdiff = "part_hashdiff" -%}
{%- set src_payload = [
    "part_name",
    "part_mfgr",
    "part_brand",
    "part_type",
    "part_size",
    "part_container",
    "part_retailprice",
    "part_comment",
] -%}
{%- set src_eff = "effective_from" -%}
{%- set src_ldts = "load_date" -%}
{%- set src_source = "record_source" -%}

{{
    automate_dv.sat(
        src_pk=src_pk,
        src_hashdiff=src_hashdiff,
        src_payload=src_payload,
        src_eff=src_eff,
        src_ldts=src_ldts,
        src_source=src_source,
        source_model=source_model,
    )
}}
