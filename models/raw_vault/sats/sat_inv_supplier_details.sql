{%- set source_model = "v_stg_inventory" -%}
{%- set src_pk       = "supplier_pk" -%}
{%- set src_hashdiff = "supplier_hashdiff" -%}
{%- set src_payload  = [
            "supplier_address",
            "supplier_phone",
            "supplier_acctbal",
            "supplier_name",
            "supplier_comment"] -%}
{%- set src_eff      = "effective_from" -%}
{%- set src_ldts     = "load_date" -%}
{%- set src_source   = "record_source" -%}

{{ automate_dv.sat(
        src_pk=src_pk,
        src_hashdiff=src_hashdiff,
        src_payload=src_payload,
        src_eff=src_eff,
        src_ldts=src_ldts,
        src_source=src_source,
        source_model=source_model) }}
