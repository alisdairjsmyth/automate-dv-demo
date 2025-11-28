{%- set source_model = "v_stg_orders" -%}
{%- set src_pk = "lineitem_pk" -%}
{%- set src_hashdiff = "lineitem_hashdiff" -%}
{%- set src_payload = [
            "commitdate",
            "discount",
            "extendedprice",
            "line_comment",
            "quantity",
            "receiptdate",
            "returnflag",
            "shipdate",
            "shipinstruct",
            "shipmode",
            "tax"] -%}
{%- set src_eff = "effective_from" -%}
{%- set src_ldts = "load_date" -%}
{%- set src_source = "record_source" -%}

{{ automate_dv.sat(
        src_pk=src_pk,
        src_hashdiff=src_hashdiff,
        src_payload=src_payload,
        src_eff=src_eff,
        src_ldts=src_ldts,
        src_source=src_source,
        source_model=source_model) }}
