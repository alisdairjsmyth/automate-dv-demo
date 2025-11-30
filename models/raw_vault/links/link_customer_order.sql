{%- set source_model = "v_stg_orders" -%}
{%- set src_pk = "order_customer_pk" -%}
{%- set src_fk = ["customer_pk", "order_pk"] -%}
{%- set src_ldts = "load_date" -%}
{%- set src_source = "record_source" -%}

{{
    automate_dv.link(
        src_pk=src_pk,
        src_fk=src_fk,
        src_ldts=src_ldts,
        src_source=src_source,
        source_model=source_model,
    )
}}
