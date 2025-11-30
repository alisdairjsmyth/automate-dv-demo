{%- set source_model = "v_stg_transactions" -%}
{%- set src_pk = "transaction_pk" -%}
{%- set src_fk = ["customer_pk", "order_pk"] -%}
{%- set src_payload = [
    "transaction_number",
    "transaction_date",
    "transaction_type",
    "amount",
] -%}
{%- set src_eff = "effective_from" -%}
{%- set src_ldts = "load_date" -%}
{%- set src_source = "record_source" -%}

{{
    automate_dv.t_link(
        src_pk=src_pk,
        src_fk=src_fk,
        src_ldts=src_ldts,
        src_payload=src_payload,
        src_eff=src_eff,
        src_source=src_source,
        source_model=source_model,
    )
}}
