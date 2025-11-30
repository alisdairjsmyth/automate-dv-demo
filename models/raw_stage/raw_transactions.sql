select
    b.o_orderkey as order_id,
    b.o_custkey as customer_id,
    b.o_orderdate as order_date,
    b.o_totalprice as amount,
    dateadd(day, 20, b.o_orderdate) as transaction_date,
    to_number(
        rpad(
            concat(b.o_orderkey, b.o_custkey, to_char(b.o_orderdate, 'yyyyMMdd')),
            24,
            '0'
        ),
        rpad('0', 24, '0')
    ) as transaction_number,
    case
        abs(mod(random(1), 2)) + 1 when 1 then 'DR' when 2 then 'CR'
    end as transaction_type
from {{ source("tpch_sample", "orders") }} as b
left join {{ source("tpch_sample", "customer") }} as c on b.o_custkey = c.c_custkey
where b.o_orderdate = to_date('{{ var("load_date") }}')
