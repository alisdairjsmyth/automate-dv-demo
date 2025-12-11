select
    orders.o_orderkey as order_id,
    orders.o_custkey as customer_id,
    orders.o_orderdate as order_date,
    orders.o_totalprice as amount,
    dateadd(day, 20, orders.o_orderdate) as transaction_date,
    to_number(
        rpad(
            orders.o_orderkey
            || orders.o_custkey
            || to_char(orders.o_orderdate, 'yyyyMMdd'),
            24,
            '0'
        ),
        rpad('0', 24, '0')
    ) as transaction_number,
    case
        abs(mod(random(1), 2)) + 1 when 1 then 'DR' when 2 then 'CR'
    end as transaction_type,
from {{ source("tpch_sample", "orders") }} as orders
left join
    {{ source("tpch_sample", "customer") }} as customer
    on orders.o_custkey = customer.c_custkey
where orders.o_orderdate = to_date('{{ var("load_date") }}')
