SELECT
    b.o_orderkey AS order_id,
    b.o_custkey AS customer_id,
    b.o_orderdate AS order_date,
    b.o_totalprice AS amount,
    DATEADD(DAY, 20, b.o_orderdate) AS transaction_date,
    TO_NUMBER(
        RPAD(
            CONCAT(
                b.o_orderkey, b.o_custkey, TO_CHAR(b.o_orderdate, 'yyyyMMdd')
            ),
            24,
            '0'
        ),
        RPAD('0', 24, '0')
    ) AS transaction_number,
    CAST(
        CASE ABS(MOD(RANDOM(1), 2)) + 1
            WHEN 1 THEN 'DR'
            WHEN 2 THEN 'CR'
        END AS VARCHAR(2)
    ) AS transaction_type
FROM {{ source('tpch_sample', 'orders') }} AS b
LEFT JOIN {{ source('tpch_sample', 'customer') }} AS c
    ON b.o_custkey = c.c_custkey
WHERE b.o_orderdate = TO_DATE('{{ var('load_date') }}')
