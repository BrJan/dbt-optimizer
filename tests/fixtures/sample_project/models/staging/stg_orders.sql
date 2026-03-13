-- Staging model with SELECT * and a subquery
SELECT *
FROM (
    SELECT
        order_id,
        customer_id,
        DATE(created_at) AS order_date,
        amount
    FROM {{ source('raw', 'orders') }}
    WHERE DATE(created_at) >= '2023-01-01'
) subq
WHERE amount > 0
