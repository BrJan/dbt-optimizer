{{ config(materialized='table') }}

-- Fact orders model with multiple issues for testing
WITH orders AS (
    SELECT DISTINCT
        order_id,
        customer_id,
        order_date,
        amount
    FROM {{ ref('stg_orders') }}
),

-- Direct table reference bypassing dbt (bad practice)
customers AS (
    SELECT customer_id, customer_name
    FROM analytics.public.customers  -- should use {{ ref() }}
),

order_summary AS (
    SELECT
        o.order_id,
        o.customer_id,
        c.customer_name,
        o.order_date,
        o.amount,
        SUM(o.amount) OVER (PARTITION BY o.customer_id) AS customer_total
    FROM orders o, customers c  -- implicit cross join!
    WHERE o.customer_id = c.customer_id
)

SELECT
    order_id,
    customer_id,
    customer_name,
    order_date,
    amount,
    customer_total
FROM order_summary
WHERE order_date >= '2020-01-01'
UNION
SELECT
    order_id,
    customer_id,
    customer_name,
    order_date,
    amount,
    customer_total
FROM order_summary
WHERE amount = 0
