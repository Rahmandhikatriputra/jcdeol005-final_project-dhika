{{
config(
        materialized='incremental',
        unique_key = 'transaction_id'
)
}}

WITH

fact_table AS (
        SELECT transaction_id,
                customer_id,
                merchant_id,
                transaction_datetime
        FROM {{ref('model_fact_table')}}
),

dim_customer AS (
        SELECT customer_id,
        customer_long_lat,
        FROM {{ref('model_dim_customer')}}
),

dim_merchant AS (
        SELECT merchant_id,
        merchant_long_lat
        FROM {{ref('model_dim_merchant')}}
)


SELECT fact_table.transaction_id, 
        fact_table.transaction_datetime,
        dim_customer.customer_long_lat, 
        dim_merchant.merchant_long_lat
FROM fact_table
INNER JOIN dim_customer ON fact_table.customer_id = dim_customer.customer_id
INNER JOIN dim_merchant ON fact_table.merchant_id = dim_merchant.merchant_id