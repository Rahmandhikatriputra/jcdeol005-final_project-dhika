{{
config(
        materialized='incremental',
        unique_key = ['age', 'transaction_monthly']
)
}}

WITH

fact_table AS (
        SELECT transaction_id,
                customer_id,
                movement,
                transaction_datetime
        FROM {{ref('model_fact_table')}}
),

dim_customer AS (
        SELECT customer_id,
        age
        FROM {{ref('model_dim_customer')}}
),

monthly_aggregates AS (
    SELECT DATE_TRUNC(fact_table.transaction_datetime, MONTH) AS transaction_monthly,
            COUNT(transaction_id) AS total_count_payment,
            dim_customer.age as age
    FROM fact_table
    INNER JOIN dim_customer ON fact_table.customer_id = dim_customer.customer_id
    WHERE fact_table.movement = 'debit'
    
        {% if is_incremental() %}
                AND fact_table.transaction_datetime > (SELECT MAX(transaction_monthly) FROM {{ this }})
        {% endif %}
    
    GROUP BY age, transaction_monthly
)

SELECT transaction_monthly,
        total_count_payment,
        age
FROM monthly_aggregates
-- GROUP BY age, transaction_monthly