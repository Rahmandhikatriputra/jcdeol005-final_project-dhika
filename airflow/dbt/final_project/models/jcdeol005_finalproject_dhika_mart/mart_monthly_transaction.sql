{{
config(
        materialized='incremental',
        unique_key = 'transaction_monthly'
)
}}

WITH

monthly_aggregates AS (
    SELECT DATE_TRUNC(transaction_datetime, MONTH) AS transaction_monthly,
            COUNT(transaction_id) AS total_transaction
    FROM {{ref('model_fact_table')}}

            {% if is_incremental() %}
                WHERE transaction_datetime > (SELECT MAX(transaction_monthly) FROM {{ this }})
            {% endif %}

    GROUP BY transaction_monthly
)

SELECT transaction_monthly,
    total_transaction
FROM monthly_aggregates


-- SELECT COUNT(transaction_id) AS total_transaction,
--         FORMAT_DATETIME("%Y - %B", transaction_datetime) AS year_month
-- FROM {{ref('model_fact_table')}}
-- GROUP BY year_month