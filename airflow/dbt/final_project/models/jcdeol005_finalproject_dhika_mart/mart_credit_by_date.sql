{{
config(
        materialized='incremental', 
        unique_key = 'transaction_daily'
)
}}

WITH daily_aggregates AS (
        SELECT DATE_TRUNC(transaction_datetime, DAY) AS transaction_daily,
                COUNT(transaction_id) AS total_transaction_by_credit
        FROM {{ref('model_fact_table')}}
        WHERE movement = 'credit'

        {% if is_incremental() %}
                AND transaction_datetime > (SELECT MAX(transaction_daily) FROM {{ this }})
        {% endif %}

        GROUP BY transaction_daily
)

SELECT transaction_daily,
        total_transaction_by_credit
FROM daily_aggregates

