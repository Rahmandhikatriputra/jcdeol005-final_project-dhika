{{
config(
        materialized='incremental',
        unique_key = ['txn_description', 'transaction_daily']

)
}}

WITH

daily_aggregates AS (
    SELECT DATE_TRUNC(transaction_datetime, DAY) AS transaction_daily,
            COUNT(transaction_id) AS total_count_transaction,
            txn_description
    FROM {{ref('model_fact_table')}}
    WHERE movement = 'debit'

        {% if is_incremental() %}
                AND transaction_datetime > (SELECT MAX(transaction_daily) FROM {{ this }})
        {% endif %}

    GROUP BY txn_description, transaction_daily
)

SELECT transaction_daily,
        total_count_transaction,
        txn_description
FROM daily_aggregates
-- GROUP BY txn_description, transaction_daily