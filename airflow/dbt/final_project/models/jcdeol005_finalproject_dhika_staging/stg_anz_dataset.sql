{{
config(
        materialized='incremental'
)
}}

SELECT status,
        CAST(card_present_flag AS INTEGER) AS card_present_flag,
        CAST(bpay_biller_code AS INTEGER) AS bpay_biller_code,
        account,
        currency,
        REPLACE(long_lat, ' ', ',') AS customer_long_lat,
        txn_description,
        merchant_id,
        CAST(merchant_code AS INTEGER) AS merchant_code,
        first_name,
        CAST(date AS DATE) AS transaction_date,
        gender,
        age,
        merchant_suburb,
        merchant_state,
        PARSE_DATETIME('%Y-%m-%dT%H:%M:%S', SUBSTR(extraction, 1, 19)) AS transaction_datetime,
        CAST(amount AS FLOAT64) AS amount,
        transaction_id,
        country,
        customer_id,
        REPLACE(merchant_long_lat, ' ', ',') AS merchant_long_lat,
        movement,
        CURRENT_DATETIME() AS load_date
FROM {{source('raw_data', 'raw_anz_dataset')}}