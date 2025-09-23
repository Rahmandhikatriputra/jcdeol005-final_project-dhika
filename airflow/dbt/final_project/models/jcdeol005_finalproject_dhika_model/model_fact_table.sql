{{
config(
        materialized='incremental'
)
}}

SELECT transaction_id, 
        status, 
        customer_id, 
        merchant_id,
        amount, 
        currency, 
        transaction_datetime, 
        card_present_flag, 
        bpay_biller_code, 
        txn_description, 
        movement,
        load_date
FROM {{ref('stg_anz_dataset')}}


{% if is_incremental() %}

    WHERE load_date > (SELECT MAX(load_date) FROM {{ this }})

{% endif %}