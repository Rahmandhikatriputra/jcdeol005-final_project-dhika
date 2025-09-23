{{
config(
        materialized='incremental',
        incremental_strategy = 'merge',
        unique_key='merchant_id' 
)
}}

WITH 

stg_data AS (
        SELECT *,
                ROW_NUMBER() OVER(PARTITION BY merchant_id ORDER BY transaction_date DESC) AS row_num
        FROM {{ref('stg_anz_dataset')}}
)

SELECT merchant_id, 
        merchant_code, 
        merchant_suburb, 
        merchant_state, 
        merchant_long_lat, 
        country
FROM stg_data
WHERE row_num = 1


-- {% if is_incremental() %}

--     WHERE merchant_id NOT IN (SELECT merchant_id FROM {{ this }})

-- {% endif %}