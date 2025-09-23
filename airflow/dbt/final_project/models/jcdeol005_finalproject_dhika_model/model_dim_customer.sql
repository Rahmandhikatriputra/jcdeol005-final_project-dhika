{{
config(
        materialized='incremental',
        incremental_strategy = 'merge',
        unique_key='customer_id' 
)
}}

WITH 

stg_data AS (
        SELECT *,
                ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY transaction_date DESC) AS row_num
FROM {{ref('stg_anz_dataset')}}
)

SELECT customer_id,
        first_name,
        account, 
        gender, 
        age, 
        customer_long_lat
FROM stg_data
WHERE row_num = 1

-- {% if is_incremental() %}

--     WHERE customer_id NOT IN (SELECT customer_id FROM {{ this }})

-- {% endif %}