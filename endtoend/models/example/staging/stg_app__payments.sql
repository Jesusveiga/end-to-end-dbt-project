{{ config(
    materialized='table',
    tags=['stg_app_payments', 'staging']
) }}

WITH initial_stg_app_payments AS (
    SELECT * 
    FROM {{ source('payments', 'payments') }}
),

cleaned_stg_app_payments AS (
    SELECT
        payment_id,
        order_id,
        amount_cents/100.0 AS amount_usd,
        status,
        created_at
    FROM initial_stg_app_payments
    QUALIFY ROW_NUMBER() OVER(PARTITION BY payment_id ORDER BY created_at DESC) = 1
)

SELECT * 
FROM cleaned_stg_app_payments
