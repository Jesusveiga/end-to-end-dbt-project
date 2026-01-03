{{ config(
    materialized='incremental',
    incremental_strategy = "merge",
    unique_key = "event_id"
) }}

WITH initial_stg_app_orders AS (
    SELECT * 
    FROM {{ source('customers', 'orders') }}
),

cleaned_stg_app_orders AS (
    SELECT
        event_id,
        event_time,
        payload:"order_id"::string AS order_id,
        payload:"amount_total"::number AS amount_total,
        LOWER(payload:"status"::string) AS status,
        payload:"customer_info":"id"::string AS customer_identifier,
        payload:"user_id"::string AS user_id,
        COALESCE(payload:"customer_info":"id"::string, payload:"user_id"::string) AS customer_id
    FROM initial_stg_app_orders
),

final_stg_spp_app_orders AS (
    SELECT
        event_id,
        event_time,
        order_id,
        amount_total,
        status,
        customer_id
    FROM cleaned_stg_app_orders
)

SELECT * 
FROM final_stg_spp_app_orders
{% if is_incremental() %}
-- Incremental load: only process new records based on event_time
    WHERE event_time > (SELECT MAX(event_time) FROM {{ this }})
{% endif %}
order by event_time desc