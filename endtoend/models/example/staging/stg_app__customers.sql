{{ config(
    materialized='view',
    tags=['stg_app_customers', 'staging']
) }}

with initial_stg_app_customers AS (
    SELECT * FROM {{ source('customers', 'customers') }}
),

cleaned_stg_app_customers AS (
    SELECT
        ID,
        {{string_utils('name')}} AS name,
        COALESCE(TRY_TO_DATE(signup_date), '1900-01-01'::DATE) AS signup_date,
        region
    FROM initial_stg_app_customers
)


SELECT *
FROM cleaned_stg_app_customers
WHERE signup_date IS NOT NULL
