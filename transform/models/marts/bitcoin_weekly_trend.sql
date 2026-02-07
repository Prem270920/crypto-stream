{{ config(materialized='table') }}

WITH raw_data AS (
    SELECT * FROM {{ source('crypto_sources', 'raw_bitcoin') }}
),

cleaned_data AS (
    SELECT
        CAST(timestamp AS DATE) as metric_date,
        price as close_price
    FROM raw_data
),

moving_average AS (
    SELECT
        metric_date,
        close_price,
        --- calculate average of the last 7 rows
        AVG(close_price) OVER (
            ORDER BY metric_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as moving_avg_7d
    FROM cleaned_data
)

SELECT * FROM moving_average
ORDER BY metric_date DESC