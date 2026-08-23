{{ config(materialized='view') }}

SELECT
    "ticker", "Date", "gt_upper", "near_upper", "lt_lower", "near_lower"
FROM {{ source('raw', 'us_all_prices') }}
WHERE "period" = 'D'
  AND "Date" >= CURRENT_DATE - INTERVAL '2 years'
