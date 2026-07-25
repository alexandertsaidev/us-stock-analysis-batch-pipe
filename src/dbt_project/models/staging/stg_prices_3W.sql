{{ config(materialized='view') }}

SELECT "ticker", "Date", "Side_1", "Side_2", "Side_3"
FROM {{ source('raw', 'us_all_prices') }}
WHERE "period" = '3W'
