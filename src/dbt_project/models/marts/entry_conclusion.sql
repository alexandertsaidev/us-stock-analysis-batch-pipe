{{ config(
    materialized='external',
    location='s3://us-stock/stock/history/prices/gold/conclusion/entry_conclusion.parquet',
    format='parquet'
) }}

WITH base AS (
    SELECT
        b."ticker",

        -- Date 全部週期
        b."Date"    AS "Date_D",
        w."Date"    AS "Date_W",
        w2."Date"   AS "Date_2W",
        w3."Date"   AS "Date_3W",
        me."Date"   AS "Date_ME",
        m2."Date"   AS "Date_2M",
        m3."Date"   AS "Date_3M",

        -- Side_1 全部週期
        b."Side_1"  AS "Side_1_D",
        w."Side_1"  AS "Side_1_W",
        w2."Side_1" AS "Side_1_2W",
        w3."Side_1" AS "Side_1_3W",
        me."Side_1" AS "Side_1_ME",
        m2."Side_1" AS "Side_1_2M",
        m3."Side_1" AS "Side_1_3M",

        -- Side_2 全部週期
        b."Side_2"  AS "Side_2_D",
        w."Side_2"  AS "Side_2_W",
        w2."Side_2" AS "Side_2_2W",
        w3."Side_2" AS "Side_2_3W",
        me."Side_2" AS "Side_2_ME",
        m2."Side_2" AS "Side_2_2M",
        m3."Side_2" AS "Side_2_3M",

        -- Side_3 全部週期
        b."Side_3"  AS "Side_3_D",
        w."Side_3"  AS "Side_3_W",
        w2."Side_3" AS "Side_3_2W",
        w3."Side_3" AS "Side_3_3W",
        me."Side_3" AS "Side_3_ME",
        m2."Side_3" AS "Side_3_2M",
        m3."Side_3" AS "Side_3_3M",

        -- 用來過濾最新日期
        MAX(b."Date") OVER (PARTITION BY b."ticker") AS "max_date"

    FROM {{ ref('stg_prices_D') }} b

    LEFT JOIN LATERAL (
        SELECT "Date", "Side_1", "Side_2", "Side_3"
        FROM {{ ref('stg_prices_W') }}
        WHERE "ticker" = b."ticker"
        AND "Date" <= b."Date" + INTERVAL '8 days'
        ORDER BY "Date" DESC
        LIMIT 1
    ) w ON TRUE

    LEFT JOIN LATERAL (
        SELECT "Date", "Side_1", "Side_2", "Side_3"
        FROM {{ ref('stg_prices_2W') }}
        WHERE "ticker" = b."ticker"
        AND "Date" <= b."Date" + INTERVAL '15 days'
        ORDER BY "Date" DESC
        LIMIT 1
    ) w2 ON TRUE

    LEFT JOIN LATERAL (
        SELECT "Date", "Side_1", "Side_2", "Side_3"
        FROM {{ ref('stg_prices_3W') }}
        WHERE "ticker" = b."ticker"
        AND "Date" <= b."Date" + INTERVAL '22 days'
        ORDER BY "Date" DESC
        LIMIT 1
    ) w3 ON TRUE

    LEFT JOIN LATERAL (
        SELECT "Date", "Side_1", "Side_2", "Side_3"
        FROM {{ ref('stg_prices_ME') }}
        WHERE "ticker" = b."ticker"
        AND "Date" <= b."Date" + INTERVAL '32 days'
        ORDER BY "Date" DESC
        LIMIT 1
    ) me ON TRUE

    LEFT JOIN LATERAL (
        SELECT "Date", "Side_1", "Side_2", "Side_3"
        FROM {{ ref('stg_prices_2ME') }}
        WHERE "ticker" = b."ticker"
        AND "Date" <= b."Date" + INTERVAL '63 days'
        ORDER BY "Date" DESC
        LIMIT 1
    ) m2 ON TRUE

    LEFT JOIN LATERAL (
        SELECT "Date", "Side_1", "Side_2", "Side_3"
        FROM {{ ref('stg_prices_3ME') }}
        WHERE "ticker" = b."ticker"
        AND "Date" <= b."Date" + INTERVAL '93 days'
        ORDER BY "Date" DESC
        LIMIT 1
    ) m3 ON TRUE
)

-- 移除每個 ticker 最新那一列
SELECT * EXCLUDE ("max_date")
FROM base
WHERE "Date_D" < "max_date"
