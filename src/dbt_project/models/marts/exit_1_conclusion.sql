{{ 
    config(
        materialized='external',
        location='s3://us-stock/stock/history/prices/gold/conclusion/exit_1_conclusion.parquet',
        format='parquet'
    )
}}

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

        -- D 週期
        b."gt_upper"   AS "gt_upper_D",
        b."near_upper" AS "near_upper_D",
        b."lt_lower"   AS "lt_lower_D",
        b."near_lower" AS "near_lower_D",

        -- W 週期
        w."gt_upper"   AS "gt_upper_W",
        w."near_upper" AS "near_upper_W",
        w."lt_lower"   AS "lt_lower_W",
        w."near_lower" AS "near_lower_W",

        -- 2W 週期
        w2."gt_upper"   AS "gt_upper_2W",
        w2."near_upper" AS "near_upper_2W",
        w2."lt_lower"   AS "lt_lower_2W",
        w2."near_lower" AS "near_lower_2W",

        -- 3W 週期
        w3."gt_upper"   AS "gt_upper_3W",
        w3."near_upper" AS "near_upper_3W",
        w3."lt_lower"   AS "lt_lower_3W",
        w3."near_lower" AS "near_lower_3W",

        -- ME 週期
        me."gt_upper"   AS "gt_upper_ME",
        me."near_upper" AS "near_upper_ME",
        me."lt_lower"   AS "lt_lower_ME",
        me."near_lower" AS "near_lower_ME",

        -- 2M 週期
        m2."gt_upper"   AS "gt_upper_2M",
        m2."near_upper" AS "near_upper_2M",
        m2."lt_lower"   AS "lt_lower_2M",
        m2."near_lower" AS "near_lower_2M",

        -- 3M 週期
        m3."gt_upper"   AS "gt_upper_3M",
        m3."near_upper" AS "near_upper_3M",
        m3."lt_lower"   AS "lt_lower_3M",
        m3."near_lower" AS "near_lower_3M",

    FROM {{ ref('stg_band_D') }} b

    LEFT JOIN LATERAL (
        SELECT "Date", "gt_upper", "near_upper", "lt_lower", "near_lower"
        FROM {{ ref('stg_band_W') }}
        WHERE "ticker" = b."ticker"
        AND "Date" <= b."Date" + INTERVAL '8 days'
        ORDER BY "Date" DESC
        LIMIT 1
    ) w ON TRUE

    LEFT JOIN LATERAL (
        SELECT "Date", "gt_upper", "near_upper", "lt_lower", "near_lower"
        FROM {{ ref('stg_band_2W') }}
        WHERE "ticker" = b."ticker"
        AND "Date" <= b."Date" + INTERVAL '15 days'
        ORDER BY "Date" DESC
        LIMIT 1
    ) w2 ON TRUE

    LEFT JOIN LATERAL (
        SELECT "Date", "gt_upper", "near_upper", "lt_lower", "near_lower"
        FROM {{ ref('stg_band_3W') }}
        WHERE "ticker" = b."ticker"
        AND "Date" <= b."Date" + INTERVAL '22 days'
        ORDER BY "Date" DESC
        LIMIT 1
    ) w3 ON TRUE

    LEFT JOIN LATERAL (
        SELECT "Date", "gt_upper", "near_upper", "lt_lower", "near_lower"
        FROM {{ ref('stg_band_ME') }}
        WHERE "ticker" = b."ticker"
        AND "Date" <= b."Date" + INTERVAL '32 days'
        ORDER BY "Date" DESC
        LIMIT 1
    ) me ON TRUE

    LEFT JOIN LATERAL (
        SELECT "Date", "gt_upper", "near_upper", "lt_lower", "near_lower"
        FROM {{ ref('stg_band_2ME') }}
        WHERE "ticker" = b."ticker"
        AND "Date" <= b."Date" + INTERVAL '63 days'
        ORDER BY "Date" DESC
        LIMIT 1
    ) m2 ON TRUE

    LEFT JOIN LATERAL (
        SELECT "Date", "gt_upper", "near_upper", "lt_lower", "near_lower"
        FROM {{ ref('stg_band_3ME') }}
        WHERE "ticker" = b."ticker"
        AND "Date" <= b."Date" + INTERVAL '93 days'
        ORDER BY "Date" DESC
        LIMIT 1
    ) m3 ON TRUE
)

SELECT * FROM base