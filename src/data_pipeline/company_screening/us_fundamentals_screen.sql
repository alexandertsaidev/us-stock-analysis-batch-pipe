SELECT *
FROM (
    SELECT "created_at","ticker",
        CASE
            WHEN( ("市值" >= 1000000000)
                AND ("市銷率P/S" >= 2 AND "市銷率P/S" <= 10)
                AND ("ROE" > 0.05 AND "ROE" <= 0.25)
                AND ("流動比率" >= 2.5)
                AND ("產權比率/負債權益比" <= 10)
                AND ("ROA(TTM)" > 0)
                AND ("EPS(TTM)" > 0)
                AND ("經營現金流" > 0)
                AND ("機構持股比例" >= 0.4 AND "機構持股比例" < 0.95)
                AND ("分析師平均評級/analystRatingMean" <= 3))
                THEN 'HighGrowth'
            WHEN( ("市值" >= 400000000)
                AND ("市銷率P/S" >= 2 AND "市銷率P/S" <= 7)
                AND ("ROE" > 0.15 AND "ROE" <= 0.25)
                AND ("流動比率" >= 2)
                AND ("產權比率/負債權益比" <= 10)
                AND ("ROA(TTM)" > 0)
                AND ("EPS(TTM)" > 0)
                AND ("經營現金流" > 0)
                AND ("目前價格" >= 15)
                AND ("年銷售收入" >= 50000000)
                AND ("機構持股比例" >= 0.4 AND "機構持股比例" < 0.95)
                AND ("分析師平均評級/analystRatingMean" <= 2))
                THEN 'SteadyGrowth'
            WHEN( ("市銷率P/S" <= 1.5)
                AND ("ROE" > 0.15 AND "ROE" <= 0.25)
                AND ("流動比率" >= 1.1)
                AND ("產權比率/負債權益比" <= 50)
                AND ("ROA(TTM)" > 0)
                AND ("EPS(TTM)" > 0)
                AND ("目前價格" > 0)
                AND ("年銷售收入" >= 50000000)
                AND ("上一季日均成交/avgDailyVolumeLastQuarter" >= 50000)
                AND ("機構持股比例" >= 0.4 AND "機構持股比例" < 0.95)
                AND ("分析師平均評級/analystRatingMean" >= 3))
                THEN 'Distressed'
            WHEN( ("52週價格變化/priceChange52W" >= 3)
                AND ("52週新高率" >= 0.9)
                -- AND ("ROA(TTM)" > 0)
                -- AND ("EPS(TTM)" > 0)
                AND ("目前價格" >= 15)
                AND ("上一季日均成交/avgDailyVolumeLastQuarter" >= 50000)
                AND ("機構持股比例" >= 0.4 AND "機構持股比例" < 0.95)
                AND ("分析師平均評級/analystRatingMean" <= 2)
                AND ("分析師建議/analystRatingKey" IS NOT NULL))
                THEN 'Super'
            WHEN( ("ticker" IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'NFLX'))
                )
                THEN 'Magnificent_8'
            WHEN( ("ticker" IN ('QQQ', 'SPY', 'DIA'))
                )
                THEN 'ETF_1'
            -- ELSE 'no_selected'
        END AS is_selected
    FROM "us_co_screen"
) sub
WHERE is_selected IS NOT NULL;