-- Implements specs/finance/revenue_usd.spec.yaml -> GOLD_FINANCE.DAILY_REVENUE_USD
SELECT
    r.ORDER_DATE,
    r.REGION_CODE,
    -- NULL (fails the quality gate) if any currency that day lacks a rate, instead of undercounting.
    IFF(COUNT_IF(fx.USD_RATE IS NULL) > 0, NULL, SUM(r.GROSS_REVENUE * fx.USD_RATE)) AS GROSS_REVENUE_USD,
    SUM(r.ORDER_COUNT)                  AS ORDER_COUNT
FROM {{ params.daily_order_revenue }} r
LEFT JOIN {{ params.fx_daily_rates }} fx
       ON fx.RATE_DATE = r.ORDER_DATE
      AND fx.CURRENCY  = r.CURRENCY
WHERE r.ORDER_DATE >= DATEADD('day', -90, CURRENT_DATE())
GROUP BY 1, 2
