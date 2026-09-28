-- Implements specs/sales/oms_orders.spec.yaml -> GOLD_SALES.DAILY_ORDER_REVENUE
SELECT
    TO_DATE(ORDER_TS)                                                          AS ORDER_DATE,
    REGION_CODE,
    CURRENCY,
    COUNT_IF(ORDER_STATUS <> 'CANCELLED')                                      AS ORDER_COUNT,
    SUM(IFF(ORDER_STATUS NOT IN ('CANCELLED', 'RETURNED'), ORDER_TOTAL, 0))    AS GROSS_REVENUE,
    DIV0(
        SUM(IFF(ORDER_STATUS NOT IN ('CANCELLED', 'RETURNED'), ORDER_TOTAL, 0)),
        COUNT_IF(ORDER_STATUS <> 'CANCELLED')
    )                                                                          AS AVG_ORDER_VALUE
FROM {{ params.silver_orders }}
WHERE TO_DATE(ORDER_TS) >= '{{ data_interval_start | ds }}'
  AND TO_DATE(ORDER_TS) <  '{{ data_interval_end | ds }}'
GROUP BY 1, 2, 3
