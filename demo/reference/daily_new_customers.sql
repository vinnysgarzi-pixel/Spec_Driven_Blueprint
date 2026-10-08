-- Implements specs/sales/oms_customers.spec.yaml -> GOLD_SALES.DAILY_NEW_CUSTOMERS
SELECT
    TO_DATE(SIGNUP_TS)                  AS SIGNUP_DATE,
    REGION_CODE,
    SEGMENT,
    COUNT(*)                            AS NEW_CUSTOMERS,
    COUNT_IF(STATUS = 'ACTIVE')         AS ACTIVE_CUSTOMERS
FROM {{ params.silver_customers }}
WHERE TO_DATE(SIGNUP_TS) >= '{{ data_interval_start | ds }}'
  AND TO_DATE(SIGNUP_TS) <  '{{ data_interval_end | ds }}'
GROUP BY 1, 2, 3
