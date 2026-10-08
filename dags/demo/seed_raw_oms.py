"""Demo-only: seed GoldenGate-style raw data so the ADW pipelines can run end to end.

Creates the ADW schemas in the dev database, (re)generates RAW_OMS.ORDERS and
RAW_OMS.CUSTOMERS as GoldenGate audit logs (I/U/D rows with OP_TYPE / OP_TS / POS)
spanning the last three days up to "now", loads FX rates, and drops silver/gold
so the next pipeline runs start clean. Trigger it manually, then trigger
sales__oms_orders (and sales__oms_customers once it exists).

Never defined under the prod profile, because it drops tables.
"""

import os
from pathlib import Path

import yaml
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag

_VARS = yaml.safe_load((Path(__file__).parents[1] / "blueprint.vars.yaml").read_text())["vars"]
ENV = os.getenv("ADW_ENV", "dev")
DB = _VARS["adw_db"][ENV]
CONN_ID = _VARS["snowflake_conn_id"]

SEED_SQL = [
    *(f"CREATE SCHEMA IF NOT EXISTS {DB}.{s}" for s in ("RAW_OMS", "REF_FINANCE", "SILVER_OMS", "GOLD_SALES", "GOLD_FINANCE")),
    f"DROP TABLE IF EXISTS {DB}.GOLD_FINANCE.DAILY_REVENUE_USD",
    f"DROP TABLE IF EXISTS {DB}.GOLD_SALES.DAILY_ORDER_REVENUE",
    f"DROP TABLE IF EXISTS {DB}.SILVER_OMS.ORDERS",
    f"DROP TABLE IF EXISTS {DB}.GOLD_SALES.DAILY_NEW_CUSTOMERS",
    f"DROP TABLE IF EXISTS {DB}.SILVER_OMS.CUSTOMERS",
    f"""
CREATE OR REPLACE TABLE {DB}.RAW_OMS.ORDERS (
    ORDER_ID NUMBER, CUSTOMER_ID NUMBER, ORDER_STATUS VARCHAR, ORDER_TS TIMESTAMP_NTZ,
    CURRENCY VARCHAR(3), ORDER_TOTAL NUMBER(12, 2), REGION_CODE VARCHAR,
    OP_TYPE VARCHAR(1), OP_TS TIMESTAMP_NTZ, POS VARCHAR
)""",
    f"""
INSERT INTO {DB}.RAW_OMS.ORDERS
    (ORDER_ID, CUSTOMER_ID, ORDER_STATUS, ORDER_TS, CURRENCY, ORDER_TOTAL, REGION_CODE, OP_TYPE, OP_TS, POS)
WITH now_utc AS (
    SELECT CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ AS TS
),
base AS (
    SELECT
        SEQ4() + 1                                                                 AS ORDER_ID,
        UNIFORM(1000, 1999, RANDOM(11))                                            AS CUSTOMER_ID,
        DATEADD('minute', -UNIFORM(90, 4320, RANDOM(12)), n.TS)                    AS ORDER_TS,
        ARRAY_CONSTRUCT('USD', 'EUR', 'GBP')[UNIFORM(0, 2, RANDOM(13))]::VARCHAR   AS CURRENCY,
        ROUND(UNIFORM(10::FLOAT, 500::FLOAT, RANDOM(14)), 2)                       AS ORDER_TOTAL,
        ARRAY_CONSTRUCT('NA', 'EMEA', 'APAC')[UNIFORM(0, 2, RANDOM(15))]::VARCHAR  AS REGION_CODE,
        UNIFORM(1, 100, RANDOM(16))                                                AS FATE,
        n.TS                                                                       AS NOW_TS
    FROM TABLE(GENERATOR(ROWCOUNT => 500)) CROSS JOIN now_utc n
)
-- insert: every order is created as NEW
SELECT ORDER_ID, CUSTOMER_ID, 'NEW', ORDER_TS, CURRENCY, ORDER_TOTAL, REGION_CODE, 'I', ORDER_TS, '1'
FROM base
UNION ALL
-- update: most orders progress; some are cancelled or returned
SELECT ORDER_ID, CUSTOMER_ID,
       CASE WHEN FATE <= 10 THEN 'CANCELLED' WHEN FATE <= 15 THEN 'RETURNED'
            WHEN FATE <= 60 THEN 'SHIPPED' ELSE 'PAID' END,
       ORDER_TS, CURRENCY, ORDER_TOTAL, REGION_CODE, 'U', LEAST(DATEADD('minute', 30, ORDER_TS), NOW_TS), '2'
FROM base WHERE FATE <= 90
UNION ALL
-- delete: a few orders are hard-deleted in OMS
SELECT ORDER_ID, CUSTOMER_ID, 'NEW', ORDER_TS, CURRENCY, ORDER_TOTAL, REGION_CODE, 'D', LEAST(DATEADD('minute', 60, ORDER_TS), NOW_TS), '3'
FROM base WHERE FATE > 98
UNION ALL
-- replication heartbeat: GoldenGate is current as of seed time, so the freshness sensor passes
SELECT ORDER_ID, CUSTOMER_ID, 'PAID', ORDER_TS, CURRENCY, ORDER_TOTAL, REGION_CODE, 'U', NOW_TS, '9'
FROM base WHERE ORDER_ID = 1
""",
    f"""
CREATE OR REPLACE TABLE {DB}.REF_FINANCE.FX_DAILY_RATES AS
SELECT DATEADD('day', 1 - d.SEQ, CURRENT_DATE()) AS RATE_DATE, c.CURRENCY, c.USD_RATE
FROM (SELECT SEQ4() AS SEQ FROM TABLE(GENERATOR(ROWCOUNT => 120))) d
CROSS JOIN (
    SELECT column1 AS CURRENCY, column2::NUMBER(18, 8) AS USD_RATE
    FROM VALUES ('USD', 1), ('EUR', 1.08), ('GBP', 1.27)
) c
""",
    # Customers (for the live "build a new pipeline" demo; see demo/oms_customers.spec.yaml)
    f"""
CREATE OR REPLACE TABLE {DB}.RAW_OMS.CUSTOMERS (
    CUSTOMER_ID NUMBER, EMAIL VARCHAR, FULL_NAME VARCHAR, SEGMENT VARCHAR, REGION_CODE VARCHAR,
    STATUS VARCHAR, SIGNUP_TS TIMESTAMP_NTZ,
    OP_TYPE VARCHAR(1), OP_TS TIMESTAMP_NTZ, POS VARCHAR
)""",
    f"""
INSERT INTO {DB}.RAW_OMS.CUSTOMERS
    (CUSTOMER_ID, EMAIL, FULL_NAME, SEGMENT, REGION_CODE, STATUS, SIGNUP_TS, OP_TYPE, OP_TS, POS)
WITH now_utc AS (
    SELECT CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ AS TS
),
base AS (
    SELECT
        SEQ4() + 1000                                                                       AS CUSTOMER_ID,
        'customer' || (SEQ4() + 1000) || '@example.com'                                     AS EMAIL,
        'Customer ' || (SEQ4() + 1000)                                                      AS FULL_NAME,
        ARRAY_CONSTRUCT('CONSUMER', 'CONSUMER', 'SMB', 'ENTERPRISE')[UNIFORM(0, 3, RANDOM(21))]::VARCHAR AS SEGMENT,
        ARRAY_CONSTRUCT('NA', 'EMEA', 'APAC')[UNIFORM(0, 2, RANDOM(22))]::VARCHAR           AS REGION_CODE,
        DATEADD('minute', -UNIFORM(90, 4320, RANDOM(23)), n.TS)                             AS SIGNUP_TS,
        UNIFORM(1, 100, RANDOM(24))                                                         AS FATE,
        n.TS                                                                                AS NOW_TS
    FROM TABLE(GENERATOR(ROWCOUNT => 1000)) CROSS JOIN now_utc n
)
-- insert: every customer signs up ACTIVE
SELECT CUSTOMER_ID, EMAIL, FULL_NAME, SEGMENT, REGION_CODE, 'ACTIVE', SIGNUP_TS, 'I', SIGNUP_TS, '1'
FROM base
UNION ALL
-- update: some are later suspended or closed
SELECT CUSTOMER_ID, EMAIL, FULL_NAME, SEGMENT, REGION_CODE,
       CASE WHEN FATE <= 8 THEN 'CLOSED' ELSE 'SUSPENDED' END,
       SIGNUP_TS, 'U', LEAST(DATEADD('minute', 45, SIGNUP_TS), NOW_TS), '2'
FROM base WHERE FATE <= 20
UNION ALL
-- delete: a few are hard-deleted in OMS
SELECT CUSTOMER_ID, EMAIL, FULL_NAME, SEGMENT, REGION_CODE, 'ACTIVE', SIGNUP_TS, 'D', LEAST(DATEADD('minute', 60, SIGNUP_TS), NOW_TS), '3'
FROM base WHERE FATE > 98
UNION ALL
-- replication heartbeat: GoldenGate is current as of seed time
SELECT CUSTOMER_ID, EMAIL, FULL_NAME, SEGMENT, REGION_CODE, 'ACTIVE', SIGNUP_TS, 'U', NOW_TS, '9'
FROM base WHERE CUSTOMER_ID = 1000
""",
]

if ENV != "prod":

    @dag(
        dag_id="demo__seed_raw_oms",
        schedule=None,
        tags=["demo"],
        default_args={"owner": "platform"},
        doc_md=__doc__,
    )
    def seed_raw_oms():
        SQLExecuteQueryOperator(task_id="seed", conn_id=CONN_ID, sql=SEED_SQL)

    seed_raw_oms()
