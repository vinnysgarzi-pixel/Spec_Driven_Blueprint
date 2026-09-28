# Spec-driven pipelines with Blueprint: an end-to-end example

**Use case:** Oracle OMS orders -> GoldenGate -> Snowflake RAW -> curated SILVER -> GOLD revenue mart,
orchestrated by Astronomer and consumed by a second domain (Finance).

**The path this demonstrates:** spec (YAML, in Git) -> coding agent (Codex / Claude Code) ->
Blueprint YAML + SQL (in Git, PR-reviewed) -> validated, runnable Airflow DAG. There is no UI or drag-and-drop step.

```
 specs/sales/oms_orders.spec.yaml          <- analyst/engineer writes this (WHAT)
          |
          |  Codex / Claude Code, governed by AGENTS.md
          v
 dags/domains/sales/oms_orders.dag.yaml    <- generated (HOW: which Blueprints, wired how)
 include/sql/sales/daily_order_revenue.sql <- generated (gold business logic)
          |
          |  blueprint lint  ->  pytest  ->  af dags errors  ->  PR  ->  astro deploy
          v
 Airflow DAG  sales__oms_orders            <- built at parse time from platform Blueprints
```

---

## 1. Architecture

| Layer | Where | Who owns it |
|---|---|---|
| Replication | Oracle OMS -> GoldenGate -> `ADW.RAW_OMS.ORDERS` (audit-log mode: one row per I/U/D with `OP_TYPE`, `OP_TS`, `POS`) | Platform / DBA |
| Silver | `ADW.SILVER_OMS.ORDERS`: current state, one row per order | Domain (Sales) |
| Gold | `ADW.GOLD_SALES.DAILY_ORDER_REVENUE` | Domain (Sales), **published** |
| Downstream gold | `ADW.GOLD_FINANCE.DAILY_REVENUE_USD` | Domain (Finance) |
| Orchestration | Astronomer (Airflow 3); DAGs built by Blueprint | Platform provides Blueprints; domains own YAML |

## 2. The use case (deliberately small)

One source table, one CDC-to-current-state merge, one quality gate, one gold aggregate, one consumer.
Every other GoldenGate table in ADW follows the same shape, so this is the repeatable unit.

## 3. The Blueprints (platform-owned building blocks)

All in `dags/blueprints/adw_blueprints.py`, each a typed, validated Pydantic config + a `render()`:

| Blueprint | What it renders | Key inputs |
|---|---|---|
| `goldengate_landed` | `SqlSensor` (reschedule mode) waiting until `MAX(OP_TS) >= data_interval_end` | `raw_table`, `op_ts_column`, `timeout_minutes` |
| `cdc_merge_to_silver` | `ensure_target` + idempotent watermark `MERGE` applying I/U/D, dedupe by `QUALIFY ROW_NUMBER()`; emits an Asset | `source_table`, `target_table`, `primary_key`, `columns`, `op_type/op_ts/tiebreak_column`, `lookback_minutes` |
| `quality_gate` | `SQLColumnCheckOperator` + `SQLTableCheckOperator` | `not_null`, `unique`, `accepted_values`, `min_values`, `row_checks`, `min_row_count` |
| `sql_model` | Materializes a `SELECT` from `include/sql/` via `replace` or interval-scoped `delete_insert`; emits an Asset | `target_table`, `sql_file`, `strategy`, `partition_column`, `params` |
| `DataProductDagArgs` (DAG-level) | Every DAG is a data product: `domain`, `data_product`, `owner`, `spec`, `schedule` **or** `consumes` | Tags, owner, retries with backoff, timeouts, `max_active_runs=1`, and a real data-interval timetable |

Guardrails built into the Blueprints (each one caught at parse/lint time, before anything reaches Snowflake):
- Identifiers and table names are pattern-validated, so YAML written by a person or an agent can't inject SQL.
- Unknown keys are rejected, which catches typos like `primary_keys`.
- Contradictions are rejected, for example `schedule` together with `consumes`, or `delete_insert` without a `partition_column`.
- A cron `schedule` becomes a `CronDataIntervalTimetable`. Airflow 3's default cron timetable gives zero-length data intervals, which would silently break windowed SQL.

Machine-readable schemas for each Blueprint live in `blueprint/generated-schemas/` (IDE autocomplete, Astro IDE, agents).

## 4. The specification

`specs/sales/oms_orders.spec.yaml`: business language only. Sources, business key, CDC columns,
freshness SLA, silver/gold tables, quality rules as sentences, metric definitions, cadence. No Airflow or
Blueprint vocabulary. `specs/finance/revenue_usd.spec.yaml` is a second domain consuming the first.

## 5. Agentic code generation

`AGENTS.md` (Codex) / `CLAUDE.md` -> `@AGENTS.md` (Claude Code) is the contract. It tells the agent:
- what it may edit: only `dags/domains/**.dag.yaml` and `include/sql/**`. Blueprints and tests are off-limits;
- how to discover inputs (`blueprint list` / `describe`) instead of guessing;
- a deterministic **spec -> Blueprint mapping table** (freshness -> `goldengate_landed`, "one of" -> `accepted_values`, "processing the previous day" -> `delete_insert`, ...);
- the **iteration loop** and the definition of done.

A prompt that works with either tool:

> Implement `specs/sales/oms_orders.spec.yaml` following AGENTS.md. Iterate until lint, tests and
> `af dags errors` pass. In your summary, list the spec -> step mapping and any assumptions.

What iteration looks like: the agent's first draft has `primary_keys:` and lint answers precisely:

```
FAIL dags/domains/sales/oms_orders.dag.yaml [dev]
  Error: 2 validation errors for CdcMergeConfig
primary_key   Field required
primary_keys  Extra inputs are not permitted
```

The agent fixes it and re-runs. Then `tests/test_blueprint_dags.py` checks what lint can't:
- every spec output table is actually produced (published as an Asset) by the DAG named `<domain>__<product>`;
- conventions: domain tag, owner, timeouts, `max_active_runs`;
- the **rendered** SQL for a sample day is windowed correctly, and the dev profile targets `ADW_DEV`.

The agent can't make these pass by editing tests or Blueprints; AGENTS.md forbids it and PR review enforces it.

## 6. Output and execution model

Generated artifacts (all plain text, all diffable in a PR):
- `dags/domains/sales/oms_orders.dag.yaml`: 5 steps, ~60 lines
- `include/sql/sales/daily_order_revenue.sql`: the gold SELECT
- `dags/domains/finance/revenue_usd.dag.yaml` + `include/sql/finance/daily_revenue_usd.sql`

At parse time, `dags/loader.py` calls `build_all_airflow_dags(profile=$ADW_ENV)`, which resolves
`${adw_db}` to `ADW` (prod) or `ADW_DEV` (dev) and builds real Airflow DAGs:

```
sales__oms_orders   (daily 06:00 America/New_York, interval = previous 24h)
  orders_landed                 wait for GoldenGate to pass the interval end (fail after 2h)
  silver_orders.ensure_target   CREATE TABLE IF NOT EXISTS
  silver_orders.merge_cdc       MERGE I/U/D into SILVER  -> Asset x-snowflake://ADW/SILVER_OMS/ORDERS
  silver_orders_quality.*       not-null / unique / accepted values / min
  gold_daily_order_revenue      DELETE+INSERT the interval's day -> Asset x-snowflake://ADW/GOLD_SALES/DAILY_ORDER_REVENUE
  gold_daily_order_revenue_quality.*

finance__revenue_usd  (schedule = the Sales gold Asset; no cron, no cross-DAG sensor)
  gold_daily_revenue_usd -> gold_daily_revenue_usd_quality.*
```

Re-running any day gives the same result. The MERGE only applies changes at least as new as the stored row, and
gold rewrites exactly one day. A failed quality gate stops everything downstream, so no bad data is published
and Finance is never triggered. The Airflow UI shows each task's resolved Blueprint config and source under
**Rendered Template** (`blueprint_step_config`, `blueprint_step_code`).

## 7. Extensibility: multiple teams and data mesh

- **Domain folders = ownership.** `dags/domains/<domain>/` + `specs/<domain>/` map cleanly to CODEOWNERS.
  Each domain can get its own DAG-args template by dropping a `dag_args.py` in its folder, because Blueprint uses the template closest above each YAML file.
- **Shared raw, owned products.** Many teams can build their own silver/gold from the same `RAW_*` tables with the
  same Blueprints. Only tables a DAG publishes become Assets that other domains can depend on.
- **Contracts are Assets.** Finance declares `consumes: [ADW.GOLD_SALES.DAILY_ORDER_REVENUE]`. It never needs
  Sales' DAG id, schedule, or silver tables. Across Astro deployments the same Asset can be fed via the REST API.
- **Platform evolves safely.** Breaking Blueprint changes ship as `CdcMergeToSilverV2`; existing YAML can pin
  `version: 1`. When several repos need them, publish the Blueprints as a package (entry point
  `airflow_blueprint.blueprints`) instead of copying files.
- **New patterns** (SCD2 history, PII masking, Iceberg external tables) are new Blueprints plus a new row in the AGENTS.md
  mapping table. Specs and agents pick them up with no other change.

## Running it

1. Create Airflow connection `snowflake_adw` (Snowflake), with a role that can read `RAW_OMS` and write `SILVER_OMS` / `GOLD_*`.
2. Set `ADW_ENV=dev` or `prod` (default `dev`).
3. Validate locally:
   ```bash
   for f in dags/domains/*/*.dag.yaml; do .venv/bin/blueprint lint --root dags "$f"; done
   uv run --no-project --python .venv/bin/python --with pytest python -m pytest tests -q
   ```
