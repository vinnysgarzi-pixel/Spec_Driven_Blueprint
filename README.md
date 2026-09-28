# ADW spec-driven pipelines with Blueprint

A small, end-to-end example of **spec → agent-generated implementation → running workflow on Astro**
for a GoldenGate → Snowflake → silver/gold architecture. Everything is code in Git, and merging to `main` deploys it.

```
 specs/sales/oms_orders.spec.yaml          WHAT  written by an engineer or analyst (business language)
        │   Codex / Claude Code, following AGENTS.md
        ▼
 dags/domains/sales/oms_orders.dag.yaml    HOW   generated: which Blueprints, configured and wired
 dags/sql/sales/daily_order_revenue.sql          generated: gold business logic (one SELECT)
        │   blueprint lint + pytest → PR → merge to main
        ▼
 Astro deployment "AH Blueprint"           RUN   auto-deployed by the Astro GitHub integration
```

---

## How this answers the original ask

| Ask | Where it's shown | Why it satisfies it |
|---|---|---|
| **1. Familiar architecture**: Oracle/Postgres → GoldenGate → Snowflake raw → silver/gold, orchestrated by Astronomer | `specs/sales/oms_orders.spec.yaml`, `sales__oms_orders` DAG | Raw is a GoldenGate audit log (`OP_TYPE` I/U/D, `OP_TS`, `POS`). The DAG waits for replication, merges changes into current-state silver, checks quality, builds gold, and runs on Astro. |
| **2. Limited, repeatable use case** | One table (`ORDERS`), four Blueprints | Every other GoldenGate table follows the same four steps; only the spec changes. |
| **3. Show the Blueprint and its inputs** | `dags/blueprints/adw_blueprints.py`, `blueprint describe <name>` | Each Blueprint is a typed Pydantic config plus a `render()`. The config *is* the list of required inputs, with types, defaults and constraints. |
| **4. Show the specification** | `specs/**/*.spec.yaml` | Keys, CDC columns, freshness SLA, quality rules as sentences, metric definitions, cadence. No Airflow vocabulary. |
| **5. Agentic generation (Codex / Claude)** | `AGENTS.md` (+ `CLAUDE.md`) | A deterministic spec → Blueprint mapping table, a list of the only files the agent may edit, and a lint/test loop it iterates on until green. Plain files in the repo; no proprietary tool needed. |
| **6. Output and execution model** | `dags/domains/**.dag.yaml`, `dags/sql/**.sql`, the running DAGs | About 60 lines of YAML and one SELECT become real Airflow DAGs: sensor → idempotent MERGE → quality gates → gold → Asset hand-off. |
| **7. Multiple teams / data mesh** | `specs/finance/`, `finance__revenue_usd` | Finance runs when Sales *publishes* its gold table (an Airflow Asset). It never references Sales' DAG, schedule, raw or silver tables. |
| **Code-first, Git-compatible** (not UI) | This repo + GitHub → Astro mapping | Everything is diffable text, reviewed in PRs, and deployed on merge to `main`. |

**Verified on Astro (AH Blueprint deployment, Snowflake `DEMO` database):**
- `demo__seed_raw_oms` loaded GoldenGate-style data.
- `sales__oms_orders` passed all 9 tasks.
- `finance__revenue_usd` then started **automatically** from the Sales Asset and passed.

---

## What each part does

### Inputs (owned by domain teams)
| File | What it is |
|---|---|
| `specs/sales/oms_orders.spec.yaml` | Sales' data product: GoldenGate `RAW_OMS.ORDERS` → `SILVER_OMS.ORDERS` → `GOLD_SALES.DAILY_ORDER_REVENUE`. |
| `specs/finance/revenue_usd.spec.yaml` | Finance's data product: builds `GOLD_FINANCE.DAILY_REVENUE_USD` from Sales' published gold plus FX rates. |

### Platform (owned by the platform team; agents may not edit)
| File | What it does |
|---|---|
| `AGENTS.md`, `CLAUDE.md` | The agent contract. Codex reads `AGENTS.md`; Claude Code reads `CLAUDE.md`, which imports it. |
| `dags/blueprints/adw_blueprints.py` | The four Blueprints plus `DataProductDagArgs`, which makes every DAG a data product (domain, owner, spec link, tags, retries, timeouts, real daily data windows). |
| `dags/blueprint.vars.yaml` | Shared variables: `${adw_db}` (`dev` → `DEMO`, `prod` → `ADW`) and `${snowflake_conn_id}`. |
| `dags/loader.py` | Builds every `*.dag.yaml` into an Airflow DAG using the profile named in `$ADW_ENV` (default `dev`). |
| `tests/test_blueprint_dags.py` | Checks that DAGs build under both profiles, follow conventions, implement every spec output, render correctly windowed SQL, and that SQL edits create new DAG versions. |
| `blueprint/generated-schemas/` | JSON Schemas for each Blueprint, for editor autocomplete, the Astro IDE and agents. |

### The Blueprints
| Blueprint | What it renders | Why |
|---|---|---|
| `goldengate_landed` | Reschedule-mode sensor: `MAX(OP_TS) >= data_interval_end` | Don't transform until GoldenGate has replicated the whole window; fail if replication lags past the SLA. |
| `cdc_merge_to_silver` | `CREATE TABLE IF NOT EXISTS` + `MERGE` that applies I/U/D, keeps only the latest change per key (`QUALIFY ROW_NUMBER()`), and re-reads a lookback window | Current-state silver that is safe to re-run and tolerates late rows. Publishes the silver Asset. |
| `quality_gate` | `SQLColumnCheckOperator` + `SQLTableCheckOperator` | Turns the spec's quality sentences into checks. A failure stops the run before bad data is published. |
| `sql_model` | Runs a `SELECT` from `dags/sql/` as `replace` (full rebuild) or `delete_insert` (rewrites only this run's day) | Business logic stays in reviewable SQL; re-running a day gives the same result. Publishes the gold Asset. |

Guardrails catch these at lint or parse time, before Snowflake is touched:
- unknown or misspelled keys;
- SQL-injection attempts in table or column names;
- contradictory settings (`schedule` together with `consumes`, `delete_insert` without a partition column).

### Generated implementation (the agent's output)
| File | What it does |
|---|---|
| `dags/domains/sales/oms_orders.dag.yaml` | `sales__oms_orders`: daily 06:00 America/New_York, processing the previous 24h: `orders_landed → silver_orders → silver_orders_quality → gold_daily_order_revenue → gold_daily_order_revenue_quality` |
| `dags/sql/sales/daily_order_revenue.sql` | Gold metrics: `ORDER_COUNT`, `GROSS_REVENUE`, `AVG_ORDER_VALUE` per day × region × currency |
| `dags/domains/finance/revenue_usd.dag.yaml` | `finance__revenue_usd`: `consumes` the Sales gold Asset, so no cron is needed |
| `dags/sql/finance/daily_revenue_usd.sql` | Trailing 90 days in USD; a missing FX rate yields NULL, which the quality gate catches |

### Demo helpers (dev only, not part of the pattern)
| DAG | What it does |
|---|---|
| `demo__seed_raw_oms` | Creates the schemas in `DEMO`, generates ~500 GoldenGate-style orders (I/U/D over the last 3 days, replicated up to "now"), loads FX rates, and drops silver and gold for a clean run. Not defined when `ADW_ENV=prod`. |
| `demo__snowflake_access_probe` | Read-only: reports where the Snowflake connection's role can create schemas and tables. It found `DEMO` (via `DEMO_WRITE_ALL`). |

---

## Use it

**Explore the building blocks**
```bash
.venv/bin/blueprint list
.venv/bin/blueprint describe cdc_merge_to_silver
```

**Generate or change a pipeline with an agent.** In Codex or Claude Code:
> Implement `specs/sales/oms_orders.spec.yaml` following AGENTS.md. Iterate until lint, tests and
> `af dags errors` pass. Summarize the spec → step mapping and any assumptions.

**Validate** (the same loop the agent runs)
```bash
for f in dags/domains/*/*.dag.yaml; do .venv/bin/blueprint lint --root dags "$f"; done
uv run --no-project --python .venv/bin/python --with pytest python -m pytest tests -q
af dags errors
```

**Deploy.** Push or merge to `main`. The Astro GitHub integration deploys to **AH Blueprint**
(Cosmic Energy → Sandbox workspace, AWS `us-east-1`):
- a change only under `dags/` (YAML, SQL, Blueprints) gets a DAG-only deploy;
- a change to `requirements.txt` / `Dockerfile` gets a full image deploy;
- watch progress in **Overview → Deploy History** (Deployed By: GitHub App).

Git deploys don't run tests, so validate before merging.

**Run on Astro**
1. Trigger `demo__seed_raw_oms`.
2. Trigger `sales__oms_orders` from the UI. When triggering through the API, pass a `logical_date`; a run without one has no data interval.
3. `finance__revenue_usd` starts automatically when Sales' gold Asset updates.

Requirements: the `snowflake` connection (Environment Manager, Sandbox workspace, auto-linked) and `ADW_ENV` (unset = `dev` = `DEMO`).

---

## Extending to more teams

- **Ownership:** `specs/<domain>/` and `dags/domains/<domain>/` map to CODEOWNERS. A domain can override DAG defaults with its own `dag_args.py` in its folder.
- **Shared raw data, owned products:** any team can build from the same `RAW_*` tables with the same Blueprints. Only published tables become Assets others can consume.
- **New patterns** (SCD2, PII masking, Iceberg): a new Blueprint plus a row in `AGENTS.md`. Breaking changes ship as `…V2`, and YAML can pin `version: 1`.
- **Many repos:** publish the Blueprints as a Python package (entry point `airflow_blueprint.blueprints`).

## Notes

- **Pinned providers:** `apache-airflow-providers-snowflake==6.9.1` is pinned with Runtime 3.1-12's `common-sql`/`common-compat`; newer releases break on this Runtime.
- **SQL lives in `dags/sql/`**, because Astro's versioned DAG bundle contains only `dags/` (not `include/`).
- **DAG versions:** Airflow 3 runs the code of the latest *DAG version*, and only creates a new one when the serialized DAG changes. `sql_model` embeds its SQL in `doc_md` so SQL edits count. An edit only inside a Python task body doesn't; change something serialized too, such as a docstring.
- **Blueprint 0.5.0** renders each Blueprint's source as Jinja, so `adw_blueprints.py` builds Jinja delimiters at runtime. A test enforces this.
- **UTC:** `ORDER_TS` / `OP_TS` are UTC; gold order dates are UTC days.
