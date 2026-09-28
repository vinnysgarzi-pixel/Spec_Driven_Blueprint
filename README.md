# ADW spec-driven pipelines with Blueprint

A small, end-to-end example of **spec → agent-generated implementation → Airflow on Astro** for a
GoldenGate → Snowflake → silver/gold architecture. Everything is code in Git; merging to `main` deploys it.

```
 specs/sales/oms_orders.spec.yaml            WHAT  - written by an engineer or analyst
          │  Codex / Claude Code, governed by AGENTS.md
          ▼
 dags/domains/sales/oms_orders.dag.yaml      HOW   - generated: Blueprint steps + wiring
 include/sql/sales/daily_order_revenue.sql         - generated: gold business logic
          │  lint + tests → PR → merge to main
          ▼
 Astro deployment "AH Blueprint"             RUN   - auto-deployed by the Astro GitHub integration
```

## What it proves

- **Familiar architecture.** Oracle OMS → GoldenGate (I/U/D change rows) → Snowflake `RAW_OMS` → current-state `SILVER_OMS` → `GOLD_SALES` mart, orchestrated by Astronomer.
- **A repeatable unit.** Four platform Blueprints cover the whole flow; every other GoldenGate table reuses them.
- **Specs in business language.** `specs/**.spec.yaml` holds keys, freshness SLA, quality rules and metric definitions. There is no Airflow vocabulary.
- **Agent-generated, reviewable code.** `AGENTS.md` (Claude Code reads it via `CLAUDE.md`) maps spec → Blueprints, limits what the agent may edit, and defines the lint/test loop it iterates on.
- **Safe to merge.** Typos, SQL-injection attempts and contradictory settings fail at lint time. Tests check that every spec output is produced and that the rendered SQL is windowed correctly.
- **Ready for a data mesh.** Finance's DAG runs when Sales *publishes* its gold table (an Airflow Asset). It never touches Sales' DAG or silver tables.

## Layout

| Path | Owner | Purpose |
|---|---|---|
| `specs/<domain>/*.spec.yaml` | Domain team | Input: what the pipeline must do |
| `AGENTS.md`, `CLAUDE.md` | Platform | Agent contract: mapping rules, allowed edits, iteration loop |
| `dags/blueprints/adw_blueprints.py` | Platform | Blueprints + the data-product DAG template |
| `dags/blueprint.vars.yaml` | Platform | `dev`/`prod` profiles (`DEMO` / `ADW`), selected by `$ADW_ENV` |
| `dags/domains/<domain>/*.dag.yaml` | Generated | Implementation: Blueprint steps + dependencies |
| `include/sql/<domain>/*.sql` | Generated | Gold `SELECT` models |
| `tests/` | Platform | Build, convention, spec-coverage and rendered-SQL tests |

| Blueprint | Does |
|---|---|
| `goldengate_landed` | Sensor: waits until GoldenGate has replicated past the run window |
| `cdc_merge_to_silver` | Idempotent `MERGE` of I/U/D changes into a current-state table; publishes an Asset |
| `quality_gate` | Not-null / unique / accepted-values / min / custom row checks |
| `sql_model` | Materializes a gold `SELECT` (`replace` or per-day `delete_insert`); publishes an Asset |

```
sales__oms_orders     daily 06:00 America/New_York, previous 24h
  orders_landed → silver_orders (merge) → silver quality → gold_daily_order_revenue → gold quality
finance__revenue_usd  runs when GOLD_SALES.DAILY_ORDER_REVENUE is published
  gold_daily_revenue_usd → gold quality
```

## Use it

**Explore the building blocks**
```bash
.venv/bin/blueprint list
.venv/bin/blueprint describe cdc_merge_to_silver
```

**Generate a pipeline from a spec**: in Codex or Claude Code:
> Implement `specs/sales/oms_orders.spec.yaml` following AGENTS.md. Iterate until lint, tests and
> `af dags errors` pass. Summarize the spec → step mapping and any assumptions.

To demo from scratch, delete the generated `.dag.yaml` and `.sql` first. To demo a change, edit the spec and ask the agent to update the implementation.

**Validate**
```bash
for f in dags/domains/*/*.dag.yaml; do .venv/bin/blueprint lint --root dags "$f"; done
uv run --no-project --python .venv/bin/python --with pytest python -m pytest tests -q
```

## Deploy: push to `main`

`main` is mapped to the **AH Blueprint** deployment (Cosmic Energy org → Sandbox workspace, AWS `us-east-1`)
through the Astro GitHub integration. **Every commit or merged PR on `main` deploys automatically**:

- Changes only under `dags/` → a DAG-only deploy (fast, no image build).
- Changes to `requirements.txt`, `Dockerfile` or other project files → a full image deploy.
- Track progress in the deployment's **Overview → Deploy History**. Git deploys show **Deployed By: GitHub App** and the commit message.

The integration does **not** run tests, so run the validation commands above (or add them as a PR check) before merging.

## Run against Snowflake

- **Connection `snowflake`:** comes from the Astro Environment Manager (Sandbox workspace, auto-linked to the deployment).
- **`ADW_ENV`:** `dev` (default, uses `DEMO`) or `prod` (uses `ADW`).
- **Tables:** reads `<db>.RAW_OMS.ORDERS` and `<db>.REF_FINANCE.FX_DAILY_RATES`; creates and writes `SILVER_OMS`, `GOLD_SALES` and `GOLD_FINANCE` tables.
- **Raw shape:** business columns plus `OP_TYPE` (I/U/D), `OP_TS` (UTC commit time) and `POS` (tiebreak).
- **Freshness sensor:** `orders_landed` waits until `MAX(OP_TS)` passes the end of the run window.

New DAGs arrive **paused** on Astro.

**Run the demo end to end (dev only):**
1. Trigger `demo__seed_raw_oms`. It creates the schemas in `DEMO`, generates ~500 GoldenGate-style orders (I/U/D rows over the last 3 days, replicated up to "now"), loads FX rates, and drops silver and gold for a clean start.
2. Trigger `sales__oms_orders`. It fills `SILVER_OMS.ORDERS` and yesterday's `GOLD_SALES.DAILY_ORDER_REVENUE`.
3. `finance__revenue_usd` starts on its own when the Sales gold Asset is published (unpause it first).

The seed DAG is not defined when `ADW_ENV=prod`, because it drops tables.

## Extending

- **Ownership:** folders per domain (`specs/<domain>/`, `dags/domains/<domain>/`) map to CODEOWNERS.
- **Shared raw data:** many teams build their own products from the same raw tables. Only published tables become Assets others can depend on.
- **New patterns:** SCD2, PII masking, Iceberg and the like become a new Blueprint plus a row in `AGENTS.md`. Breaking changes ship as `…V2`, and YAML can pin `version: 1`.
- **Many repos:** publish the Blueprints as a Python package (entry point `airflow_blueprint.blueprints`).

## Notes

- **Pinned Snowflake provider:** `apache-airflow-providers-snowflake==6.9.1`, pinned together with Runtime 3.1-12's `common-sql`/`common-compat`. Newer releases break on this Runtime.
- **No Jinja in the Blueprint file:** Blueprint 0.5.0 renders each Blueprint's source as Jinja, so `adw_blueprints.py` builds Jinja delimiters at runtime. A test enforces this.
- **UTC:** `ORDER_TS` and `OP_TS` are assumed UTC; gold order dates are UTC days.
- **DAG versions on Astro:** runs execute the code of the latest *DAG version*, and Airflow 3 only creates a new version when the serialized DAG changes. `sql_model` embeds its SQL in the task's `doc_md` so SQL edits count. An edit only inside a Python task body (as in the `demo/` DAGs) does not, so change something serialized (a docstring or parameter) as well.
