# AGENTS.md: spec -> Blueprint implementation contract

You are implementing Airflow pipelines for the ADW platform. Pipelines are
**specified** in `specs/` and **implemented** by composing platform Blueprints.
Follow this file exactly; it is the same contract for Codex, Claude Code, and humans.

## Repository map

| Path | Owner | Agent may edit? |
|---|---|---|
| `specs/<domain>/<product>.spec.yaml` | Domain team (engineer/analyst) | No: it is the input. Ask if it is ambiguous. |
| `dags/blueprints/` | Platform team | **No.** If a spec can't be expressed with existing Blueprints, stop and report the gap. |
| `dags/blueprint.vars.yaml` | Platform team | No |
| `dags/domains/<domain>/<product>.dag.yaml` | Generated | **Yes**: this is your output |
| `include/sql/<domain>/<model>.sql` | Generated | **Yes**: this is your output |
| `tests/` | Platform team | No (run them) |

## Discover the building blocks first

```bash
.venv/bin/blueprint list                 # blueprints, versions, DAG-args template
.venv/bin/blueprint describe <name>      # every config field, type, default, constraint
```

Never invent a config field; `describe` is authoritative. Step configs forbid unknown keys.

## Mapping a spec to an implementation

| Spec element | Implementation |
|---|---|
| `domain`, `data_product`, `owner`, `summary` | DAG-level keys `domain`, `data_product`, `owner`, `description`; set `spec:` to the spec path |
| dag_id | `<domain>__<data_product>` |
| `schedule.cadence` as a time ("daily at 06:00 America/New_York") | `schedule: "0 6 * * *"`, `timezone: America/New_York`. Runs process the preceding data interval. |
| `schedule.cadence` "whenever X publishes" | `consumes: [<X table>]`; omit `schedule` |
| `source.freshness` | `goldengate_landed` step on the raw table, `timeout_minutes` from the SLA |
| `source.tables[*]` + a silver output | `cdc_merge_to_silver` step: `business_key` -> `primary_key`; `change_tracking` -> `op_type_column` / `op_ts_column` / `tiebreak_column`; `late_arriving_changes` -> `lookback_minutes` |
| `quality` bullets on any output | `quality_gate` step on that table, placed **before** anything that reads the table. Map: never null -> `not_null`; unique -> `unique`; "one of" -> `accepted_values`; "never negative" -> `min_values: {COL: 0}`; anything else -> `row_checks` |
| gold output with metrics | write `include/sql/<domain>/<table_lower>.sql` + `sql_model` step |
| "processing the previous day" / re-runnable by day | `strategy: delete_insert`, `partition_column` = the date column |
| "trailing N days, fully rebuilt" | `strategy: replace` |

Other rules:

- Table names: always `${adw_db}.<SCHEMA>.<TABLE>`, where spec names are `SCHEMA.TABLE`. Never hard-code the database; the profile picks the database (`ADW` in prod).
- Connection: `conn_id: ${snowflake_conn_id}` on every step.
- `consumes` lists only tables **published by another DAG** (they are Airflow Assets). Reference tables that nothing in Airflow publishes (for example FX rates) are read in SQL only.
- Wire steps with `depends_on`. The order is always: landed -> merge -> silver quality -> gold model -> gold quality.

## SQL model rules (`include/sql/**`)

- A single `SELECT`. No DDL/DML; the `sql_model` Blueprint materializes it.
- Reference tables only through `{{ params.<name> }}` and pass them via the step's `params:` (so `${adw_db}` resolves per environment).
- For `delete_insert`, filter to the run window using exactly
  `<date_col> >= '{{ data_interval_start | ds }}' AND <date_col> < '{{ data_interval_end | ds }}'`.
- Output column names are upper-case and match the spec's metric names. Use `DIV0` for ratios.
- First line is a comment naming the spec file it implements.

## Iteration loop (repeat until everything is green)

```bash
for f in dags/domains/*/*.dag.yaml; do .venv/bin/blueprint lint --root dags "$f"; done   # schema + vars + deps, every profile
.venv/bin/blueprint vars --root dags dags/domains/<domain>/<product>.dag.yaml          # resolved ${...} values
uv run --no-project --python .venv/bin/python --with pytest python -m pytest tests -q   # builds DAGs, checks conventions + spec coverage, renders SQL
af dags errors                                                                        # the running Airflow parses it cleanly
```

`blueprint lint` takes one file at a time here: a bare `lint` walks the whole project tree (including Airflow log dirs).

Read each failure message and fix the YAML/SQL; the Pydantic errors name the exact field.
Never edit Blueprints or tests to get a green run.

## Definition of done

1. Every spec output table has a producing step, and every `quality` bullet maps to a check.
2. `blueprint lint`, `pytest`, and `af dags errors` pass.
3. The PR description lists: spec -> step mapping, any assumption you made, and any spec gap found.
