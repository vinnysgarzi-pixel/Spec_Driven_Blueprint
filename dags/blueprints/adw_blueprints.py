"""ADW transformation Blueprints: GoldenGate raw (Snowflake) -> silver -> gold.

Platform-owned, versioned building blocks. Domain teams (and coding agents acting
on their specs) compose these from `*.dag.yaml` files; they do not write
operators directly. See AGENTS.md for the spec -> implementation contract.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import re
from pathlib import Path
from typing import Annotated, Any, Literal

from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator,
    SQLExecuteQueryOperator,
    SQLTableCheckOperator,
)
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.sdk import Asset, TaskGroup
from airflow.timetables.interval import CronDataIntervalTimetable
from blueprint import BaseModel, Blueprint, BlueprintDagArgs, ConfigDict, Field
from pydantic import field_validator, model_validator

# Config values are interpolated into SQL, so identifiers are pattern-restricted
# to keep YAML (human- or agent-authored) from injecting arbitrary SQL.
Ident = Annotated[str, Field(pattern=r"^[A-Za-z_][A-Za-z0-9_$]*$")]
TableFqn = Annotated[
    str,
    Field(
        pattern=r"^[A-Za-z_][A-Za-z0-9_$]*\.[A-Za-z_][A-Za-z0-9_$]*\.[A-Za-z_][A-Za-z0-9_$]*$",
        description="Fully qualified Snowflake table: DATABASE.SCHEMA.TABLE",
    ),
]
# Blueprint rejects Annotated types nested in list/dict/Optional, so those
# fields are plain types checked by these validators instead.
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_FQN_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*\.[A-Za-z_][A-Za-z0-9_$]*\.[A-Za-z_][A-Za-z0-9_$]*$")
_LITERAL_RE = re.compile(r"^[A-Za-z0-9_ .\-]*$")


def _require(values: Any, regex: re.Pattern[str], what: str) -> Any:
    items = values.keys() if isinstance(values, dict) else values if isinstance(values, list) else [values]
    for v in items:
        if v is not None and not regex.fullmatch(v):
            raise ValueError(f"{v!r} is not a valid {what}")
    return values

# Blueprint renders this module's source as a Jinja template (the blueprint_step_code
# field shown in the Airflow UI), so the file must not contain literal Jinja delimiters.
# Build them at runtime instead.
_OPEN, _CLOSE = "{" * 2, "}" * 2


def jinja(expr: str) -> str:
    return f"{_OPEN} {expr} {_CLOSE}"


def jinja_include(path: str) -> str:
    return "{" + f"% include '{path}' %" + "}"


SQL_ROOT = Path(__file__).resolve().parents[2] / "include" / "sql"
DEFAULT_CONN_ID = "snowflake"


def table_asset(fqn: str) -> Asset:
    """Every published table is an Airflow Asset; this is the data-mesh contract."""
    db, schema, table = fqn.upper().split(".")
    return Asset(uri=f"x-snowflake://{db}/{schema}/{table}", name=fqn.upper())


# --------------------------------------------------------------------------- #
# DAG-level template: every DAG is a *data product* with an owning domain.
# --------------------------------------------------------------------------- #
class DataProductDagConfig(BaseModel):
    domain: Ident = Field(description="Owning domain / team, e.g. sales, finance")
    data_product: Ident = Field(description="Data product this DAG maintains")
    owner: str = Field(description="Owning team (Airflow owner)")
    spec: str | None = Field(default=None, description="Path of the spec this DAG implements")
    description: str | None = None
    schedule: str | None = Field(
        default=None, description="Cron or preset; windowed by data interval. Mutually exclusive with consumes."
    )
    timezone: str = "UTC"
    consumes: list[str] = Field(
        default=[], description="Upstream published tables; DAG runs when all of them are updated"
    )
    start_date: str = "2026-01-01"
    retries: int = Field(default=2, ge=0, le=10)
    retry_delay_minutes: int = Field(default=5, ge=1)
    execution_timeout_minutes: int = Field(default=60, ge=1)
    tags: list[str] = []

    _consumes_fqn = field_validator("consumes")(lambda v: _require(v, _FQN_RE, "DATABASE.SCHEMA.TABLE"))

    @model_validator(mode="after")
    def _one_trigger(self) -> DataProductDagConfig:
        if self.schedule and self.consumes:
            raise ValueError("Set either 'schedule' or 'consumes', not both")
        return self


class DataProductDagArgs(BlueprintDagArgs[DataProductDagConfig], default=True):
    def render(self, config: DataProductDagConfig) -> dict[str, Any]:
        if config.consumes:
            schedule: Any = [table_asset(t) for t in config.consumes]
        elif config.schedule:
            schedule = CronDataIntervalTimetable(config.schedule, timezone=config.timezone)
        else:
            schedule = None

        doc = f"**Domain:** `{config.domain}` · **Data product:** `{config.data_product}` · **Owner:** {config.owner}"
        if config.spec:
            doc += f"\n\n**Implements spec:** `{config.spec}`"

        return {
            "schedule": schedule,
            "description": config.description,
            "start_date": datetime.fromisoformat(config.start_date),
            "max_active_runs": 1,
            "template_searchpath": [str(SQL_ROOT)],
            "doc_md": doc,
            "tags": sorted(
                {
                    *config.tags,
                    "blueprint",
                    f"domain:{config.domain}",
                    f"product:{config.data_product}",
                }
            ),
            "default_args": {
                "owner": config.owner,
                "retries": config.retries,
                "retry_delay": timedelta(minutes=config.retry_delay_minutes),
                "retry_exponential_backoff": True,
                "execution_timeout": timedelta(minutes=config.execution_timeout_minutes),
            },
        }


# --------------------------------------------------------------------------- #
# 1. Wait until GoldenGate has replicated past the end of the data interval.
# --------------------------------------------------------------------------- #
class GoldenGateLandedConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conn_id: str = DEFAULT_CONN_ID
    raw_table: TableFqn = Field(description="GoldenGate-replicated raw table")
    op_ts_column: Ident = Field(default="OP_TS", description="GoldenGate commit timestamp column (UTC)")
    poke_interval_seconds: int = Field(default=300, ge=30)
    timeout_minutes: int = Field(default=120, ge=1, description="Fail if replication lags this long")


class GoldengateLanded(Blueprint[GoldenGateLandedConfig]):
    """Reschedule-mode sensor: succeeds once MAX(op_ts) in the raw table passes data_interval_end."""

    def render(self, config: GoldenGateLandedConfig) -> SqlSensor:
        return SqlSensor(
            task_id=self.step_id,
            conn_id=config.conn_id,
            sql=(
                f"SELECT MAX({config.op_ts_column}) >= "
                f"CONVERT_TIMEZONE('UTC', '{jinja('data_interval_end.isoformat()')}'::TIMESTAMP_TZ)::TIMESTAMP_NTZ "
                f"FROM {config.raw_table}"
            ),
            mode="reschedule",
            poke_interval=config.poke_interval_seconds,
            timeout=config.timeout_minutes * 60,
            retries=0,
        )


# --------------------------------------------------------------------------- #
# 2. Apply GoldenGate CDC (I/U/D audit rows) to a current-state silver table.
# --------------------------------------------------------------------------- #
class CdcMergeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conn_id: str = DEFAULT_CONN_ID
    source_table: TableFqn = Field(description="Raw GoldenGate audit-log table")
    target_table: TableFqn = Field(description="Silver current-state table (created if missing)")
    primary_key: list[str] = Field(min_length=1)
    columns: list[str] = Field(min_length=1, description="Non-key columns to carry into silver")
    op_type_column: Ident = "OP_TYPE"
    op_ts_column: Ident = "OP_TS"
    tiebreak_column: str | None = Field(
        default=None, description="Orders changes with identical op_ts, e.g. GoldenGate POS/CSN"
    )
    delete_op_values: list[str] = ["D"]
    lookback_minutes: int = Field(default=60, ge=0, description="Re-read window behind the watermark for late rows")

    _idents = field_validator("primary_key", "columns", "tiebreak_column")(
        lambda v: _require(v, _IDENT_RE, "column name")
    )
    _literals = field_validator("delete_op_values")(lambda v: _require(v, _LITERAL_RE, "literal"))


class CdcMergeToSilver(Blueprint[CdcMergeConfig]):
    """Idempotent watermark MERGE of GoldenGate change rows into a silver table; publishes it as an Asset."""

    def render(self, config: CdcMergeConfig) -> TaskGroup:
        c = config
        keys, payload = c.primary_key, c.columns
        all_cols = [*keys, *payload]
        order_by = f"{c.op_ts_column} DESC" + (f", {c.tiebreak_column} DESC" if c.tiebreak_column else "")
        deletes = ", ".join(f"'{v}'" for v in c.delete_op_values)

        create_sql = (
            f"CREATE TABLE IF NOT EXISTS {c.target_table} AS "
            f"SELECT {', '.join(all_cols)}, {c.op_ts_column} AS _SRC_OP_TS, "
            f"CURRENT_TIMESTAMP()::TIMESTAMP_LTZ AS _LOADED_AT FROM {c.source_table} WHERE 1 = 0"
        )

        interval_end = jinja("data_interval_end.isoformat()")
        merge_sql = f"""
MERGE INTO {c.target_table} AS t
USING (
    SELECT {', '.join(all_cols)},
           {c.op_type_column} AS _SRC_OP_TYPE,
           {c.op_ts_column} AS _SRC_OP_TS
    FROM {c.source_table}
    WHERE {c.op_ts_column} > (
            SELECT COALESCE(DATEADD('minute', -{c.lookback_minutes}, MAX(_SRC_OP_TS)), '1900-01-01'::TIMESTAMP_NTZ)
            FROM {c.target_table})
      AND {c.op_ts_column} <= CONVERT_TIMEZONE('UTC', '{interval_end}'::TIMESTAMP_TZ)::TIMESTAMP_NTZ
    QUALIFY ROW_NUMBER() OVER (PARTITION BY {', '.join(keys)} ORDER BY {order_by}) = 1
) AS s
ON {' AND '.join(f't.{k} = s.{k}' for k in keys)}
WHEN MATCHED AND s._SRC_OP_TYPE IN ({deletes}) AND s._SRC_OP_TS >= t._SRC_OP_TS THEN DELETE
WHEN MATCHED AND s._SRC_OP_TYPE NOT IN ({deletes}) AND s._SRC_OP_TS >= t._SRC_OP_TS THEN UPDATE SET
    {', '.join(f't.{col} = s.{col}' for col in payload)},
    t._SRC_OP_TS = s._SRC_OP_TS,
    t._LOADED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED AND s._SRC_OP_TYPE NOT IN ({deletes}) THEN INSERT
    ({', '.join(all_cols)}, _SRC_OP_TS, _LOADED_AT)
    VALUES ({', '.join(f's.{col}' for col in all_cols)}, s._SRC_OP_TS, CURRENT_TIMESTAMP())
""".strip()

        with TaskGroup(group_id=self.step_id) as group:
            create = SQLExecuteQueryOperator(task_id="ensure_target", conn_id=c.conn_id, sql=create_sql)
            merge = SQLExecuteQueryOperator(
                task_id="merge_cdc",
                conn_id=c.conn_id,
                sql=merge_sql,
                outlets=[table_asset(c.target_table)],
            )
            create >> merge
        return group


# --------------------------------------------------------------------------- #
# 3. Declarative data-quality gate (fails the run -> nothing downstream publishes).
# --------------------------------------------------------------------------- #
class QualityGateConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conn_id: str = DEFAULT_CONN_ID
    table: TableFqn
    not_null: list[str] = []
    unique: list[str] = []
    min_values: dict[str, float] = Field(default={}, description="column -> inclusive minimum")
    accepted_values: dict[str, list[str]] = {}
    min_row_count: int = Field(default=1, ge=0)
    # Raw SQL predicates by design; they are reviewed in the PR like any other SQL.
    row_checks: dict[str, str] = Field(
        default={}, description="check_name -> boolean SQL predicate that must hold for every row"
    )

    _idents = field_validator("not_null", "unique", "min_values", "accepted_values", "row_checks")(
        lambda v: _require(v, _IDENT_RE, "column/check name")
    )
    _literals = field_validator("accepted_values")(
        lambda v: {k: _require(vals, _LITERAL_RE, "literal") for k, vals in v.items()}
    )


class QualityGate(Blueprint[QualityGateConfig]):
    """Column + row-level checks on a table using common-sql check operators."""

    def render(self, config: QualityGateConfig) -> TaskGroup:
        column_mapping: dict[str, dict[str, Any]] = {}
        for col in config.not_null:
            column_mapping.setdefault(col, {})["null_check"] = {"equal_to": 0}
        for col in config.unique:
            column_mapping.setdefault(col, {})["unique_check"] = {"equal_to": 0}
        for col, lo in config.min_values.items():
            column_mapping.setdefault(col, {})["min"] = {"geq_to": lo}

        checks: dict[str, dict[str, str]] = {}
        for col, values in config.accepted_values.items():
            allowed = ", ".join(f"'{v}'" for v in values)
            checks[f"{col.lower()}_accepted_values"] = {"check_statement": f"{col} IN ({allowed})"}
        for name, predicate in config.row_checks.items():
            checks[name] = {"check_statement": predicate}

        with TaskGroup(group_id=self.step_id) as group:
            SQLTableCheckOperator(
                task_id="row_count",
                conn_id=config.conn_id,
                table=f"(SELECT COUNT(*) AS N FROM {config.table}) rc",
                checks={"min_row_count": {"check_statement": f"N >= {config.min_row_count}"}},
                retries=0,
            )
            if column_mapping:
                SQLColumnCheckOperator(
                    task_id="column_checks",
                    conn_id=config.conn_id,
                    table=config.table,
                    column_mapping=column_mapping,
                    retries=0,
                )
            if checks:
                SQLTableCheckOperator(
                    task_id="row_checks", conn_id=config.conn_id, table=config.table, checks=checks, retries=0
                )
        return group


# --------------------------------------------------------------------------- #
# 4. Materialize a SQL model (silver -> gold) for the run's data interval.
# --------------------------------------------------------------------------- #
class SqlModelConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conn_id: str = DEFAULT_CONN_ID
    target_table: TableFqn
    sql_file: str = Field(
        pattern=r"^[a-z0-9_/]+\.sql$", description="SELECT statement, relative to include/sql/"
    )
    strategy: Literal["replace", "delete_insert"] = Field(
        default="delete_insert",
        description="replace: full rebuild; delete_insert: rewrite only the rows of this data interval",
    )
    partition_column: str | None = Field(
        default=None, description="Date/timestamp column scoping delete_insert to [data_interval_start, end)"
    )
    params: dict[str, str] = Field(default={}, description="Values exposed to the SQL file as params.<key>")

    _idents = field_validator("partition_column")(lambda v: _require(v, _IDENT_RE, "column name"))

    @model_validator(mode="after")
    def _partition_required(self) -> SqlModelConfig:
        if self.strategy == "delete_insert" and not self.partition_column:
            raise ValueError("delete_insert requires partition_column")
        return self


class SqlModel(Blueprint[SqlModelConfig]):
    """Runs a SELECT from include/sql and materializes it transactionally; publishes the target as an Asset."""

    def render(self, config: SqlModelConfig) -> SQLExecuteQueryOperator:
        c = config
        select = f"\n{jinja_include(c.sql_file)}\n"
        if c.strategy == "replace":
            statements = [f"CREATE OR REPLACE TABLE {c.target_table} AS {select}"]
        else:
            window = (
                f"{c.partition_column} >= '{jinja('data_interval_start | ds')}' "
                f"AND {c.partition_column} < '{jinja('data_interval_end | ds')}'"
            )
            # The hook runs non-autocommit and commits once at the end, so DELETE + INSERT
            # land atomically; the leading DDL commits on its own, which is harmless here.
            statements = [
                f"CREATE TABLE IF NOT EXISTS {c.target_table} AS SELECT * FROM ({select}) WHERE 1 = 0",
                f"DELETE FROM {c.target_table} WHERE {window}",
                f"INSERT INTO {c.target_table} SELECT * FROM ({select})",
            ]
        return SQLExecuteQueryOperator(
            task_id=self.step_id,
            conn_id=c.conn_id,
            sql=statements,
            params=c.params,
            outlets=[table_asset(c.target_table)],
            # The SQL file is only referenced by an include, so without this an edit to it
            # wouldn't change the serialized DAG: Airflow 3 wouldn't cut a new DAG version and
            # runs would keep executing the previous bundle's SQL.
            doc_md=f"```sql\n{(SQL_ROOT / c.sql_file).read_text()}\n```",
        )
