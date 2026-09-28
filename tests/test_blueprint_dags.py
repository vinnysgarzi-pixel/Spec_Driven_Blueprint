"""Guardrails the agent must satisfy: DAGs build, follow platform conventions, and trace back to their specs."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from blueprint import build_all_airflow_dags

ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = ROOT / "dags"
SPECS = sorted((ROOT / "specs").rglob("*.spec.yaml"))


def build(profile: str) -> dict:
    return {d.dag_id: d for d in build_all_airflow_dags(search_path=DAGS_DIR, register_globals={}, profile=profile)}


@pytest.fixture(scope="module")
def dags() -> dict:
    return build("prod")


@pytest.mark.parametrize("profile", ["dev", "prod"])
def test_all_dags_build(profile):
    assert build(profile)


def test_conventions(dags):
    for dag in dags.values():
        assert any(t.startswith("domain:") for t in dag.tags), dag.dag_id
        assert dag.max_active_runs == 1, dag.dag_id
        for task in dag.tasks:
            assert task.owner not in ("airflow", ""), task.task_id
            assert task.execution_timeout is not None, task.task_id


def test_blueprint_ui_fields_are_valid_jinja(dags):
    # Blueprint templates the step config and the Blueprint's source into every task;
    # a stray Jinja delimiter there fails the task at runtime, not at parse time.
    for dag in dags.values():
        env = dag.get_template_env()
        for task in dag.tasks:
            for field in ("blueprint_step_config", "blueprint_step_code", "blueprint_dag_args"):
                env.from_string(getattr(task, field))


@pytest.mark.parametrize("spec_path", SPECS, ids=lambda p: p.stem)
def test_spec_is_implemented(spec_path, dags):
    spec = yaml.safe_load(spec_path.read_text())
    dag_id = f"{spec['domain']}__{spec['data_product']}"
    assert dag_id in dags, f"{spec_path.name} has no DAG {dag_id}"
    dag = dags[dag_id]
    assert str(spec_path.relative_to(ROOT)) in dag.doc_md

    published = {o.name for t in dag.tasks for o in getattr(t, "outlets", [])}
    for layer in spec["outputs"].values():
        for out in layer:
            assert f"ADW.{out['table']}" in published, f"{out['table']} is not produced by {dag_id}"


def render_sql(dag, task_id: str) -> str:
    task = dag.get_task(task_id)
    assert isinstance(task, SQLExecuteQueryOperator)
    env = dag.get_template_env()
    ctx = {
        "data_interval_start": datetime(2026, 9, 27, 10, tzinfo=timezone.utc),
        "data_interval_end": datetime(2026, 9, 28, 10, tzinfo=timezone.utc),
        "params": task.params,
    }
    stmts = task.sql if isinstance(task.sql, list) else [task.sql]
    return ";\n".join(env.from_string(s).render(**ctx) for s in stmts)


def test_gold_sql_is_windowed_and_fully_rendered(dags):
    sql = render_sql(dags["sales__oms_orders"], "gold_daily_order_revenue")
    assert "{{" not in sql and "{%" not in sql
    assert "DELETE FROM ADW.GOLD_SALES.DAILY_ORDER_REVENUE WHERE ORDER_DATE >= '2026-09-27' AND ORDER_DATE < '2026-09-28'" in sql
    assert "FROM ADW.SILVER_OMS.ORDERS" in sql


def test_cdc_merge_is_bounded_by_interval_end(dags):
    sql = render_sql(dags["sales__oms_orders"], "silver_orders.merge_cdc")
    assert "OP_TS <= CONVERT_TIMEZONE('UTC', '2026-09-28T10:00:00+00:00'::TIMESTAMP_TZ)" in sql
    assert "PARTITION BY ORDER_ID ORDER BY OP_TS DESC, POS DESC" in sql


def test_dev_profile_targets_dev_database():
    sql = render_sql(build("dev")["sales__oms_orders"], "gold_daily_order_revenue")
    assert "ADW_DEV.GOLD_SALES.DAILY_ORDER_REVENUE" in sql and " ADW.GOLD" not in sql
