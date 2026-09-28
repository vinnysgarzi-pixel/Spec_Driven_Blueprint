"""Demo-only, read-only: report where the Snowflake connection can create ADW schemas.

Walks the role hierarchy (SHOW GRANTS) of the connection's role and of every
role granted to its user, and reports databases with CREATE SCHEMA / OWNERSHIP
and schemas with CREATE TABLE / OWNERSHIP. Result is returned as XCom.
"""

from contextlib import closing
from pathlib import Path

import yaml
from airflow.sdk import dag, task

CONN_ID = yaml.safe_load((Path(__file__).parents[1] / "blueprint.vars.yaml").read_text())["vars"]["snowflake_conn_id"]


@dag(dag_id="demo__snowflake_access_probe", schedule=None, tags=["demo"], default_args={"owner": "platform"}, doc_md=__doc__)
def snowflake_access_probe():
    @task(retries=0)
    def probe() -> dict:
        """List databases/schemas where the connection's roles can create ADW objects."""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from snowflake.connector import DictCursor

        hook = SnowflakeHook(snowflake_conn_id=CONN_ID)
        with closing(hook.get_conn()) as conn, closing(conn.cursor(DictCursor)) as cur:

            def q(sql: str) -> list[dict]:
                try:
                    cur.execute(sql)
                    return cur.fetchall()
                except Exception as e:
                    print(f"skipped {sql!r}: {e}")
                    return []

            ctx = q("SELECT CURRENT_USER() AS U, CURRENT_ROLE() AS R, CURRENT_DATABASE() AS D, CURRENT_SCHEMA() AS S")[0]
            print(f"session: {ctx}")

            def effective(role: str) -> dict:
                seen, todo, grants = [], [role], []
                while todo:
                    r = todo.pop()
                    if r in seen:
                        continue
                    seen.append(r)
                    for g in q(f'SHOW GRANTS TO ROLE "{r}"'):
                        grants.append(g)
                        if g["granted_on"] == "ROLE" and g["privilege"] == "USAGE":
                            todo.append(g["name"])
                return {
                    "inherits": seen[1:],
                    "can_create_schema_in": sorted(
                        {g["name"] for g in grants if g["name"] and g["granted_on"] == "DATABASE" and g["privilege"] in ("CREATE SCHEMA", "OWNERSHIP")}
                    ),
                    "can_create_table_in": sorted(
                        {g["name"] for g in grants if g["name"] and g["granted_on"] == "SCHEMA" and g["privilege"] in ("CREATE TABLE", "OWNERSHIP")}
                    ),
                }

            user_roles = sorted({g["role"] for g in q(f'SHOW GRANTS TO USER "{ctx["U"]}"') if g.get("role")})
            report = {
                "user": ctx["U"],
                "primary_role": ctx["R"],
                "default_database": ctx["D"],
                "default_schema": ctx["S"],
                "visible_databases": sorted(d["name"] for d in q("SHOW DATABASES") if d.get("name")),
                "roles_granted_to_user": user_roles,
                "by_role": {r: effective(r) for r in sorted({ctx["R"], *user_roles} - {None})},
            }
        for k, v in report.items():
            print(f"{k}: {v}")
        return report

    probe()


snowflake_access_probe()
