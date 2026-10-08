# Live demo assets

Used by the "build a new pipeline" part of the demo. Nothing here is loaded by Airflow or the tests.

- `oms_customers.spec.yaml`: the new spec the analyst "hands over". On stage, copy it to `specs/sales/`.
- `reference/`: a known-good implementation of that spec, as a fallback if the agent stalls on stage:
  - `reference/oms_customers.dag.yaml` → `dags/domains/sales/oms_customers.dag.yaml`
  - `reference/daily_new_customers.sql` → `dags/sql/sales/daily_new_customers.sql`

The raw table `RAW_OMS.CUSTOMERS` is created by the `demo__seed_raw_oms` DAG.
