import os

from blueprint import build_all_airflow_dags

build_all_airflow_dags(profile=os.getenv("ADW_ENV", "dev"))
