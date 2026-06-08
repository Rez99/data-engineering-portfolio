from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="lakehouse_3_transform",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    run_fact_session = BashOperator(
        task_id="run_fact_session",
        bash_command="""
        cd /opt/airflow/lakehouse &&
        dbt run --select +fact_session
        """,
    )