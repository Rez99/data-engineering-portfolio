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

    stg_2019_oct = BashOperator(
        task_id="stg_2019_oct",
        bash_command="""
        cd /opt/airflow/lakehouse &&
        dbt run --select stg-2019-Oct
        """,
    )

    int_session = BashOperator(
        task_id="int_session",
        bash_command="""
        cd /opt/airflow/lakehouse &&
        dbt run --select int_session
        """,
    )

    mart_session = BashOperator(
        task_id="mart_session",
        bash_command="""
        cd /opt/airflow/lakehouse &&
        dbt run --select mart_session
        """,
    )

    stg_2019_oct >> int_session >> mart_session