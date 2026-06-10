from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)


with DAG(
    dag_id="lakehouse_0_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    extract = TriggerDagRunOperator(
        task_id="extract",
        trigger_dag_id="lakehouse_1_extract",
        wait_for_completion=True,
        poke_interval=5,
    )

    load = TriggerDagRunOperator(
        task_id="load",
        trigger_dag_id="lakehouse_2_load",
        wait_for_completion=True,
        poke_interval=5,
    )

    transform = TriggerDagRunOperator(
        task_id="transform",
        trigger_dag_id="lakehouse_3_transform",
        wait_for_completion=True,
        poke_interval=10,
    )

    ml = TriggerDagRunOperator(
        task_id="ml",
        trigger_dag_id="lakehouse_4_ml",
        wait_for_completion=True,
        poke_interval=10,
    )

    extract >> load >> transform >> ml