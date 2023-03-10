import os
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
YEAR = "{{ execution_date.strftime('%Y') }}"

default_args = {
    "owner": "airflow",
    "retries": 1
}

with DAG(
    dag_id="fastf1_extract",
    description="Extracts data from fastf1 library. Sends to postgres database",
    default_args=default_args,
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2022, 12, 31),
    schedule_interval="@yearly",
    max_active_runs=1,
    catchup=True,
) as dag:

    begin_task = EmptyOperator(
        task_id="begin_dag"
    )

    extract_task = BashOperator(
        task_id="extract_fastf1_data",
        bash_command=f"cd {AIRFLOW_HOME} && python extract/fastf1_extract.py {YEAR} airflow airflow"
    )

    end_task = EmptyOperator(
        task_id="end_dag"
    )

    begin_task >> extract_task >> end_task