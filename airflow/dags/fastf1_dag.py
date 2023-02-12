import os
import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
FASTF1_VENV_NAME = "fastf1-venv"
YEAR = "{{ execution_date.strftime('%Y') }}"

with DAG(
    dag_id="fastf1_extract_dag",
    description="Extracts data from fastf1 library",
    start_date=datetime(2018, 0, 0),
    end_date=datetime(2022, 12, 31),
    schedule_interval="@yearly",
    max_active_runs=1,
    catchup=True,
    owner="airflow",
    retries=1
) as dag:

    begin_task = EmptyOperator(
        task_id="begin dag"
    )

    extract_task = BashOperator(
        task_id="extract fastf1 data",
        bash_command=f"cd {AIRFLOW_HOME} && source {FASTF1_VENV_NAME}/bin/activate && python extract/fastf1_extract.py {YEAR}"
    )

    end_task = EmptyOperator(
        task_id="end dag"
    )

    begin_task >> extract_task >> end_task