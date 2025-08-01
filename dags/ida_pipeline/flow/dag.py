from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from ida_pipeline.tasks.landing_ida_task import LandingIDATask
from ida_pipeline.tasks.raw_ida_task import RawIDATask

default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="ida_pipeline",
    description="ETL dos dados de IDA (Índice de Desempenho no Atendimento)",
    default_args=default_args,
    schedule_interval="0 4 * * *",  # todo dia às 4h
    catchup=False,
    max_active_runs=1,
    tags=["telecom", "ida", "etl"],
) as dag:

    start = EmptyOperator(task_id="start")

    extract_ida = extract_ida = LandingIDATask(task_id="extract_ida")

    raw_ida = RawIDATask(task_id="raw_ida")

    end = EmptyOperator(task_id="end")

    start >> extract_ida >> raw_ida >> end
