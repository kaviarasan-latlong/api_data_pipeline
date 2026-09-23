"""
api_dag.py

Airflow DAG that triggers the latlong extraction pipeline on a schedule.
The DAG itself does no logic - all orchestration (windowing, chunking,
resume-from-watermark) lives in main.py so the pipeline behaves the same
whether it's run by Airflow or by hand for a backfill/debug run.

Adjust `schedule` to match how often you want the 3-day rolling window
job to run (e.g. daily catches up incrementally; every 3 days runs
windows back-to-back with no overlap).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PIPELINE_DIR = "/opt/airflow/pipelines/latlong_pipeline"  # adjust to actual deploy path

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="latlong_extraction_pipeline",
    default_args=default_args,
    description="Extracts lat/long + geo-enriches request/response logs on a rolling window",
    schedule="0 2 * * *",  # daily at 02:00 - main.py's watermark decides the actual window
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # never overlap runs against the same watermark row
    tags=["latlong", "geo", "etl"],
) as dag:

    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command=f"bash {PIPELINE_DIR}/run_pipeline.sh",
        env={
            "DB_HOST": "{{ var.value.latlong_db_host }}",
            "DB_NAME": "{{ var.value.latlong_db_name }}",
            "DB_USER": "{{ var.value.latlong_db_user }}",
            "DB_PASSWORD": "{{ var.value.latlong_db_password }}",
            "TEAMS_WEBHOOK_URL": "{{ var.value.latlong_teams_webhook_url }}",
        },
    )
