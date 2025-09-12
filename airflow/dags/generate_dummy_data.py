from datetime import datetime, timedelta
from airflow.sdk import DAG, chain
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
# from scripts.notification import discord_notification

default_args = {
    "owner": "airflow",
    "depends_on_past": True
}

with DAG(
    dag_id="jcdeol005_final_project",
    default_args=default_args,
    description="end-to-end data pipeline to BigQuery",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["jcdeol005", "final_project", "etl"]
) as dag:
    #create a start task
    start = EmptyOperator(task_id="start")
    
    #create a task to generate dummy dataset
    data_source_generate = BashOperator(
        task_id = "data_source_generate",
        bash_command = "cd /scripts/generate_data_source && python generate_data_source.py",
        # on_failure_callback=discord_notification
    )

    data_source_ingestion = BashOperator(
        task_id = "data_source_ingestion",
        bash_command = "cd /scripts/generate_data_source && python gcs_data_ingestion.py",
        # on_failure_callback=discord_notification
    )

    #create an end task
    end = EmptyOperator(task_id="end")

    #upload the dummy dataset to Google Cloud Storage

    chain(start, data_source_generate, data_source_ingestion, end)