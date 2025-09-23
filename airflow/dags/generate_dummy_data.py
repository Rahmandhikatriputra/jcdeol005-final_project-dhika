from datetime import datetime
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
# from scripts.notification import discord_notification

default_args = {
    "owner": "airflow",
    "depends_on_past": False
}

with DAG(
    dag_id="generate_dummy_data",
    default_args=default_args,
    description="end-to-end data pipeline to BigQuery",
    schedule="@daily",
    start_date = datetime(2023, 1, 11),
    end_date = datetime(2025, 8, 31),
    catchup=True,
    max_active_runs=1,
    tags=["jcdeol005", "final_project", "etl"]  
) as dag:
    #create a start task
    start = EmptyOperator(task_id="start")

    # create a task to generate dummy dataset
    data_source_generate = BashOperator(
        task_id = "data_source_generate",
        bash_command = "cd /opt/airflow/dags/scripts/generate_data_source && python generate_data_source.py --date {{ ds }}",
        # on_failure_callback=discord_notification
    )
    
    data_source_ingestion = BashOperator(
        task_id = "data_source_ingestion",
        bash_command = "cd /opt/airflow/dags/scripts/generate_data_source && python gcs_data_ingestion.py --file_name $FILE_NAME",
        append_env = True,
        env = {
            "FILE_NAME" : "data_source_{{ ds }}.csv"
        }
        # on_failure_callback=discord_notification
    )

    delete_files = BashOperator(
        task_id = "delete_files",
        bash_command = f"rm /opt/airflow/dags/scripts/generate_data_source/$FILE_NAME",
        append_env = True,
        env = {
            "FILE_NAME" : "data_source_{{ ds }}.csv"
        }
        # on_failure_callback=discord_notification
    )
    #create an end task
    end = EmptyOperator(task_id="end")

    #upload the dummy dataset to Google Cloud Storage
    

    chain(start, 
          data_source_generate, 
          data_source_ingestion, 
          delete_files, 
          end)
          