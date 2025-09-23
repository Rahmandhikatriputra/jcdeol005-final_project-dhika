from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import date
# from scripts.notification import discord_notification

default_args = {
    "owner": "airflow",
    "depends_on_past": True
}

file_name = f"data_source_{date.today()}.csv"

with DAG(
    dag_id="etl_process",
    default_args=default_args,
    description="end-to-end data pipeline to BigQuery",
    schedule="@daily",
    start_date = datetime(2023, 6, 26),
    end_date = datetime(2023, 8, 31),
    catchup=True,
    max_active_runs=1,
    tags=["jcdeol005", "final_project", "etl"]
) as dag:
    #create a start task
    start = EmptyOperator(task_id="start")
    
    #create a task to generate dummy dataset
    create_table = BashOperator(
        task_id = "create_table",
        bash_command = "cd /opt/airflow/dags/scripts/load && python create_table.py",
        # on_failure_callback=discord_notification
    )
    
    insert_raw_data = BashOperator(
        task_id = "insert_raw_data",
        bash_command = "cd /opt/airflow/dags/scripts/load && python insert_raw_data.py --file_name $FILE_NAME",
        append_env = True,
        env = {
            "FILE_NAME" : "data_source_{{ ds }}.csv"
        }
        # on_failure_callback=discord_notification
    )
    
    transform_process = BashOperator(
        task_id = "transform_process",
        bash_command = "cd /opt/airflow/dbt/final_project && dbt run --profiles-dir /opt/airflow/dbt/final_project/.dbt"
    )

    #create an end task
    end = EmptyOperator(task_id="end")

    #upload the dummy dataset to Google Cloud Storage

    chain(start, 
          create_table, 
          insert_raw_data, 
          transform_process, 
          end)