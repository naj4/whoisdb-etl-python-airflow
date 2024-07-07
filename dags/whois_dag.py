from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from whois_etl import extract_files
from unzip import unzip_gz_files
from crud_postgres import load_files_to_db


default_args = {
    'owner':'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'whois_dag',
    default_args = default_args,
    description= 'Who Is db ETL',
    start_date = datetime(2024, 5, 30, 22),
    schedule_interval = '@daily'
) as dag:
    
    task_extract_files = PythonOperator(
    task_id='extract_files',
    python_callable=extract_files
    )

    task_unzip_files = PythonOperator(
    task_id='unzip_files',
    python_callable=unzip_gz_files
    )    

    task_load_files_to_db = PythonOperator(
    task_id='load_files_to_db',
    python_callable=load_files_to_db
    )  

    task_extract_files >> task_unzip_files >> task_load_files_to_db