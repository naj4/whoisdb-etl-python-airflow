from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from whois_etl import run_whois_etl

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
    run_etl = PythonOperator(
    task_id='complete_twitter_etl',
    python_callable=run_whois_etl
    )

    run_etl