from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.extraction import fetch_all_job_data
from scripts.transformation import extract_transform
from scripts.loading import load_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your.email@example.com'],  # Replace with your email
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'job_data_etl',
    default_args=default_args,
    description='ETL pipeline to extract job data from API and load into PostgreSQL',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['job-data', 'etl', 'production'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_job_data',
        python_callable=fetch_all_job_data,
    )

    transform_task = PythonOperator(
        task_id='transform_job_data',
        python_callable=extract_transform,
        op_args=[fetch_task.output],
    )

    load_task = PythonOperator(
        task_id='load_job_data',
        python_callable=load_data,
        op_args=[transform_task.output],
    )

    fetch_task >> transform_task >> load_task
