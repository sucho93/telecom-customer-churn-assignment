from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'customer_churn_elt_pipeline',
    default_args=default_args,
    description='Customer Churn ELT Pipeline',
    schedule_interval='@hourly', # or cron
    catchup=False,
)

def ingest_callable(**kwargs):
    subprocess.run(['python', '/scripts/ingest_data.py'])

def transform_callable(**kwargs):
    subprocess.run(['python', '/scripts/transform.py'])

t1 = PythonOperator(
    task_id='ingest_raw_csv',
    python_callable=ingest_callable,
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform_and_load',
    python_callable=transform_callable,
    dag=dag,
)

t1 >> t2
