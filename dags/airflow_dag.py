from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from error_handling import handle_errors
from data_ingestion import ingest_data
from data_transformation import transform_data
from store_data import store_data_in_postgresql

# Define DAG parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'advertising_pipeline',
    default_args=default_args,
    description='Pipeline for processing advertising data',
    schedule_interval='@daily',
)

# Define tasks
ingest_data_task = PythonOperator(
    task_id='ingest_data',
    python_callable=ingest_data,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

store_data_task = PythonOperator(
    task_id='store_data',
    python_callable=store_data_in_postgresql,
    dag=dag,
)

# Define task dependencies
ingest_data_task >> transform_data_task >> store_data_task
