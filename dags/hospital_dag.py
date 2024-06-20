from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from etl_script import extract_transform, load_s3, load_redshift

default_args = {
    'owner': 'hospital_admissions',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG('hosp_pipeline', default_args=default_args, schedule_interval=None) as dag:

    extract_transform_task = PythonOperator(
        task_id='extract_transform_data',
        python_callable=extract_transform
    )

    load_s3_task = PythonOperator(
        task_id='load_s3',
        python_callable=load_s3
    )

    load_redshift_task = PythonOperator(
        task_id='load_redshift',
        python_callable=load_redshift
    )

    extract_transform_task >> load_s3_task >> load_redshift_task