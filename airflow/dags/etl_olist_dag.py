# airflow/dags/etl_olist_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'etl_user',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_olist_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['olist','etl']
) as dag:

    run_extract_transform = BashOperator(
        task_id='extract_transform',
        bash_command='python /opt/airflow/dags/../../scripts/01_extract_transform.py '
    )

    run_load_postgres = BashOperator(
        task_id='load_postgres',
        bash_command='python /opt/airflow/dags/../../scripts/02_load_postgres.py '
    )

    run_extract_transform >> run_load_postgres
