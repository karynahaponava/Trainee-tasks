from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'storefront_sales_pipeline',
    default_args=default_args,
    schedule_interval=None, 
    catchup=False
) as dag:

    generate_data = BashOperator(
        task_id='generate_data',
        bash_command='python /opt/airflow/etl/main.py' 
    )

    run_spark_etl = BashOperator(
        task_id='run_spark_etl',
        bash_command='python /opt/airflow/etl/etl_job.py'
    )

    process_warehouse = PostgresOperator(
        task_id='process_warehouse',
        postgres_conn_id='gcomm_postgres',
        sql='process_staging.sql'
    )

    generate_data >> run_spark_etl >> process_warehouse