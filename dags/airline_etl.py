from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

default_args = {
    'owner': 'student',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

with DAG('airline_snowflake_elt', 
         default_args=default_args, 
         schedule_interval=None, 
         catchup=False) as dag:

    run_processing = SnowflakeOperator(
        task_id='run_snowflake_procedure',
        snowflake_conn_id='snowflake_default',
        sql="CALL AIRLINE_DWH.PROCESSED.PROCESS_FLIGHTS();",
    )

    run_processing