from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from helpers.config import SOURCE_FILE
from src.etl_logic import (
    check_file_content,
    replace_nulls,
    sort_data,
    clean_text,
    upload_to_mongo
)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'catchup': False
}

# DAG 1
with DAG(
    dag_id='1_process_data_dag',
    default_args=default_args,
    schedule_interval=None,
    tags=['homework']
) as dag1:

    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath=SOURCE_FILE,
        poke_interval=10,
        timeout=600
    )

    branching = BranchPythonOperator(
        task_id='check_if_empty',
        python_callable=check_file_content
    )

    empty_file_task = BashOperator(
        task_id='file_is_empty',
        bash_command='echo "Файл пустой! Завершаем работу."'
    )

    with TaskGroup('processing_group') as processing_group:
        
        t1 = PythonOperator(
            task_id='clean_nulls',
            python_callable=replace_nulls
        )

        t2 = PythonOperator(
            task_id='sort_by_date',
            python_callable=sort_data
        )

        t3 = PythonOperator(
            task_id='remove_emoji',
            python_callable=clean_text
        )

        t1 >> t2 >> t3

    trigger_next_dag = TriggerDagRunOperator(
        task_id='trigger_mongo_load',
        trigger_dag_id='2_load_to_mongo_dag', 
        wait_for_completion=False
    )

    wait_for_file >> branching
    branching >> empty_file_task
    branching >> processing_group >> trigger_next_dag


#DAG 2
with DAG(
    dag_id='2_load_to_mongo_dag',
    default_args=default_args,
    schedule_interval=None, 
    tags=['homework']
) as dag2:

    load_task = PythonOperator(
        task_id='load_to_mongo',
        python_callable=upload_to_mongo
    )

    load_task