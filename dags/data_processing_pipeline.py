from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
# Импортируем TriggerDagRunOperator вместо Dataset
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime
import pandas as pd
import os
import re

# --- КОНСТАНТЫ ---
DATA_PATH = '/opt/airflow/data'
SOURCE_FILE = f'{DATA_PATH}/tiktok_google_play_reviews.csv'
PROCESSED_FILE = f'{DATA_PATH}/processed_data.csv'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'catchup': False
}

# ==========================================
# DAG 1: ОБРАБОТКА ДАННЫХ
# ==========================================
with DAG(
    dag_id='1_process_data_dag',
    default_args=default_args,
    schedule_interval=None,
    tags=['homework']
) as dag1:

    # 1. Сенсор: ждет появления файла
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath=SOURCE_FILE,
        poke_interval=10,
        timeout=600
    )

    def check_file_content(**kwargs):
        try:
            df = pd.read_csv(SOURCE_FILE)
            if df.empty:
                return 'file_is_empty'
            return 'processing_group.clean_nulls'
        except Exception:
            return 'file_is_empty'

    # 2. Ветвление
    branching = BranchPythonOperator(
        task_id='check_if_empty',
        python_callable=check_file_content
    )

    # 3.1 Ветка "Файл пустой"
    empty_file_task = BashOperator(
        task_id='file_is_empty',
        bash_command='echo "Файл пустой! Завершаем работу."'
    )

    # 3.2 Ветка "Обработка" (TaskGroup)
    with TaskGroup('processing_group') as processing_group:
        
        def replace_nulls():
            df = pd.read_csv(SOURCE_FILE)
            df.fillna('-', inplace=True)
            df.to_csv(f'{DATA_PATH}/temp_step1.csv', index=False)

        t1_clean_nulls = PythonOperator(
            task_id='clean_nulls',
            python_callable=replace_nulls
        )

        def sort_data():
            df = pd.read_csv(f'{DATA_PATH}/temp_step1.csv')
            if 'created_date' in df.columns:
                df.sort_values(by='created_date', inplace=True)
            df.to_csv(f'{DATA_PATH}/temp_step2.csv', index=False)

        t2_sort = PythonOperator(
            task_id='sort_by_date',
            python_callable=sort_data
        )

        def clean_text():
            df = pd.read_csv(f'{DATA_PATH}/temp_step2.csv')
            def remove_special_chars(text):
                if not isinstance(text, str): return text
                return re.sub(r'[^\w\s.,?!-]', '', text)

            if 'content' in df.columns:
                df['content'] = df['content'].apply(remove_special_chars)
            
            df.to_csv(PROCESSED_FILE, index=False)

        t3_remove_chars = PythonOperator(
            task_id='remove_emoji',
            python_callable=clean_text
            # Убрали outlets=[DATASET_SIGNAL], так как Dataset не поддерживается
        )

        t1_clean_nulls >> t2_sort >> t3_remove_chars

    # 4. ТРИГГЕР ВТОРОГО ДАГА (Вместо Dataset)
    trigger_next_dag = TriggerDagRunOperator(
        task_id='trigger_mongo_load',
        trigger_dag_id='2_load_to_mongo_dag', # ID второго дага
        wait_for_completion=False # Не ждать выполнения, просто запустить
    )

    # Строй цепочку: Сначала обработка -> потом запуск второго дага
    wait_for_file >> branching
    branching >> empty_file_task
    branching >> processing_group >> trigger_next_dag


# ==========================================
# DAG 2: ЗАГРУЗКА В MONGODB
# ==========================================
with DAG(
    dag_id='2_load_to_mongo_dag',
    default_args=default_args,
    schedule_interval=None, # Запускается только по команде от первого DAG
    tags=['homework']
) as dag2:

    def upload_to_mongo():
        # Читаем обработанный файл
        df = pd.read_csv(PROCESSED_FILE)
        data_dict = df.to_dict('records')

        hook = MongoHook(conn_id='mongo_default')
        # Вставляем данные
        hook.insert_many(mongo_collection='posts', docs=data_dict, mongo_db='airflow_db')
        print(f"Inserted {len(data_dict)} records into MongoDB")

    load_task = PythonOperator(
        task_id='load_to_mongo',
        python_callable=upload_to_mongo
    )

    load_task