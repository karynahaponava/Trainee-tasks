from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import boto3
import os
import glob

S3_BUCKET = "helsinki-bikes-data"
S3_ENDPOINT = "http://localstack:4566"
AWS_ID = "test"
AWS_KEY = "test"

SPARK_SCRIPT = "/opt/airflow/scripts/spark_metrics.py"
INPUT_DATA = "/opt/airflow/data/monthly_data/bikes_2020-05.csv"
OUTPUT_DIR = "/opt/airflow/data/metrics_output"

default_args = {
    'owner': 'admin',
    'start_date': days_ago(1),
}

def upload_to_s3():
    s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT, aws_access_key_id=AWS_ID, aws_secret_access_key=AWS_KEY)
    
    folders_to_upload = ["departures_agg", "returns_agg", "daily_metrics"]
    
    for folder in folders_to_upload:
        local_folder = os.path.join(OUTPUT_DIR, folder)
        files = glob.glob(f"{local_folder}/*.csv")
        
        if files:
            file_path = files[0]
            s3_key = f"metrics/{folder}.csv"
            print(f"Загружаю {file_path} -> {s3_key}")
            s3.upload_file(file_path, S3_BUCKET, s3_key)
        else:
            print(f"Файл в папке {folder} пока не найден.")

with DAG('02_spark_process', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    run_spark = BashOperator(
        task_id='run_spark_script',
        bash_command=f'python {SPARK_SCRIPT} {INPUT_DATA} {OUTPUT_DIR}'
    )

    upload_task = PythonOperator(
        task_id='upload_metrics',
        python_callable=upload_to_s3
    )

    run_spark >> upload_task