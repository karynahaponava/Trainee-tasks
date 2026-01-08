from datetime import datetime, timedelta
import os
import glob
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from botocore.exceptions import ClientError

AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = "helsinki-bikes-data"
LOCAL_DATA_PATH = "/opt/airflow/data/monthly_data"

class S3Uploader:
    """
    Класс для работы с S3 (LocalStack).
    """
    def __init__(self):
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=AWS_ENDPOINT_URL,
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name="us-east-1"
        )

    def create_bucket_if_not_exists(self, bucket_name: str):
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            print(f"Бакет {bucket_name} уже существует.")
        except ClientError:
            print(f"Бакет {bucket_name} не найден. Создаем...")
            self.s3_client.create_bucket(Bucket=bucket_name)

    def upload_file(self, file_path: str, bucket_name: str):
        file_name = os.path.basename(file_path)
        object_name = f"raw/{file_name}"
        
        try:
            self.s3_client.upload_file(file_path, bucket_name, object_name)
            print(f"Загружен: {file_name}")
        except ClientError as e:
            print(f"Ошибка загрузки {file_name}: {e}")

def task_flow():
    uploader = S3Uploader()
    uploader.create_bucket_if_not_exists(BUCKET_NAME)
    
    # Ищем CSV файлы
    files = glob.glob(f"{LOCAL_DATA_PATH}/*.csv")
    print(f"Найдено файлов: {len(files)}")
    
    if not files:
        print(f"ВАЖНО: Файлы не найдены в {LOCAL_DATA_PATH}. Проверьте папку data/monthly_data.")
    
    # Загружаем файлы
    for file_path in files:
        uploader.upload_file(file_path, BUCKET_NAME)

default_args = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='01_upload_raw_data_to_s3',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval='@once',
    catchup=False,
    tags=['localstack', 'etl']
) as dag:

    upload_op = PythonOperator(
        task_id='upload_csvs',
        python_callable=task_flow
    )