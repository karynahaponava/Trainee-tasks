import os
import boto3
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger("airflow.task")

FILENAME = os.environ.get("SOURCE_FILENAME", "data/database.csv")
BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "helsinki-bikes-data")


def upload_source_data():
    """
    Загружает исходный файл в S3 бакет.
    """
    file_path = f"/opt/airflow/{FILENAME}"

    if not os.path.exists(file_path):
        if not os.path.exists(FILENAME):
            raise FileNotFoundError(f"File {file_path} not found")
        file_path = FILENAME

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL", "http://localstack:4566"),
    )

    logger.info(f"Starting upload of {file_path} to s3://{BUCKET_NAME}...")
    try:
        s3.upload_file(file_path, BUCKET_NAME, "data/database.csv")
        logger.info("Upload complete successfully.")
    except Exception as e:
        logger.error(f"Failed to upload file: {e}")
        raise e


with DAG(
    "01_upload_to_s3",
    start_date=days_ago(1),
    schedule_interval="@monthly",
    catchup=False,
    tags=["s3", "upload"],
) as dag:

    upload_task = PythonOperator(
        task_id="upload_data", python_callable=upload_source_data
    )
