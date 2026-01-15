import os
import logging
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger("airflow.task")

BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "helsinki-bikes-data")


def send_alert_on_failure(context):
    """
    Эта функция запускается автоматически, если таска упала.
    Здесь должна быть отправка в Slack/Telegram/Email.
    """
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    error_msg = str(context.get("exception"))

    logger.error(
        f"""
    CRITICAL ALERT
    DAG: {dag_id}
    TASK: {task_id} FAILED
    ERROR: {error_msg}
    Sending notification to DevOps team...
    """
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_alert_on_failure,
}

with DAG(
    "02_spark_process",
    default_args=default_args,
    description="Run Spark job to process Helsinki City Bikes data",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["spark", "localstack"],
) as dag:

    run_spark = BashOperator(
        task_id="run_spark_metrics",
        bash_command=(
            f"python /opt/airflow/scripts/spark_metrics.py "
            f"s3a://{BUCKET_NAME}/data/database.csv "
            f"{BUCKET_NAME}"
        ),
    )

    run_spark
