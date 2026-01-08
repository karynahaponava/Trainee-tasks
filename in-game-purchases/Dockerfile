FROM apache/airflow:2.7.1-python3.10

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install faker pandas boto3 s3fs pyspark==3.5.0 apache-airflow-providers-amazon apache-airflow-providers-postgres
RUN pip install faker pandas boto3 s3fs pyspark==3.5.0 apache-airflow-providers-amazon apache-airflow-providers-postgres pytest

