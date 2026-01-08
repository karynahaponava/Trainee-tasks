import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

def get_spark_session():
    aws_key = os.getenv("AWS_ACCESS_KEY_ID", "test")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY", "test")

    spark = SparkSession.builder \
        .appName("StorefrontETL") \
        .master("local[*]") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
        .config("spark.hadoop.fs.s3a.access.key", aws_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", 
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "org.postgresql:postgresql:42.5.4") \
        .getOrCreate()
    return spark

def extract_data(spark):
    df = spark.read.json("s3a://gcomm-raw-data/raw/*.json")
    return df

def transform_data(df):
    from pyspark.sql.functions import col, to_timestamp

    transformed_df = df.withColumn("purchase_timestamp", to_timestamp(col("timestamp"))) \
                       .withColumn("total_amount", (col("price") * col("quantity")).cast("decimal(10,2)")) \
                       .withColumnRenamed("event_id", "purchase_id") \
                       .select(
                           "purchase_id",
                           "user_id",
                           "game_id",
                           "product_id",
                           "purchase_timestamp",
                           "quantity",
                           "total_amount"
                       )
    return transformed_df

def load_data(df):
    db_user = os.getenv("POSTGRES_USER", "admin")

    jdbc_url = "jdbc:postgresql://gcomm-postgres:5432/gcomm_db"
    
    properties = {
        "user": "admin",
        "password": "admin123",
        "driver": "org.postgresql.Driver"
    }
    
    df.write.jdbc(url=jdbc_url, table="storefront.stg_purchases", mode="append", properties=properties)

if __name__ == "__main__":
    spark = get_spark_session()
    
    print(">>> 1. Extracting from S3...")
    df_raw = extract_data(spark)
    
    print(">>> 2. Transforming...")
    df_clean = transform_data(df_raw)
    
    print(">>> 3. Loading to Postgres Staging...")
    load_data(df_clean)
    
    print(">>> ETL Job Completed Successfully!")
    spark.stop()