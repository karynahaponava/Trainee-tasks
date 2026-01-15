import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format, avg, count
from pyspark.sql.functions import when

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    if len(sys.argv) < 3:
        logger.error("Usage: spark_metrics.py <source_path> <bucket_name>")
        sys.exit(1)

    source_path = sys.argv[1]
    bucket_name = sys.argv[2]

    logger.info(f"Starting Spark job. Source: {source_path}")

    try:
        spark = (
            SparkSession.builder.appName("HelsinkiBikesMetrics")
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            )
            .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566")
            .config("spark.hadoop.fs.s3a.access.key", "testing")
            .config("spark.hadoop.fs.s3a.secret.key", "testing")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.driver.host", "localhost")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
            .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
            .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
            .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
            .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
            .getOrCreate()
        )

        logger.info("SparkSession created successfully.")
    except Exception as e:
        logger.error(f"Failed to create SparkSession: {e}")
        sys.exit(1)

    try:
        df = spark.read.option("header", "true").csv(source_path)
        logger.info("Data loaded successfully.")

        df = (
            df.withColumnRenamed("duration (sec.)", "duration_raw")
            .withColumnRenamed("distance (m)", "distance_raw")
            .withColumnRenamed("Air temperature (degC)", "temp_raw")
            .withColumnRenamed("departure", "departure_time_raw")
            .withColumnRenamed("Departure time", "departure_time_raw")
            .withColumnRenamed("departure_name", "station_dep")
            .withColumnRenamed("return_name", "station_ret")
        )

        df_cleaned = (
            df.withColumn("date", to_timestamp(col("departure_time_raw")))
            .withColumn("distance", col("distance_raw").cast("float"))
            .withColumn("duration", col("duration_raw").cast("float"))
            .withColumn("temperature", col("temp_raw").cast("float"))
        )

        df_cleaned = df_cleaned.withColumn(
            "speed_kmh",
            when(
                col("duration") > 0, (col("distance") / 1000) / (col("duration") / 3600)
            ).otherwise(0.0),
        )

        daily_stats = df_cleaned.groupBy(
            date_format(col("date"), "yyyy-MM-dd").alias("Date")
        ).agg(
            avg("distance").alias("AvgDistance"),
            avg("duration").alias("AvgDuration"),
            avg("speed_kmh").alias("AvgSpeed"),
            avg("temperature").alias("AvgTemp"),
            count("*").alias("TripCount"),
        )

        daily_stats.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            f"s3a://{bucket_name}/metrics/daily/"
        )
        logger.info("Daily metrics saved.")

        dep_stats = (
            df_cleaned.groupBy("station_dep")
            .count()
            .withColumnRenamed("count", "DepCount")
        )
        dep_stats.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            f"s3a://{bucket_name}/metrics/departures/"
        )
        logger.info("Departure metrics saved.")

        ret_stats = (
            df_cleaned.groupBy("station_ret")
            .count()
            .withColumnRenamed("count", "RetCount")
        )
        ret_stats.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            f"s3a://{bucket_name}/metrics/returns/"
        )
        logger.info("Return metrics saved.")

    except Exception as e:
        logger.error(f"Error during Spark processing: {e}")
        spark.stop()
        sys.exit(1)

    spark.stop()


if __name__ == "__main__":
    main()
