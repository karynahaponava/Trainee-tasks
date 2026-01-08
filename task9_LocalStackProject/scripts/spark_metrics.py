import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, avg, to_date, when

def process_data(input_file, output_path):
    spark = SparkSession.builder \
        .appName("HelsinkiBikesMetrics") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"--- Spark: Читаю файл {input_file} ---")

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_file)
    
    print("--- Исходная схема ---")
    df.printSchema()

    df_clean = df \
        .withColumnRenamed("distance (m)", "distance") \
        .withColumnRenamed("Covered distance (m)", "distance") \
        .withColumnRenamed("duration (sec.)", "duration") \
        .withColumnRenamed("Duration (sec.)", "duration") \
        .withColumnRenamed("Air temperature (degC)", "temp") \
        .withColumnRenamed("departure", "departure_time") \
        .withColumnRenamed("Departure time", "departure_time")

    print("--- Spark: Считаю количество ---")
    dep_counts = df_clean.groupBy("departure_name").agg(count("*").alias("count"))
    ret_counts = df_clean.groupBy("return_name").agg(count("*").alias("count"))

    print("--- Spark: Считаю средние по дням ---")
    
    df_enriched = df_clean.withColumn("Date", to_date(col("departure_time"))) \
        .withColumn("speed_kmh", 
            when(col("duration") > 0, 
                 (col("distance") / col("duration")) * 3.6
            ).otherwise(0)
        )

    daily_stats = df_enriched.groupBy("Date").agg(
        avg("distance").alias("AvgDistance"),
        avg("duration").alias("AvgDuration"),
        avg("speed_kmh").alias("AvgSpeedKmH"),
        avg("temp").alias("AvgTemp")
    )

    print(f"--- Spark: Сохраняю в {output_path} ---")
    
    dep_counts.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}/departures_agg")
    ret_counts.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}/returns_agg")
    daily_stats.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}/daily_metrics")

    print("--- Spark: Готово! ---")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: spark_metrics.py <input_file> <output_dir>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_dir = sys.argv[2]
    
    process_data(input_path, output_dir)