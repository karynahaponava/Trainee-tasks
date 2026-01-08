import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DecimalType
from etl.etl_job import transform_data

# 1. ТЕСТ "HAPPY PATH"
def test_transform_calculation(spark):
    data = [
        ("evt_1", "u_1", "game_1", "prod_1", 10.0, 2, "2023-12-18 10:00:00")
    ]
    schema = ["event_id", "user_id", "game_id", "product_id", "price", "quantity", "timestamp"]
    df_input = spark.createDataFrame(data, schema)

    df_result = transform_data(df_input)
    row = df_result.collect()[0]

    assert row["purchase_id"] == "evt_1"
    assert row["total_amount"] == 20.0
    assert len(df_result.columns) == 7 

# 2. ТЕСТ НА ТИПЫ ДАННЫХ
def test_data_types(spark):
    data = [("evt_1", "u_1", "g_1", "p_1", 10.0, 1, "2023-12-18 10:00:00")]
    schema = ["event_id", "user_id", "game_id", "product_id", "price", "quantity", "timestamp"]
    df_input = spark.createDataFrame(data, schema)

    df_result = transform_data(df_input)
    dtypes = dict(df_result.dtypes)
    
    assert dtypes["purchase_timestamp"] == "timestamp"
    assert "decimal" in dtypes["total_amount"]

# 3. ТЕСТ НА ПУСТОЙ ФАЙЛ
def test_empty_dataframe(spark):
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("game_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("timestamp", StringType(), True)
    ])
    df_empty = spark.createDataFrame([], schema)

    df_result = transform_data(df_empty)

    assert df_result.count() == 0
    assert "purchase_id" in df_result.columns

# 4. ТЕСТ НА NULL
def test_null_values(spark):
    data = [
        ("evt_1", "u_1", "g_1", "p_1", None, 5, "2023-12-18 10:00:00")
    ]
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("game_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("price", DoubleType(), True), 
        StructField("quantity", IntegerType(), True),
        StructField("timestamp", StringType(), True)
    ])
    
    df_input = spark.createDataFrame(data, schema)

    df_result = transform_data(df_input)
    row = df_result.collect()[0]

    assert row["total_amount"] is None