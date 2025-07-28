from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

def run_streaming_pipeline(duration_minutes=10):
    spark = SparkSession.builder \
        .appName("NYC_Taxi_Streaming") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    # Khai báo schema thủ công (không đọc từ file .parquet)
    schema = StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", LongType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", LongType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", LongType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("Airport_fee", DoubleType(), True),
    ])

    # Đọc dữ liệu streaming từ thư mục (mô phỏng)
    streaming_df = spark.readStream \
        .schema(schema) \
        .format("parquet") \
        .option("maxFilesPerTrigger", 1) \
        .load("data/streaming_input")

    streaming_processed = streaming_df \
        .withColumn("tpep_pickup_datetime", to_utc_timestamp("tpep_pickup_datetime", "UTC")) \
        .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
        .withColumn("trip_duration_minutes",
                    (unix_timestamp("tpep_dropoff_datetime") -
                     unix_timestamp("tpep_pickup_datetime")) / 60) \
        .filter(col("trip_duration_minutes") > 0)

    windowed_counts = streaming_processed \
        .withWatermark("tpep_pickup_datetime", "10 minutes") \
        .groupBy(
            window("tpep_pickup_datetime", "5 minutes", "1 minute"),
            "PULocationID"
        ) \
        .agg(
            count("*").alias("trip_count"),
            avg("fare_amount").alias("avg_fare"),
            avg("trip_duration_minutes").alias("avg_duration")
        ) \
        .withColumn("processing_time", current_timestamp())

    # Ghi ra console
    query_console = windowed_counts.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="30 seconds") \
        .start()

    # Ghi ra file (đảm bảo path là thư mục!)
    query_files = windowed_counts.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "data/streaming_output") \
        .option("checkpointLocation", "checkpoint/streaming_agg/") \
        .trigger(processingTime="1 minute") \
        .start()

    # Chờ đủ thời gian (phút)
    spark.streams.awaitAnyTermination(duration_minutes * 60)

    # Tắt stream
    query_console.stop()
    query_files.stop()
    spark.stop()
