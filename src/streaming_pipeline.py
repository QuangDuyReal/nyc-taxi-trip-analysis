from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_streaming_pipeline():
    spark = SparkSession.builder \
        .appName("NYC_Taxi_Streaming") \
        .config("spark.sql.streaming.checkpointLocation", "checkpoint/") \
        .getOrCreate()

    # Read streaming data from file source (simulation)
    streaming_df = spark.readStream \
        .format("parquet") \
        .option("path", "data/raw/yellow_tripdata_2024-01.parquet") \
        .option("maxFilesPerTrigger", 1) \
        .load()

    # Real-time transformations
    streaming_processed = streaming_df \
        .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
        .withColumn("trip_duration_minutes",
            (unix_timestamp("tpep_dropoff_datetime") -
            unix_timestamp("tpep_pickup_datetime")) / 60) \
    .filter(col("trip_duration_minutes") > 0)

    # Real-time aggregations (sliding windows)
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

    # Output to console for monitoring
    query_console = windowed_counts.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="30 seconds") \
        .start()

    # Output to files for persistence
    query_files = windowed_counts.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "data/streaming_output/realtime_aggregations/") \
        .option("checkpointLocation", "checkpoint/streaming_agg/") \
        .trigger(processingTime="1 minute") \
        .start()

    return query_console, query_files