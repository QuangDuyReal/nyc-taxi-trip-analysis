from pyspark.sql import SparkSession
from pyspark.sql.types import *
def create_bronze_pipeline():
    spark = SparkSession.builder \
        .appName("NYC_Taxi_Bronze_Layer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    yellow_taxi_schema = StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
        StructField("cbd_congestion_fee", DoubleType(), True)
    ])

    df_yellow = spark.read \
        .schema(yellow_taxi_schema) \
        .option("multiline", "true") \
        .parquet("data/raw/yellow_tripdata_*.parquet")
        
    from pyspark.sql.functions import current_timestamp, lit
    df_bronze = df_yellow \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("yellow_taxi")) \
        .withColumn("data_layer", lit("bronze"))

    df_bronze.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet("data/bronze/yellow_taxi/")
    return df_bronze