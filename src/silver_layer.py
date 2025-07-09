from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_silver_pipeline(spark, bronze_df):
    df_silver = bronze_df \
        .filter(col("tpep_pickup_datetime").isNotNull()) \
        .filter(col("tpep_dropoff_datetime").isNotNull()) \
        .filter(col("trip_distance") > 0) \
        .filter(col("fare_amount") > 0) \
        .filter(col("total_amount") > 0) \
        .filter(col("passenger_count").between(1, 8)) \
        .filter(col("PULocationID").isNotNull()) \
        .filter(col("DOLocationID").isNotNull())

    df_silver = df_silver \
    .withColumn("trip_duration_minutes",
        (unix_timestamp("tpep_dropoff_datetime") -
        unix_timestamp("tpep_pickup_datetime")) / 60) \
    .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
    .withColumn("pickup_day_of_week", dayofweek("tpep_pickup_datetime")) \
    .withColumn("pickup_month", month("tpep_pickup_datetime")) \
    .withColumn("pickup_year", year("tpep_pickup_datetime")) \
    .withColumn("speed_mph",
        when(col("trip_duration_minutes") > 0,
            col("trip_distance") / (col("trip_duration_minutes") / 60))
        .otherwise(0)) \
    .withColumn("tip_percentage",
        when(col("fare_amount") > 0,
            col("tip_amount") / col("fare_amount") * 100)
        .otherwise(0))

    df_silver = df_silver \
        .filter(col("trip_duration_minutes").between(1, 180)) \
        .filter(col("speed_mph") < 100) \
        .filter(col("tip_percentage") <= 50)

    df_silver = df_silver \
    .withColumn("quality_score",
        when((col("trip_distance") > 0) &
            (col("trip_duration_minutes") > 0) &
            (col("fare_amount") > 0), 1.0)
        .otherwise(0.5)) \
    .withColumn("processing_timestamp", current_timestamp())

    return df_silver

def process_silver_layer(spark, bronze_df):
    """
    Function để xử lý Silver layer - wrapper cho create_silver_pipeline
    """
    import logging
    logger = logging.getLogger("SilverLayer")
    
    try:
        # Thực hiện transformations
        logger.info("Starting Silver Layer transformations...")
        silver_df = create_silver_pipeline(spark, bronze_df)
        
        # Lưu kết quả vào Silver layer
        logger.info("Saving Silver layer data...")
        silver_df.write \
            .mode("overwrite") \
            .partitionBy("pickup_year", "pickup_month") \
            .parquet("data/silver/taxi_trips/")
        
        logger.info("Silver layer processing completed successfully")
        return silver_df
        
    except Exception as e:
        logger.error(f"Silver layer processing failed: {str(e)}")
        raise