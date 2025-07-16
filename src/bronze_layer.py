# src/bronze_layer.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, year, month, input_file_name

def process_bronze_layer(spark: SparkSession):
    """
    Processes the Bronze layer by:
    1. Reading all Parquet files from the raw directory.
    2. Using the 'mergeSchema' option to handle schema inconsistencies between files.
    3. Adding partitioning and metadata columns for lineage and optimization.
    4. Writing the data to a partitioned Delta table, overwriting previous data.
    
    :param spark: The active SparkSession object.
    """
    print("--- Starting Bronze Layer processing ---")

    # Define paths for input and output
    raw_data_path = "data/raw/*.parquet"
    bronze_output_path = "data/bronze/yellow_taxi_trips" # Giữ tên rõ ràng

    try:
        # --- SOLUTION: Use mergeSchema option ---
        # This option tells Spark to scan all Parquet files, merge their individual
        # schemas, and create a unified schema that can read all files correctly.
        # This is the standard way to handle schema evolution in source files.
        print(f"Reading all Parquet files from '{raw_data_path}' with schema merging enabled...")
        
        df_yellow = spark.read \
            .option("mergeSchema", "true") \
            .parquet(raw_data_path)

        print("Data read successfully. The final merged schema is:")
        df_yellow.printSchema()

        # Create partitioning columns (year, month) from the pickup datetime.
        # This is crucial for efficient querying in subsequent layers.
        df_with_partitions = df_yellow \
            .withColumn("year", year("tpep_pickup_datetime")) \
            .withColumn("month", month("tpep_pickup_datetime")) \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", input_file_name())

        print("Added partitioning and metadata columns (year, month, ingestion_timestamp, source_file).")
        
        # Write the data to the Bronze Delta table.
        # 'overwrite' mode ensures that each run is idempotent.
        # 'partitionBy' organizes the data on disk for faster reads.
        print(f"Writing data to Delta table at '{bronze_output_path}'...")
        df_with_partitions.write \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .format("delta") \
            .option("overwriteSchema", "true") \
            .save(bronze_output_path)

        # Verification step: Count records in the newly written table.
        written_df_count = spark.read.format("delta").load(bronze_output_path).count()
        print(f"--- Successfully wrote {written_df_count} records to the Bronze Delta table. ---")
        
        return df_with_partitions

    except Exception as e:
        print(f"An error occurred during Bronze Layer processing: {e}")
        # Re-raise the exception to stop the main pipeline if this layer fails.
        raise