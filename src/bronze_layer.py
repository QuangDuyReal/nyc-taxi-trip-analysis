# src/bronze_layer.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, year, month, input_file_name

def process_bronze_layer(spark: SparkSession):
    """
    Processes the Bronze layer by:
    1. Inferring the schema from the source to avoid type mismatches.
    2. Reading the raw data using the inferred schema.
    3. Adding partitioning and metadata columns.
    4. Writing the data to a partitioned Delta table.
    
    :param spark: The active SparkSession object.
    """
    print("Starting Bronze Layer processing...")

    # Define paths for input and output
    raw_data_path = "data/raw/yellow_tripdata_2023-01.parquet"
    bronze_output_path = "data/bronze/yellow_taxi"

    try:
        # --- BEST PRACTICE: INFER SCHEMA FIRST ---
        # Let Spark read the Parquet file and figure out the exact schema.
        print("Inferring schema from the source Parquet file...")
        inferred_schema = spark.read.parquet(raw_data_path).schema
        
        print("Schema inferred successfully. Here is the correct schema:")
        # Use .simpleString() to print the schema from a StructType object
        print(inferred_schema.simpleString())

        # Now, read the actual data using the 100% correct schema we just inferred.
        print(f"Reading data from {raw_data_path} using the correct schema...")
        df_yellow = spark.read.schema(inferred_schema).parquet(raw_data_path)

        # Create partitioning columns (year, month) from the pickup datetime
        # Also add metadata columns for data lineage
        df_with_partitions = df_yellow \
            .withColumn("year", year("tpep_pickup_datetime")) \
            .withColumn("month", month("tpep_pickup_datetime")) \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", input_file_name())

        print("Added partitioning and metadata columns.")
        
        # Write the transformed data to the Bronze layer as a partitioned Delta table
        print(f"Writing data to Delta table at: {bronze_output_path}")
        df_with_partitions.write \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .format("delta") \
            .save(bronze_output_path)

        # Verify the write operation by counting the records in the new Delta table
        written_df_count = spark.read.format("delta").load(bronze_output_path).count()
        print(f"Successfully wrote {written_df_count} records to the Bronze Delta table.")
        
        return df_with_partitions

    except Exception as e:
        print(f"An error occurred during Bronze Layer processing: {e}")
        # Re-raise the exception to stop the main pipeline if this layer fails
        raise