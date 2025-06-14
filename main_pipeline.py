from src.bronze_layer import create_bronze_pipeline
from src.silver_layer import create_silver_pipeline
from src.gold_layer import create_gold_aggregations
from src.streaming_pipeline import create_streaming_pipeline
from pyspark.sql import SparkSession
import logging
import sys

def main():
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("NYC_Taxi_Pipeline_Main") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
        .getOrCreate()
    try:
        # Step 1: Bronze Layer

        logger.info("Starting Bronze Layer Processing...")
        bronze_df = create_bronze_pipeline()
        logger.info(f"Bronze Layer: {bronze_df.count()} records processed")
        # Step 2: Silver Layer

        logger.info("Starting Silver Layer Processing...")
        silver_df = create_silver_pipeline(spark, bronze_df)
        silver_df.cache() # Cache for multiple uses
        logger.info(f"Silver Layer: {silver_df.count()} records processed")
        # Step 3: Gold Layer

        logger.info("Starting Gold Layer Processing...")
        gold_tables = create_gold_aggregations(spark, silver_df)
        logger.info("Gold Layer aggregations completed")

        # Step 4: Streaming (optional)
        if "--streaming" in sys.argv:
            logger.info("Starting Streaming Pipeline...")
            query_console, query_files = create_streaming_pipeline()
            query_console.awaitTermination()

    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()