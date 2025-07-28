import logging
import sys
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Import các hàm xử lý từ các module trong thư mục src
from src.bronze_layer import process_bronze_layer
from src.silver_layer import process_silver_layer
from src.gold_layer import process_gold_layer
from src.data_quality import DataQualityChecker
from src.streaming_pipeline import run_streaming_pipeline

def main():
    """
    Hàm chính điều phối toàn bộ pipeline dữ liệu, chạy tuần tự các lớp
    Bronze -> Silver -> Data Quality -> Gold -> Streaming (optional).
    """
    # Thiết lập logging để theo dõi tiến trình một cách chuyên nghiệp
    logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s: %(name)s: %(message)s')
    logger = logging.getLogger("MainPipeline")

    spark = None  # Khởi tạo spark là None để có thể dùng trong khối finally
    try:
        # --- KHỞI TẠO SPARK SESSION ---
        logger.info("Initializing Spark Session with Delta Lake support...")
        builder = SparkSession.builder \
            .appName("NYC_Taxi_Medallion_Pipeline") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.streaming.checkpointLocation", "checkpoint/")

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        logger.info("Spark Session initialized successfully.")

        # --- BƯỚC 1: THỰC THI BRONZE LAYER ---
        logger.info("========== Starting Bronze Layer Processing ==========")
        bronze_df = process_bronze_layer(spark)
        if bronze_df:
             logger.info("========== Bronze Layer Processing Completed Successfully ==========")
        else:
             logger.error("Bronze Layer Processing Failed. Aborting pipeline.")
             return

        # --- BƯỚC 2: THỰC THI SILVER LAYER ---
        logger.info("========== Starting Silver Layer Processing ==========")
        silver_df = process_silver_layer(spark, bronze_df)
        if silver_df:
            logger.info("========== Silver Layer Processing Completed Successfully ==========")
        else:
            logger.error("Silver Layer Processing Failed. Aborting pipeline.")
            return

        # --- BƯỚC 3: DATA QUALITY CHECK ---
        logger.info("========== Running Data Quality Checks on Silver Layer ==========")
        dq = DataQualityChecker(spark)
        metrics = dq.check_data_quality(silver_df, layer_name="silver")
        report = dq.generate_quality_report(metrics)
        print("\nDATA QUALITY REPORT:\n" + report + "\n")
        logger.info("========== Data Quality Check Completed ==========")

        # --- BƯỚC 4: THỰC THI GOLD LAYER ---
        logger.info("========== Starting Gold Layer Processing ==========")
        gold_tables = process_gold_layer(spark, silver_df)
        if gold_tables:
            logger.info("========== Gold Layer Processing Completed Successfully ==========")

            # In thông tin tóm tắt
            logger.info("=== PIPELINE SUMMARY ===")
            logger.info(f"Gold layer tables created: {len(gold_tables)}")
            for table_name, table_df in gold_tables.items():
                try:
                    count = table_df.count()
                    logger.info(f"  - {table_name}: {count:,} records")
                except Exception as e:
                    logger.warning(f"  - {table_name}: Could not count records ({str(e)})")
        else:
            logger.error("Gold Layer Processing Failed.")
            return

        # --- BƯỚC 5: STREAMING PIPELINE (OPTIONAL) ---
        if "--streaming" in sys.argv:
            logger.info("========== Starting Streaming Pipeline ==========")
            duration = 10
            if "--duration" in sys.argv:
                try:
                    duration_index = sys.argv.index("--duration") + 1
                    duration = int(sys.argv[duration_index])
                except (ValueError, IndexError):
                    logger.warning("Invalid duration argument, using default 10 minutes")

            run_streaming_pipeline(duration_minutes=duration)
            logger.info("========== Streaming Pipeline Completed ==========")

        logger.info(">>>>>>>>>> ENTIRE PIPELINE COMPLETED SUCCESSFULLY <<<<<<<<<")

    except Exception as e:
        logger.error(f"!!!!!!!!!! MAIN PIPELINE EXECUTION FAILED !!!!!!!!!!", exc_info=True)

    finally:
        if spark:
            logger.info("Stopping Spark Session.")
            spark.stop()

if __name__ == "__main__":
    main()
