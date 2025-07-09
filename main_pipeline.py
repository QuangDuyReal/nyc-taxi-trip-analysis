# main_pipeline.py

import logging
from pyspark.sql import SparkSession

# Import các hàm xử lý từ các module trong thư mục src
from src.bronze_layer import process_bronze_layer
from src.silver_layer import process_silver_layer
# from src.gold_layer import process_gold_layer # Sẽ được thêm vào trong các bước tiếp theo

def main():
    """
    Hàm chính điều phối toàn bộ pipeline dữ liệu, chạy tuần tự các lớp
    Bronze -> Silver -> Gold.
    """
    # Thiết lập logging để theo dõi tiến trình một cách chuyên nghiệp
    logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s: %(name)s: %(message)s')
    logger = logging.getLogger("MainPipeline")
    
    spark = None  # Khởi tạo spark là None để có thể dùng trong khối finally
    try:
        # --- KHỞI TẠO SPARK SESSION ---
        logger.info("Initializing Spark Session with Delta Lake support...")
        spark = SparkSession.builder \
            .appName("NYC_Taxi_Medallion_Pipeline") \
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.1") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
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

        # --- BƯỚC 3: THỰC THI GOLD LAYER (sẽ làm ở bước sau) ---
        # logger.info("========== Starting Gold Layer Processing ==========")
        # gold_tables = process_gold_layer(spark, silver_df)
        # if gold_tables:
        #     logger.info("========== Gold Layer Processing Completed Successfully ==========")
        #
        # logger.info(">>>>>>>>> ENTIRE PIPELINE COMPLETED SUCCESSFULLY <<<<<<<<<")

    except Exception as e:
        logger.error(f"!!!!!!!!!! MAIN PIPELINE EXECUTION FAILED !!!!!!!!!!", exc_info=True)
    
    finally:
        if spark:
            logger.info("Stopping Spark Session.")
            spark.stop()

if __name__ == "__main__":
    main()