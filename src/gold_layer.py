import logging
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Thiết lập logger
logger = logging.getLogger("GoldLayer")

def validate_silver_data(silver_df):
    """
    Kiểm tra tính hợp lệ của Silver layer data trước khi xử lý Gold layer
    """
    try:
        # Kiểm tra DataFrame không rỗng
        if silver_df is None:
            logger.error("Silver DataFrame is None")
            return False
        
        # Kiểm tra có dữ liệu không
        row_count = silver_df.count()
        if row_count == 0:
            logger.error("Silver layer DataFrame is empty")
            return False
        
        # Kiểm tra các cột cần thiết
        required_columns = [
            "pickup_year", "pickup_month", "pickup_hour", "PULocationID", 
            "fare_amount", "tip_amount", "trip_distance", "total_amount",
            "passenger_count", "payment_type", "VendorID"
        ]
        
        # Kiểm tra các cột tùy chọn (có thể được tạo trong silver layer)
        optional_columns = ["trip_duration_minutes", "speed_mph", "quality_score"]
        
        missing_required = [col for col in required_columns if col not in silver_df.columns]
        if missing_required:
            logger.error(f"Missing required columns in Silver layer: {missing_required}")
            return False
        
        # Thông báo về các cột tùy chọn bị thiếu
        missing_optional = [col for col in optional_columns if col not in silver_df.columns]
        if missing_optional:
            logger.warning(f"Missing optional columns (will be created): {missing_optional}")
        
        logger.info(f"Silver layer validation passed. Row count: {row_count:,}")
        return True
        
    except Exception as e:
        logger.error(f"Error during Silver layer validation: {str(e)}")
        return False

def ensure_required_columns(silver_df):
    """
    Đảm bảo các cột cần thiết tồn tại, tạo nếu chưa có
    """
    # Tạo trip_duration_minutes nếu chưa có
    if "trip_duration_minutes" not in silver_df.columns:
        logger.info("Creating trip_duration_minutes column")
        silver_df = silver_df.withColumn("trip_duration_minutes", 
            when(col("tpep_dropoff_datetime").isNotNull() & col("tpep_pickup_datetime").isNotNull(),
                 (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60.0)
            .otherwise(0.0))
    
    # Tạo speed_mph nếu chưa có
    if "speed_mph" not in silver_df.columns:
        logger.info("Creating speed_mph column")
        silver_df = silver_df.withColumn("speed_mph",
            when((col("trip_duration_minutes") > 0) & (col("trip_distance") > 0),
                 col("trip_distance") / (col("trip_duration_minutes") / 60.0))
            .otherwise(0.0))
    
    # Tạo quality_score nếu chưa có
    if "quality_score" not in silver_df.columns:
        logger.info("Creating quality_score column")
        silver_df = silver_df.withColumn("quality_score",
            when((col("trip_distance") > 0) & 
                 (col("fare_amount") > 0) & 
                 (col("total_amount") > 0), 1.0)
            .otherwise(0.5))
    
    return silver_df

def create_gold_aggregations(spark, silver_df):
    """
    Tạo các aggregation tables cho Gold layer
    """
    try:
        # Validation đầu vào
        if not validate_silver_data(silver_df):
            logger.error("Silver data validation failed")
            return None
        
        # Đảm bảo các cột cần thiết tồn tại
        silver_df = ensure_required_columns(silver_df)
        
        logger.info("Creating Gold layer aggregations...")
        
        # 1. Hourly Trip Analytics
        logger.info("Processing hourly trip analytics...")
        hourly_stats = silver_df \
            .groupBy("pickup_year", "pickup_month", "pickup_hour") \
            .agg(
                count("*").alias("total_trips"),
                avg("fare_amount").alias("avg_fare"),
                avg("tip_amount").alias("avg_tip"),
                avg("trip_distance").alias("avg_distance"),
                avg("trip_duration_minutes").alias("avg_duration"),
                avg("passenger_count").alias("avg_passengers"),
                sum("total_amount").alias("total_revenue"),
                max("total_amount").alias("max_fare"),
                min("total_amount").alias("min_fare")
            ) \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("layer", lit("gold"))
        
        # 2. Location-based Analytics (Hotspots)
        logger.info("Processing location-based analytics...")
        location_stats = silver_df \
            .groupBy("PULocationID", "pickup_hour") \
            .agg(
                count("*").alias("pickup_count"),
                avg("fare_amount").alias("avg_fare_from_location"),
                avg("trip_distance").alias("avg_trip_distance"),
                avg("tip_amount").alias("avg_tip_from_location"),
                avg("trip_duration_minutes").alias("avg_duration_from_location")
            ) \
            .withColumn("pickup_density_rank",
                dense_rank().over(Window.partitionBy("pickup_hour")
                    .orderBy(desc("pickup_count")))) \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("layer", lit("gold"))
        
        # 3. Payment Analytics
        logger.info("Processing payment analytics...")
        payment_stats = silver_df \
            .groupBy("payment_type", "pickup_year", "pickup_month") \
            .agg(
                count("*").alias("payment_count"),
                avg("total_amount").alias("avg_total_amount"),
                avg("tip_amount").alias("avg_tip_amount"),
                sum("total_amount").alias("total_revenue"),
                (avg("tip_amount") / avg("fare_amount") * 100).alias("avg_tip_percentage")
            ) \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("layer", lit("gold"))
        
        # 4. Vendor Performance Analytics
        logger.info("Processing vendor performance analytics...")
        vendor_stats = silver_df \
            .groupBy("VendorID", "pickup_year", "pickup_month") \
            .agg(
                count("*").alias("vendor_trip_count"),
                avg("trip_distance").alias("avg_trip_distance"),
                avg("speed_mph").alias("avg_speed"),
                avg("quality_score").alias("avg_quality_score"),
                avg("trip_duration_minutes").alias("avg_trip_duration"),
                sum("total_amount").alias("vendor_total_revenue")
            ) \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("layer", lit("gold"))
        
        # Tạo thư mục nếu chưa tồn tại
        gold_base_path = "data/gold"
        os.makedirs(gold_base_path, exist_ok=True)
        
        # Save Gold Layer Tables với error handling
        logger.info("Saving Gold layer tables...")
        
        # Save hourly stats
        try:
            hourly_stats.coalesce(10).write.mode("overwrite") \
                .partitionBy("pickup_year", "pickup_month") \
                .parquet("data/gold/hourly_trip_analytics/")
            logger.info("✓ Hourly analytics saved successfully")
        except Exception as e:
            logger.error(f"Error saving hourly analytics: {str(e)}")
        
        # Save location stats
        try:
            location_stats.coalesce(10).write.mode("overwrite") \
                .partitionBy("pickup_hour") \
                .parquet("data/gold/location_hotspots/")
            logger.info("✓ Location analytics saved successfully")
        except Exception as e:
            logger.error(f"Error saving location analytics: {str(e)}")
        
        # Save payment stats
        try:
            payment_stats.coalesce(5).write.mode("overwrite") \
                .partitionBy("pickup_year", "pickup_month") \
                .parquet("data/gold/payment_analytics/")
            logger.info("✓ Payment analytics saved successfully")
        except Exception as e:
            logger.error(f"Error saving payment analytics: {str(e)}")
        
        # Save vendor stats
        try:
            vendor_stats.coalesce(5).write.mode("overwrite") \
                .partitionBy("pickup_year", "pickup_month") \
                .parquet("data/gold/vendor_performance/")
            logger.info("✓ Vendor performance analytics saved successfully")
        except Exception as e:
            logger.error(f"Error saving vendor performance analytics: {str(e)}")
        
        # Log summary statistics
        logger.info("=" * 50)
        logger.info("GOLD LAYER SUMMARY")
        logger.info("=" * 50)
        try:
            logger.info(f"Hourly stats records: {hourly_stats.count():,}")
            logger.info(f"Location stats records: {location_stats.count():,}")
            logger.info(f"Payment stats records: {payment_stats.count():,}")
            logger.info(f"Vendor stats records: {vendor_stats.count():,}")
        except Exception as e:
            logger.warning(f"Could not count records: {str(e)}")
        
        return {
            "hourly_stats": hourly_stats,
            "location_stats": location_stats,
            "payment_stats": payment_stats,
            "vendor_stats": vendor_stats
        }
        
    except Exception as e:
        logger.error(f"Error in Gold layer processing: {str(e)}", exc_info=True)
        return None

def process_gold_layer(spark, silver_df):
    """
    Hàm wrapper để tương thích với main_pipeline.py
    """
    logger.info("=" * 60)
    logger.info("STARTING GOLD LAYER PROCESSING")
    logger.info("=" * 60)
    
    try:
        # Gọi hàm chính để tạo aggregations
        gold_tables = create_gold_aggregations(spark, silver_df)
        
        if gold_tables:
            logger.info("=" * 60)
            logger.info("GOLD LAYER PROCESSING COMPLETED SUCCESSFULLY")
            logger.info("=" * 60)
            return gold_tables
        else:
            logger.error("=" * 60)
            logger.error("GOLD LAYER PROCESSING FAILED")
            logger.error("=" * 60)
            return None
            
    except Exception as e:
        logger.error(f"Error in Gold layer processing: {str(e)}", exc_info=True)
        return None

def get_gold_layer_summary(spark):
    """
    Hàm tiện ích để lấy tổng quan về Gold layer data
    """
    try:
        summary = {}
        
        # Đọc và đếm records từ các bảng Gold
        paths = {
            "hourly_analytics": "data/gold/hourly_trip_analytics/",
            "location_hotspots": "data/gold/location_hotspots/",
            "payment_analytics": "data/gold/payment_analytics/",
            "vendor_performance": "data/gold/vendor_performance/"
        }
        
        for name, path in paths.items():
            try:
                if os.path.exists(path):
                    df = spark.read.parquet(path)
                    summary[name] = df.count()
                else:
                    summary[name] = 0
            except Exception as e:
                logger.warning(f"Could not read {name}: {str(e)}")
                summary[name] = 0
        
        return summary
        
    except Exception as e:
        logger.error(f"Error getting Gold layer summary: {str(e)}")
        return {}

def create_business_kpis(spark, gold_tables):
    """
    Tạo các KPI business từ Gold layer data
    """
    try:
        if not gold_tables:
            logger.error("No Gold tables provided for KPI calculation")
            return None
        
        logger.info("Creating business KPIs...")
        
        kpis = {}
        
        # KPI từ hourly stats
        if "hourly_stats" in gold_tables:
            hourly_df = gold_tables["hourly_stats"]
            kpis["peak_hour"] = hourly_df.orderBy(desc("total_trips")).first()["pickup_hour"]
            kpis["total_daily_trips"] = hourly_df.agg(sum("total_trips")).collect()[0][0]
            kpis["avg_daily_revenue"] = hourly_df.agg(avg("total_revenue")).collect()[0][0]
        
        # KPI từ payment stats
        if "payment_stats" in gold_tables:
            payment_df = gold_tables["payment_stats"]
            kpis["most_popular_payment"] = payment_df.orderBy(desc("payment_count")).first()["payment_type"]
        
        logger.info(f"Business KPIs created: {kpis}")
        return kpis
        
    except Exception as e:
        logger.error(f"Error creating business KPIs: {str(e)}")
        return None