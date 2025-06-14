from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

def create_gold_aggregations(spark, silver_df):
    # 1. Hourly Trip Analytics
    hourly_stats = silver_df \
        .groupBy("pickup_year", "pickup_month", "pickup_hour") \
        .agg(
            count("*").alias("total_trips"),
            avg("fare_amount").alias("avg_fare"),
            avg("tip_amount").alias("avg_tip"),
            avg("trip_distance").alias("avg_distance"),
            avg("trip_duration_minutes").alias("avg_duration"),
            avg("passenger_count").alias("avg_passengers")
        )

    # 2. Location-based Analytics (Hotspots)
    location_stats = silver_df \
        .groupBy("PULocationID", "pickup_hour") \
        .agg(
            count("*").alias("pickup_count"),
            avg("fare_amount").alias("avg_fare_from_location"),
            avg("trip_distance").alias("avg_trip_distance")
        ) \
        .withColumn("pickup_density_rank",
            dense_rank().over(Window.partitionBy("pickup_hour")
                .orderBy(desc("pickup_count"))))

    # 3. Payment Analytics
    payment_stats = silver_df \
        .groupBy("payment_type", "pickup_year", "pickup_month") \
        .agg(
            count("*").alias("payment_count"),
            avg("total_amount").alias("avg_total_amount"),
            avg("tip_amount").alias("avg_tip_amount"),
            sum("total_amount").alias("total_revenue")
        )

    # 4. Driver Performance Analytics
    vendor_stats = silver_df \
        .groupBy("VendorID", "pickup_year", "pickup_month") \
        .agg(
            count("*").alias("vendor_trip_count"),
            avg("trip_distance").alias("avg_trip_distance"),
            avg("speed_mph").alias("avg_speed"),
            avg("quality_score").alias("avg_quality_score")
        )

    # Save Gold Layer Tables
    hourly_stats.write.mode("overwrite").partitionBy("pickup_year",
"pickup_month") \
        .parquet("data/gold/hourly_trip_analytics/")

    location_stats.write.mode("overwrite").partitionBy("pickup_hour") \
        .parquet("data/gold/location_hotspots/")

    payment_stats.write.mode("overwrite").partitionBy("pickup_year",
    "pickup_month") \
        .parquet("data/gold/payment_analytics/")

    vendor_stats.write.mode("overwrite").partitionBy("pickup_year",
    "pickup_month") \
        .parquet("data/gold/vendor_performance/")

    return {
        "hourly_stats": hourly_stats,
        "location_stats": location_stats,
        "payment_stats": payment_stats,
        "vendor_stats": vendor_stats
    }