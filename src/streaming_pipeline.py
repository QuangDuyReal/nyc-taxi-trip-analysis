import os
import time
import logging
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, count, sum, max, min, expr
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from delta import configure_spark_with_delta_pip
import psutil
import platform

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s: %(name)s: %(message)s')
logger = logging.getLogger("StreamingPipeline")

def init_spark_session():
    """Initialize and return a Spark session configured for streaming"""
    builder = SparkSession.builder \
        .appName("NYC_Taxi_Streaming_Pipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.streaming.checkpointLocation", "checkpoint/streaming") \
        .config("spark.sql.streaming.minBatchesToRetain", "10") \
        .config("spark.sql.streaming.pollingDelay", "1s")
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

def get_taxi_schema():
    """Define the schema for the NYC Taxi data"""
    from pyspark.sql.types import LongType, IntegerType
    
    return StructType([
        StructField("VendorID", LongType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", LongType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", LongType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),  # Changed from LongType to IntegerType
        StructField("DOLocationID", IntegerType(), True),  # Changed from LongType to IntegerType
        StructField("payment_type", LongType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True)
    ])

def setup_streaming_directories():
    """Create or clean directories for streaming input and output"""
    input_dir = "data/streaming_input"
    output_dir = "data/streaming_output"
    checkpoint_dir = "checkpoint/streaming"
    
    for directory in [input_dir, output_dir, checkpoint_dir]:
        if os.path.exists(directory):
            # Clean directory but keep it
            for item in os.listdir(directory):
                item_path = os.path.join(directory, item)
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                else:
                    os.remove(item_path)
        else:
            # Create directory if it doesn't exist
            os.makedirs(directory, exist_ok=True)
    
    return input_dir, output_dir

def simulate_data_arrival(spark, input_dir, duration_minutes=10):
    """
    Simulate real-time data arrival by periodically copying parquet files to the input directory.
    This function runs in a separate thread to simulate streaming data sources.
    """
    logger.info(f"Starting data arrival simulation for {duration_minutes} minutes")
    
    # Source data directory with sample parquet files
    sample_file = "data/raw/yellow_tripdata_2024-01.parquet"
    
    if not os.path.exists(sample_file):
        logger.error(f"Sample file {sample_file} not found. Cannot simulate data arrival.")
        return
    
    # Read sample file and repartition to create smaller chunks
    try:
        df = spark.read.parquet(sample_file)
        
        # Repartition into smaller chunks for streaming simulation
        num_partitions = 20
        df.repartition(num_partitions).write.mode("overwrite").parquet("data/streaming_source_temp")
        
        # Get list of generated part files
        part_files = []
        for root, dirs, files in os.walk("data/streaming_source_temp"):
            for file in files:
                if file.endswith(".parquet"):
                    part_files.append(os.path.join(root, file))
        
        # Simulate data arrival every 30 seconds
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        file_index = 0
        
        while time.time() < end_time:
            if file_index < len(part_files):
                source_file = part_files[file_index]
                target_file = os.path.join(input_dir, f"batch_{file_index}_{int(time.time())}.parquet")
                
                # Copy the file to simulate new data arrival
                shutil.copy2(source_file, target_file)
                logger.info(f"New data arrived: {target_file}")
                
                file_index += 1
                if file_index >= len(part_files):
                    file_index = 0  # Loop back to the beginning
                    
                # Wait 30 seconds before next batch
                time.sleep(30)
            else:
                break
                
        # Clean up temp directory
        shutil.rmtree("data/streaming_source_temp", ignore_errors=True)
        
    except Exception as e:
        logger.error(f"Error in data arrival simulation: {str(e)}")

def process_stream(spark, input_dir, output_dir):
    """
    Process streaming data from input directory, apply transformations,
    and write results to output directory.
    """
    try:
        # Define schema for input data
        schema = get_taxi_schema()
        
        # Read streaming data
        stream_df = spark.readStream \
            .format("parquet") \
            .schema(schema) \
            .option("maxFilesPerTrigger", 1) \
            .load(input_dir)
        
        # --- BRONZE STREAM PROCESSING ---
        # Simple validations and metadata enrichment
        bronze_stream = stream_df \
            .withColumn("processing_time", expr("current_timestamp()")) \
            .withColumn("source", expr("'streaming'"))
            
        # --- SILVER STREAM PROCESSING ---
        # Apply cleansing rules similar to batch processing
        silver_stream = bronze_stream \
            .filter(col("fare_amount") > 0) \
            .filter(col("trip_distance") > 0) \
            .filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")) \
            .withColumn("trip_duration_minutes", 
                        expr("(unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60")) \
            .withColumn("speed_mph", 
                        expr("CASE WHEN trip_duration_minutes > 0 THEN trip_distance / (trip_duration_minutes / 60) ELSE NULL END"))

        # --- GOLD STREAM PROCESSING ---
        # 1. Real-time KPIs: Metrics calculated over sliding windows
        
        # Aggregate by pickup time windows (10-minute windows, sliding every 5 minutes)
        pickup_time_metrics = silver_stream \
            .withWatermark("tpep_pickup_datetime", "10 minutes") \
            .groupBy(window(col("tpep_pickup_datetime"), "10 minutes", "5 minutes")) \
            .agg(
                count("*").alias("trip_count"),
                sum("total_amount").alias("total_revenue"),
                avg("trip_distance").alias("avg_distance"),
                avg("trip_duration_minutes").alias("avg_duration"),
                avg("speed_mph").alias("avg_speed")
            )
        
        # 2. Location-based metrics
        location_metrics = silver_stream \
            .withWatermark("tpep_pickup_datetime", "10 minutes") \
            .groupBy("PULocationID", window(col("tpep_pickup_datetime"), "15 minutes")) \
            .agg(
                count("*").alias("pickup_count"),
                sum("total_amount").alias("zone_revenue")
            )
        
        # 3. Payment type metrics
        payment_metrics = silver_stream \
            .withWatermark("tpep_pickup_datetime", "10 minutes") \
            .groupBy("payment_type", window(col("tpep_pickup_datetime"), "15 minutes")) \
            .agg(
                count("*").alias("payment_count"),
                sum("total_amount").alias("payment_revenue"),
                avg("tip_amount").alias("avg_tip")
            )
            
        # Write the streams to output
        query1 = pickup_time_metrics.writeStream \
            .outputMode("append") \
            .format("delta") \
            .option("checkpointLocation", "checkpoint/streaming/time_metrics") \
            .start(f"{output_dir}/time_metrics")
            
        query2 = location_metrics.writeStream \
            .outputMode("append") \
            .format("delta") \
            .option("checkpointLocation", "checkpoint/streaming/location_metrics") \
            .start(f"{output_dir}/location_metrics")
            
        query3 = payment_metrics.writeStream \
            .outputMode("append") \
            .format("delta") \
            .option("checkpointLocation", "checkpoint/streaming/payment_metrics") \
            .start(f"{output_dir}/payment_metrics")
            
        # Return queries for management
        return [query1, query2, query3]
        
    except Exception as e:
        logger.error(f"Error in stream processing: {str(e)}")
        return []



def get_system_info():
    """Collect system information for logging"""
    return {
        "os": platform.platform(),
        "python_version": platform.python_version(),
        "cpu_count": psutil.cpu_count(logical=True),
        "physical_cpu_count": psutil.cpu_count(logical=False),
        "memory_total_gb": round(psutil.virtual_memory().total / (1024**3), 2),
        "memory_available_gb": round(psutil.virtual_memory().available / (1024**3), 2),
        "cpu_usage_percent": psutil.cpu_percent(interval=1),
        "memory_usage_percent": psutil.virtual_memory().percent,
        "disk_usage_percent": psutil.disk_usage('/').percent
    }

def get_spark_info(spark):
    """Collect Spark configuration information for logging"""
    return {
        "app_id": spark.sparkContext.applicationId,
        "app_name": spark.sparkContext.appName,
        "master": spark.sparkContext.master,
        "version": spark.version,
        "executor_memory": spark.sparkContext.getConf().get("spark.executor.memory", "Not set"),
        "executor_instances": spark.sparkContext.getConf().get("spark.executor.instances", "Not set"),
        "executor_cores": spark.sparkContext.getConf().get("spark.executor.cores", "Not set"),
        "default_parallelism": spark.sparkContext.defaultParallelism
    }

def log_infrastructure_metrics(spark, logger):
    """Log infrastructure metrics"""
    system_info = get_system_info()
    spark_info = get_spark_info(spark)
    
    logger.info("===== INFRASTRUCTURE METRICS =====")
    logger.info(f"SYSTEM: OS={system_info['os']}, CPU={system_info['cpu_count']} cores "
                f"({system_info['physical_cpu_count']} physical), "
                f"Memory={system_info['memory_total_gb']}GB total, {system_info['memory_available_gb']}GB available")
    logger.info(f"CURRENT USAGE: CPU={system_info['cpu_usage_percent']}%, "
                f"Memory={system_info['memory_usage_percent']}%, Disk={system_info['disk_usage_percent']}%")
    logger.info(f"SPARK: Version={spark_info['version']}, Master={spark_info['master']}, "
                f"App ID={spark_info['app_id']}, Default Parallelism={spark_info['default_parallelism']}")
    logger.info(f"SPARK CONFIG: Executor Memory={spark_info['executor_memory']}, "
                f"Instances={spark_info['executor_instances']}, Cores={spark_info['executor_cores']}")
    logger.info("===================================")
def analyze_streaming_output(spark, output_dir, logger):
    """Analyze streaming output data and log insights"""
    logger.info("===== STREAMING OUTPUT ANALYSIS =====")
    
    try:
        # Analyze time metrics
        time_metrics_path = f"{output_dir}/time_metrics"
        if os.path.exists(time_metrics_path):
            df = spark.read.format("delta").load(time_metrics_path)
            if df.count() > 0:
                # Get the most recent window for statistics
                latest_window = df.selectExpr("max(window.end) as max_end").collect()[0].max_end
                latest_df = df.filter(f"window.end = '{latest_window}'")
                
                # Calculate key statistics
                stats = latest_df.agg(
                    sum("trip_count").alias("total_trips"),
                    sum("total_revenue").alias("total_revenue"),
                    avg("avg_distance").alias("avg_trip_distance"),
                    avg("avg_duration").alias("avg_trip_duration"),
                    avg("avg_speed").alias("avg_speed")
                ).collect()[0]
                
                logger.info(f"TIME METRICS (latest window ending {latest_window}):")
                logger.info(f"  Total Trips: {stats['total_trips']}")
                logger.info(f"  Total Revenue: ${stats['total_revenue']:.2f}")
                logger.info(f"  Average Trip Distance: {stats['avg_trip_distance']:.2f} miles")
                logger.info(f"  Average Trip Duration: {stats['avg_trip_duration']:.2f} minutes")
                logger.info(f"  Average Speed: {stats['avg_speed']:.2f} mph")
                
                # Trend analysis (comparing to previous window)
                window_counts = df.groupBy("window.end").agg(
                    sum("trip_count").alias("total_trips"),
                    sum("total_revenue").alias("total_revenue")
                ).orderBy("end", ascending=False).limit(2).collect()
                
                if len(window_counts) > 1:
                    current = window_counts[0]
                    previous = window_counts[1]
                    trip_change_pct = ((current["total_trips"] - previous["total_trips"]) / previous["total_trips"]) * 100
                    revenue_change_pct = ((current["total_revenue"] - previous["total_revenue"]) / previous["total_revenue"]) * 100
                    
                    logger.info(f"  Trend (vs previous window): Trips {trip_change_pct:+.1f}%, Revenue {revenue_change_pct:+.1f}%")
        
        # Analyze location metrics
        location_metrics_path = f"{output_dir}/location_metrics"
        if os.path.exists(location_metrics_path):
            location_df = spark.read.format("delta").load(location_metrics_path)
            
            if location_df.count() > 0:
                # Find top pickup locations
                top_locations = location_df.groupBy("PULocationID") \
                    .agg(sum("pickup_count").alias("total_pickups"), 
                         sum("zone_revenue").alias("total_revenue")) \
                    .orderBy("total_pickups", ascending=False) \
                    .limit(5) \
                    .collect()
                
                logger.info("TOP PICKUP LOCATIONS:")
                for loc in top_locations:
                    logger.info(f"  Location ID {loc['PULocationID']}: {loc['total_pickups']} pickups, ${loc['total_revenue']:.2f} revenue")
        
        # Analyze payment metrics
        payment_metrics_path = f"{output_dir}/payment_metrics"
        if os.path.exists(payment_metrics_path):
            payment_df = spark.read.format("delta").load(payment_metrics_path)
            
            if payment_df.count() > 0:
                # Payment type distribution
                payment_dist = payment_df.groupBy("payment_type") \
                    .agg(sum("payment_count").alias("total_count"),
                         sum("payment_revenue").alias("total_revenue"),
                         avg("avg_tip").alias("overall_avg_tip")) \
                    .collect()
                
                payment_map = {1: "Credit Card", 2: "Cash", 3: "No Charge", 4: "Dispute", 5: "Unknown", 6: "Voided"}
                
                logger.info("PAYMENT METHOD DISTRIBUTION:")
                for payment in payment_dist:
                    payment_name = payment_map.get(payment["payment_type"], f"Type {payment['payment_type']}")
                    logger.info(f"  {payment_name}: {payment['total_count']} trips, ${payment['total_revenue']:.2f} revenue, ${payment['overall_avg_tip']:.2f} avg tip")
    
    except Exception as e:
        logger.error(f"Error analyzing streaming output: {str(e)}")
    
    logger.info("=====================================")
def run_streaming_pipeline(duration_minutes=10):
    """
    Main function to run the streaming pipeline for a specified duration.
    
    Args:
        duration_minutes (int): Duration to run the streaming pipeline in minutes
    """
    logger.info(f"Starting streaming pipeline to run for {duration_minutes} minutes")
    
    try:
        # Initialize Spark session
        spark = init_spark_session()
        
        # Log infrastructure information
        log_infrastructure_metrics(spark, logger)
        
        # Setup streaming directories
        input_dir, output_dir = setup_streaming_directories()
        
        # Start the data simulation in a separate thread
        import threading
        sim_thread = threading.Thread(
            target=simulate_data_arrival, 
            args=(spark, input_dir, duration_minutes)
        )
        sim_thread.daemon = True
        sim_thread.start()
        
        # Process the stream and get queries
        queries = process_stream(spark, input_dir, output_dir)
        
        if not queries:
            logger.error("Failed to start streaming queries")
            return
            
        # Let the queries run for the specified duration
        logger.info(f"Streaming queries started. Running for {duration_minutes} minutes...")
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        
        try:
            # Keep the main thread alive and monitor queries
            while time.time() < end_time:
                # Check if any query has terminated
                if any(query.exception() is not None for query in queries):
                    for query in queries:
                        if query.exception() is not None:
                            logger.error(f"Query failed: {query.exception()}")
                    break
                    
                # Print streaming metrics and infrastructure metrics every minute
                current_time = time.time()
                if int((current_time - start_time) % 60) == 0:
                    log_infrastructure_metrics(spark, logger)
                    
                    for i, query in enumerate(queries):
                        recent_progress = query.recentProgress
                        if recent_progress:
                            latest = recent_progress[-1]
                            logger.info(f"Query {i+1} progress: "
                                      f"Input rows: {latest.get('numInputRows', 0)}, "
                                      f"Processing rate: {latest.get('processedRowsPerSecond', 0):.2f} rows/sec, "
                                      f"Batch duration: {latest.get('batchDuration', 0):.2f} ms")
                        else:
                            logger.info(f"Query {i+1} progress: No recent progress information")
                
                time.sleep(5)
                
        except KeyboardInterrupt:
            logger.info("Streaming job interrupted by user")
            
        finally:
            # Stop all queries
            for query in queries:
                query.stop()
            
            logger.info("All streaming queries stopped")
            
            # Analyze streaming output
            analyze_streaming_output(spark, output_dir, logger)
            
            # Log final infrastructure metrics
            log_infrastructure_metrics(spark, logger)
            
            # Generate summary of processed data
            try:
                for output_type in ["time_metrics", "location_metrics", "payment_metrics"]:
                    output_path = f"{output_dir}/{output_type}"
                    delta_log_path = os.path.join(output_path, "_delta_log")

                    if os.path.exists(delta_log_path):
                        try:
                            result_df = spark.read.format("delta").load(output_path)
                            count = result_df.count()
                            logger.info(f"Streaming output summary - {output_type}: {count} records processed")
                        except Exception as e:
                            logger.warning(f"Could not read delta table at {output_path}: {str(e)}")
                    else:
                        logger.warning(f"Delta table not found at {output_path}, skipping summary.")
            except Exception as e:
                logger.error(f"Error generating summary: {str(e)}")
                
    except Exception as e:
        logger.error(f"Error in streaming pipeline: {str(e)}")
    
    logger.info("Streaming pipeline completed")
if __name__ == "__main__":
    # This allows the module to be run standalone for testing
    run_streaming_pipeline(duration_minutes=10)