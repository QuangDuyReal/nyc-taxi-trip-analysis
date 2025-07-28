# src/export_gold_to_csv.py

import os
import shutil
from pyspark.sql import SparkSession

def export_gold_layer_to_csv(spark: SparkSession):
    """
    Reads the aggregated Delta tables from the Gold layer
    and exports them as single CSV files for Power BI consumption.
    This script is designed to be robust and runnable from any location.
    """
    print("--- Starting Gold Layer to CSV Export Process ---")

    # --- THE FIX: Determine the project's absolute root path ---
    # This ensures that all relative paths are built correctly,
    # regardless of where the script is executed from.
    try:
        # __file__ is the path to the current script.
        # os.path.dirname gets the directory of the script (src).
        # os.path.dirname again goes up one level to the project root.
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    except NameError:
        # Fallback for interactive environments like a simple Python REPL
        project_root = os.path.abspath(".") 
    
    print(f"Project root directory determined as: {project_root}")

    # Build absolute paths for input and output
    gold_base_path = os.path.join(project_root, "data", "gold")
    csv_output_path = os.path.join(project_root, "data", "for_power_bi")

    if not os.path.exists(csv_output_path):
        os.makedirs(csv_output_path)
        print(f"Created directory: {csv_output_path}")

    tables_to_export = {
        "hourly_trip_analytics": "hourly_stats.csv",
        "location_hotspots": "location_stats.csv",
        "payment_analytics": "payment_stats.csv",
        "vendor_performance": "vendor_stats.csv" # Thêm bảng này
    }

    for table_name, csv_file_name in tables_to_export.items():
        delta_path = os.path.join(gold_base_path, table_name)
        
        # Check if the source delta table directory exists before trying to read
        if not os.path.exists(delta_path):
            print(f"WARNING: Delta table path does not exist, skipping: {delta_path}")
            continue # Bỏ qua và chuyển đến bảng tiếp theo

        try:
            print(f"Reading Delta table from: {delta_path}")
            gold_df = spark.read.format("delta").load(delta_path)

            output_file = os.path.join(csv_output_path, csv_file_name)
            temp_csv_folder = output_file + "_temp"

            print(f"Writing to temporary CSV folder: {temp_csv_folder}")
            
            gold_df.coalesce(1).write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(temp_csv_folder)

            # Find the generated part-....csv file and rename it
            generated_file = [f for f in os.listdir(temp_csv_folder) if f.startswith("part-") and f.endswith(".csv")][0]
            final_csv_path = os.path.join(csv_output_path, csv_file_name)
            
            # If the final file already exists, remove it before renaming
            if os.path.exists(final_csv_path):
                os.remove(final_csv_path)

            os.rename(os.path.join(temp_csv_folder, generated_file), final_csv_path)
            shutil.rmtree(temp_csv_folder) # Clean up the temporary directory

            print(f"Successfully exported '{table_name}' to '{final_csv_path}'")

        except Exception as e:
            print(f"ERROR: Failed to export table '{table_name}'. Details: {e}")
            # Continue to the next table instead of stopping the whole script
            continue

    print("--- CSV Export Process Finished. ---")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Gold to CSV Export") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
        
    export_gold_layer_to_csv(spark)
    
    spark.stop()