from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SchemaInfer").getOrCreate()

# Đọc file parquet tĩnh để lấy schema
df = spark.read.parquet("data/raw/yellow_tripdata_2024-01.parquet")
df.printSchema()
