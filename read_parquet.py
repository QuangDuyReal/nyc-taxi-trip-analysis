import pandas as pd

# read BRONZE
print("=== BRONZE LAYER DATA ===")
df = pd.read_parquet('data/bronze/yellow_taxi_trips/year=2024/month=1/part-00009-14f8233a-a055-4e8f-a456-c745f7a11c69.c000.snappy.parquet')
print(df.describe())
print(df.info())
print(df.head())

# read SILVER
print("=== SILVER LAYER DATA ===")
df = pd.read_parquet('data/silver/taxi_trips/pickup_year=2024/pickup_month=1/part-00009-e1cdd698-63e9-4f86-942e-b276f18130fc.c000.snappy.parquet')
print(df.describe())
print(df.info())
print(df.head())

# read GOLD
print("=== GOLD LAYER DATA ===")
df = pd.read_parquet('data/gold/hourly_trip_analytics/pickup_year=2024/pickup_month=1/part-00000-13cf77ca-b1a6-4d9c-b677-21d3487aa7b0.c000.snappy.parquet')
print(df.describe())
print(df.info())
print(df.head())

# read STREAMING_INPUT
print("=== STREAMING INPUT DATA ===")
df = pd.read_parquet('data/streaming_input/batch_0_1753789604.parquet')
print(df.describe())
print(df.info())
print(df.head())

# read STREAMING_OUTPUT
print("=== STREAMING OUTPUT DATA ===")
df = pd.read_parquet('data/streaming_output/time_metrics/part-00063-03787a60-cfdc-4790-a753-72d3c1b062cd-c000.snappy.parquet')
print(df.describe())
print(df.info())
print(df.head())