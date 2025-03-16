from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, to_date
from pyspark.sql.types import StringType
import geohash2 as geohash
import os

# ✅ Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("NYC_Green_Taxi_ETL") \
    .getOrCreate()

# ✅ Step 2: Define Paths
input_folder = "C:/Users/alami/Downloads/Update 2023/2024csv/green"  # Folder containing all CSV files
output_folder = "D:/New folder/green"  # Folder to save cleaned CSV

# ✅ Step 3: Load All CSV Files
df = spark.read.csv(f"{input_folder}/*.csv", header=True, inferSchema=True)

# ✅ Step 4: Data Cleaning & Filtering

# 1️⃣ Drop Rows with NULL values
df = df.dropna()

# 2️⃣ Remove Outliers (Negative fares, trip distance > 100 miles)
df = df.filter(
    (col("fare_amount") > 0) &
    (col("trip_distance") > 0) &
    (col("trip_distance") < 100) &
    (col("passenger_count") > 0)
)

# 3️⃣ Convert Timestamp to Date
df = df.withColumn("pickup_date", to_date(col("lpep_pickup_datetime")))
df = df.withColumn("dropoff_date", to_date(col("lpep_dropoff_datetime")))

# 4️⃣ Drop Unnecessary Columns
columns_to_drop = ["ehail_fee"]  # Drop 'ehail_fee' as it's mostly null
df = df.drop(*columns_to_drop)

# ✅ Step 5: Convert PULocationID & DOLocationID to GeoHash

# Function to encode GeoHash
def geohash_encode(location_id, precision=6):
    if location_id is None:
        return None
    return geohash.encode(location_id, location_id, precision)  # Fake GeoHash (use lat/lon if available)

# Register as UDF
geohash_udf = udf(geohash_encode, StringType())

# Apply GeoHash Transformation
df = df.withColumn("pickup_geohash", geohash_udf(col("PULocationID")))
df = df.withColumn("dropoff_geohash", geohash_udf(col("DOLocationID")))

# ✅ Step 6: Save the Cleaned Data to Output Folder
df.write.mode("overwrite").option("header", True).csv(output_folder)

# ✅ Step 7: Stop Spark Session
spark.stop()

print(f"✅ Cleaned data saved to: {output_folder}")
