from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col

# ✅ 1. Initialize Spark Session
spark = SparkSession.builder.appName("GreenTaxiETL").getOrCreate()

# ✅ 2. Define Input & Output Paths
input_folder = "D:/raw/green" #D:\raw\green
output_folder = "D:/New folder/green" #D:\New folder\green

# ✅ 3. Define Explicit Schema
schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("lpep_pickup_datetime", TimestampType(), True),
    StructField("lpep_dropoff_datetime", TimestampType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("ehail_fee", StringType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", DoubleType(), True),
    StructField("trip_type", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True)
])

# ✅ 4. Read All CSV Files from Folder with Schema
df = spark.read.csv(f"{input_folder}/*.csv", header=True, schema=schema)

# ✅ 5. Drop Completely Empty Rows
df = df.dropna(how="all")

# ✅ 6. Handle Missing & Incorrect Values
df = df.fillna({"passenger_count": 1, "trip_distance": 0, "fare_amount": 0})

df = df.filter(
    (col("passenger_count") > 0) &
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0)
)

# ✅ 7. Remove Outliers (e.g., trips over 100 miles)
df = df.filter(col("trip_distance") < 100)

# ✅ 8. Show Schema and Sample Data
df.printSchema()
df.show(5)

# ✅ 9. Save Cleaned Data as CSV
df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_folder)

print("✅ Data cleaning complete! Check your output folder.")
