from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col

# ✅ 1. Initialize Spark Session
spark = SparkSession.builder.appName("YellowTaxiETL").getOrCreate()

# ✅ 2. Define Input & Output Paths
input_folder = "D:/raw/yellow"  #D:\raw\yellow Change to your yellow taxi data folder
output_folder = "D:/New folder/yellow"  #D:\New folder\yellow Change to your output folder

# ✅ 3. Define Explicit Schema (for Yellow Taxi)
schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True)
])

# ✅ 4. Read All CSV Files from Folder with Schema
df = spark.read.csv(f"{input_folder}/*.csv", header=True, schema=schema)

# ✅ 5. Drop Completely Empty Rows
df = df.dropna(how="all")

# ✅ 6. Handle Missing & Incorrect Values
df = df.fillna({"passenger_count": 1, "trip_distance": 0, "fare_amount": 0})

# ✅ 7. Filter Invalid Data
df = df.filter(
    (col("passenger_count") > 0) &  # Valid passenger count
    (col("trip_distance") > 0) &  # Trip must have a positive distance
    (col("fare_amount") > 0)  # Fare must be positive
)

# ✅ 8. Remove Outliers (e.g., trips over 100 miles)
df = df.filter(col("trip_distance") < 100)

# ✅ 9. Show Schema and Sample Data
df.printSchema()
df.show(5)

# ✅ 10. Save Cleaned Data as CSV (without deleting the folder)
df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_folder)

print("✅ Data cleaning complete! Check your output folder.")
