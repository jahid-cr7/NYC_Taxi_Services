%pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour

# Initialize Spark session (if not already created)
spark = SparkSession.builder.appName("CombinedTaxiVisualization").getOrCreate()

# Define schema for Yellow Taxi data
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType

yellow_taxi_schema = StructType([
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

# Define schema for Green Taxi data
green_taxi_schema = StructType([
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
    StructField("payment_type", IntegerType(), True),
    StructField("trip_type", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True)
])

# Define schema for FHV data
fhv_schema = StructType([
    StructField("dispatching_base_num", StringType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropOff_datetime", TimestampType(), True),
    StructField("PUlocationID", DoubleType(), True),
    StructField("DOlocationID", DoubleType(), True),
    StructField("SR_Flag", StringType(), True),
    StructField("Affiliated_base_number", StringType(), True)
])

# Load Yellow Taxi Data
yellow_taxi_df = spark.read.csv("/data/nyc_taxi/yellow", schema=yellow_taxi_schema, header=True)

# Load Green Taxi Data
green_taxi_df = spark.read.csv("/data/nyc_taxi/green", schema=green_taxi_schema, header=True)

# Load FHV Trip Data
fhv_df = spark.read.csv("/data/nyc_taxi/fhv", schema=fhv_schema, header=True)

# Extract the hour from pickup time and count trips per hour
yellow_trips = yellow_taxi_df \
    .withColumn("hour", hour(col("tpep_pickup_datetime"))) \
    .groupBy("hour") \
    .count() \
    .withColumnRenamed("count", "yellow_trips")

green_trips = green_taxi_df \
    .withColumn("hour", hour(col("lpep_pickup_datetime"))) \
    .groupBy("hour") \
    .count() \
    .withColumnRenamed("count", "green_trips")

fhv_trips = fhv_df \
    .withColumn("hour", hour(col("pickup_datetime"))) \
    .groupBy("hour") \
    .count() \
    .withColumnRenamed("count", "fhv_trips")

# Combine all datasets
combined_trips = yellow_trips \
    .join(green_trips, "hour", "outer") \
    .join(fhv_trips, "hour", "outer") \
    .fillna(0) \
    .orderBy("hour")

# Show sample data
combined_trips.show(24)

# Zeppelin Visualization: Use a Stacked Bar Chart
# - X-Axis: hour
# - Y-Axis: yellow_trips, green_trips, fhv_trips (stacked)
