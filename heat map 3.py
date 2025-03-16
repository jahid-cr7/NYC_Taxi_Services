%pyspark

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType

# Define schema for yellow_taxi_df
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

# Define schema for green_taxi_df
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
    StructField("payment_type", DoubleType(), True),
    StructField("trip_type", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True)
])

# Define schema for fhv
fhv_schema = StructType([
    StructField("dispatching_base_num", StringType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropOff_datetime", TimestampType(), True),
    StructField("PUlocationID", DoubleType(), True),
    StructField("DOlocationID", DoubleType(), True),
    StructField("SR_Flag", StringType(), True),
    StructField("Affiliated_base_number", StringType(), True)
])

# Read taxi data with defined schemas
yellow_taxi_df = spark.read.csv("/data/nyc_taxi/yellow", schema=yellow_taxi_schema, header=True)
green_taxi_df = spark.read.csv("/data/nyc_taxi/green", schema=green_taxi_schema, header=True)
fhv_df = spark.read.csv("/data/nyc_taxi/fhv", schema=fhv_schema, header=True)

# Load taxi zone lookup and coordinates
taxi_zone_lookup = spark.read.csv("/data/nyc_taxi/taxiZone", header=True, inferSchema=True)
taxi_zone_coordinates = spark.read.csv("/data/nyc_taxi/taxicordination", header=True, inferSchema=True)

# Join taxi zone lookup with coordinates
taxi_zone_lookup = taxi_zone_lookup.alias("tz").join(
    taxi_zone_coordinates.alias("tzc"),
    "LocationID",
    "inner"
)

# Alias DataFrames to avoid column ambiguity
taxi_zone_lookup_alias = taxi_zone_lookup.alias("tz")

# Perform the join operation for yellow_taxi_df
yellow_taxi_mapped = yellow_taxi_df.alias("yt").join(
    taxi_zone_lookup_alias,
    yellow_taxi_df.PULocationID == taxi_zone_lookup_alias.LocationID,
    "inner"
).drop(taxi_zone_lookup_alias.LocationID)

# Perform the join operation for green_taxi_df
green_taxi_mapped = green_taxi_df.alias("gt").join(
    taxi_zone_lookup_alias,
    green_taxi_df.PULocationID == taxi_zone_lookup_alias.LocationID,
    "inner"
).drop(taxi_zone_lookup_alias.LocationID)

# Cast PUlocationID to integer for compatibility with LocationID in fhv
fhv_df = fhv_df.withColumn("PUlocationID", fhv_df.PUlocationID.cast("integer"))

# Perform the join operation for fhv_df
fhv_mapped = fhv_df.alias("fhv").join(
    taxi_zone_lookup_alias,
    fhv_df.PUlocationID == taxi_zone_lookup_alias.LocationID,
    "inner"
).drop(taxi_zone_lookup_alias.LocationID)

# Select relevant columns (including Longitude and Latitude)
yellow_taxi_selected = yellow_taxi_mapped.select(
    yellow_taxi_mapped.tpep_pickup_datetime.alias("pickup_datetime"),
    taxi_zone_lookup_alias.Longitude,
    taxi_zone_lookup_alias.Latitude
)

green_taxi_selected = green_taxi_mapped.select(
    green_taxi_mapped.lpep_pickup_datetime.alias("pickup_datetime"),
    taxi_zone_lookup_alias.Longitude,
    taxi_zone_lookup_alias.Latitude
)

fhv_selected = fhv_mapped.select(
    fhv_mapped.pickup_datetime,
    taxi_zone_lookup_alias.Longitude,
    taxi_zone_lookup_alias.Latitude
)

# Union all DataFrames
taxi_geo_df = yellow_taxi_selected.union(green_taxi_selected).union(fhv_selected)

# Show the schema and first 5 rows of the combined DataFrame
print("Schema of taxi_geo_df:")
taxi_geo_df.printSchema()

print("First 5 rows of taxi_geo_df:")
taxi_geo_df.show(5)
%pyspark

# Register the DataFrame as a temporary view
taxi_geo_df.createOrReplaceTempView("taxi_geo_df")

# Query the temporary view using Spark SQL
result = spark.sql("""
    SELECT Latitude, Longitude, COUNT(*) AS trip_count
    FROM taxi_geo_df
    GROUP BY Latitude, Longitude
    ORDER BY trip_count DESC
""")

# Show the result
result.show()