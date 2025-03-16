%pyspark
# Load the taxi_zone_lookup file
taxi_zone_lookup = spark.read.csv("/data/nyc_taxi/taxiZone", header=True, inferSchema=True)

# Load the taxi_zone_coordinates file (with Longitude and Latitude)
taxi_zone_coordinates = spark.read.csv("/data/nyc_taxi/taxicordination", header=True, inferSchema=True)

# Join taxi_zone_lookup with taxi_zone_coordinates
taxi_zone_lookup = taxi_zone_lookup.join(
    taxi_zone_coordinates,
    taxi_zone_lookup.LocationID == taxi_zone_coordinates.LocationID,
    "inner"
)

# Perform the join operation for yellow_taxi_df
yellow_taxi_mapped = yellow_taxi_df.join(
    taxi_zone_lookup,
    yellow_taxi_df.PULocationID == taxi_zone_lookup.LocationID,
    "inner"
)

# Perform the join operation for green_taxi_df
green_taxi_mapped = green_taxi_df.join(
    taxi_zone_lookup,
    green_taxi_df.PULocationID == taxi_zone_lookup.LocationID,
    "inner"
)

# Cast PUlocationID to integer for compatibility with LocationID in fhv
fhv = fhv.withColumn("PUlocationID", fhv.PUlocationID.cast("integer"))

# Perform the join operation for fhv
fhv_mapped = fhv.join(
    taxi_zone_lookup,
    fhv.PUlocationID == taxi_zone_lookup.LocationID,
    "inner"
)

# Combine the results into a single DataFrame
# Select relevant columns (including Longitude and Latitude)
yellow_taxi_selected = yellow_taxi_mapped.select(
    yellow_taxi_mapped.tpep_pickup_datetime.alias("pickup_datetime"),
    taxi_zone_lookup.Longitude,
    taxi_zone_lookup.Latitude
)

green_taxi_selected = green_taxi_mapped.select(
    green_taxi_mapped.lpep_pickup_datetime.alias("pickup_datetime"),
    taxi_zone_lookup.Longitude,
    taxi_zone_lookup.Latitude
)

fhv_selected = fhv_mapped.select(
    fhv_mapped.pickup_datetime,
    taxi_zone_lookup.Longitude,
    taxi_zone_lookup.Latitude
)

# Union all DataFrames
taxi_geo_df = yellow_taxi_selected.union(green_taxi_selected).union(fhv_selected)

# Show the schema and first 5 rows of the combined DataFrame
print("Schema of taxi_geo_df:")
taxi_geo_df.printSchema()

print("First 5 rows of taxi_geo_df:")
taxi_geo_df.show(5)