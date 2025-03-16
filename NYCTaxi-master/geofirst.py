from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType
from pyspark.sql.functions import udf, col, round, broadcast
import pygeohash as pgh
import folium

# 🚀 Initialize Spark Session with increased memory
spark = SparkSession.builder \
    .appName("NYC Taxi Data Processing without LocationID") \
    .config("spark.driver.memory", "8g") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()

# Define schemas
yellow_taxi_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),  # FIX: Correct field name
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)
])

green_taxi_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("lpep_pickup_datetime", TimestampType(), True),
    StructField("lpep_dropoff_datetime", TimestampType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("PULocationID", IntegerType(), True),  # FIX: Correct field name
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)
])

fhv_schema = StructType([
    StructField("dispatching_base_num", StringType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("PULocationID", IntegerType(), True)  # FIX: Correct field name
])

taxi_zone_schema = StructType([
    StructField("LocationID", IntegerType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True)
])

# 🚖 Load taxi datasets
yellow_taxi_df = spark.read.csv("D:/New folder/yellow", schema=yellow_taxi_schema, header=True)
green_taxi_df = spark.read.csv("D:/New folder/green", schema=green_taxi_schema, header=True)
fhv_df = spark.read.csv("D:/New folder/fhv", schema=fhv_schema, header=True)

# 🌍 Load taxi zone coordinate data
taxi_zone_coordinates = spark.read.csv("D:/GeoHash/taxizone", schema=taxi_zone_schema, header=True)

# ✅ FIX: Join to get latitude & longitude for yellow & green taxi data
yellow_taxi_df = yellow_taxi_df.join(
    broadcast(taxi_zone_coordinates),
    yellow_taxi_df["PULocationID"] == taxi_zone_coordinates["LocationID"],
    "left"
).drop("LocationID")

green_taxi_df = green_taxi_df.join(
    broadcast(taxi_zone_coordinates),
    green_taxi_df["PULocationID"] == taxi_zone_coordinates["LocationID"],
    "left"
).drop("LocationID")

fhv_df = fhv_df.join(
    broadcast(taxi_zone_coordinates),
    fhv_df["PULocationID"] == taxi_zone_coordinates["LocationID"],
    "left"
).drop("LocationID")

# 🌍 GeoHash function
def generate_geohash(lat, lon, precision=6):
    return None if lat is None or lon is None else pgh.encode(lat, lon, precision)

# 🔧 Register UDF for GeoHash encoding
geohash_udf = udf(generate_geohash, StringType())

# 🏷️ Add GeoHash to taxi trip data
yellow_taxi_df = yellow_taxi_df.withColumn("GeoHash", geohash_udf(col("Latitude"), col("Longitude")))
green_taxi_df = green_taxi_df.withColumn("GeoHash", geohash_udf(col("Latitude"), col("Longitude")))
fhv_df = fhv_df.withColumn("GeoHash", geohash_udf(col("Latitude"), col("Longitude")))

# 🎯 Select relevant columns
yellow_taxi_selected = yellow_taxi_df.select(
    col("tpep_pickup_datetime").alias("pickup_datetime"),
    col("Longitude"),
    col("Latitude"),
    col("GeoHash")
)

green_taxi_selected = green_taxi_df.select(
    col("lpep_pickup_datetime").alias("pickup_datetime"),
    col("Longitude"),
    col("Latitude"),
    col("GeoHash")
)

fhv_selected = fhv_df.select(
    col("pickup_datetime"),
    col("Longitude"),
    col("Latitude"),
    col("GeoHash")
)

# 🔗 Union all DataFrames
taxi_geo_df = yellow_taxi_selected.union(green_taxi_selected).union(fhv_selected)

# ✅ FIX: Reduce dataset size before calling `toPandas()`
taxi_geo_df = taxi_geo_df.limit(10000)  # Adjust limit as needed

# 📍 Convert to Pandas for visualization
taxi_geo_pd = taxi_geo_df.toPandas()

# 🗺️ Create a Folium map centered at NYC
nyc_map = folium.Map(location=[40.7128, -74.0060], zoom_start=11)

# 📌 Add markers to the map
for _, row in taxi_geo_pd.iterrows():
    if row["Latitude"] and row["Longitude"]:
        folium.Marker(
            location=[row["Latitude"], row["Longitude"]],
            popup=f"GeoHash: {row['GeoHash']}",
            icon=folium.Icon(color="blue", icon="info-sign")
        ).add_to(nyc_map)

# 💾 Save map as HTML
nyc_map.save("nyc_taxi_geohash_map.html")

# 🚦 Stop Spark session
spark.stop()

print("✅ GeoHash computation complete. Map saved as nyc_taxi_geohash_map.html")
