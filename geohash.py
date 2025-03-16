from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType
from pyspark.sql.functions import udf, col, broadcast
import pygeohash as pgh
import folium

# 🚀 Initialize Spark Session with increased memory
spark = SparkSession.builder \
    .appName("NYC Taxi Data Processing") \
    .config("spark.driver.memory", "8g") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()

# Define correct schema for taxizone dataset
taxi_zone_schema = StructType([
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("trip_count", IntegerType(), True)
])

# ✅ FIX: Load taxi zone coordinate data with error handling
taxi_zone_coordinates = spark.read.option("mode", "DROPMALFORMED") \
    .csv("D:/GeoHash/taxizone/cordination.csv", schema=taxi_zone_schema, header=True)

# 🌍 GeoHash function
def generate_geohash(lat, lon, precision=6):
    return None if lat is None or lon is None else pgh.encode(lat, lon, precision)

# 🔧 Register UDF for GeoHash encoding
geohash_udf = udf(generate_geohash, StringType())

# ✅ FIX: Add GeoHash to dataset
taxi_zone_coordinates = taxi_zone_coordinates.withColumn(
    "GeoHash", geohash_udf(col("Latitude"), col("Longitude"))
)

# ✅ FIX: Reduce dataset size before calling `toPandas()`
taxi_zone_coordinates = taxi_zone_coordinates.limit(10000)

# 📍 Convert to Pandas for visualization
taxi_geo_pd = taxi_zone_coordinates.toPandas()

# ✅ FIX: Drop NaN values to avoid Folium crash
taxi_geo_pd = taxi_geo_pd.dropna(subset=["Latitude", "Longitude"])

# 🗺️ Create a Folium map centered at NYC
nyc_map = folium.Map(location=[40.7128, -74.0060], zoom_start=11)

# 📌 Add markers to the map
for _, row in taxi_geo_pd.iterrows():
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
# Stop Spark session properly
spark.stop()
print("✅ Spark session stopped successfully.")
