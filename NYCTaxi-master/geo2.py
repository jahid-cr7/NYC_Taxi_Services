from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql.functions import udf, col
import pygeohash as pgh
import folium
from folium.plugins import MarkerCluster

# 🚀 Initialize Spark Session with increased memory
spark = SparkSession.builder \
    .appName("NYC Taxi Data Processing") \
    .config("spark.driver.memory", "8g") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()

# ✅ Define schema for taxizone dataset
taxi_zone_schema = StructType([
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("trip_count", IntegerType(), True)
])

# ✅ Load taxi zone coordinate data with error handling
taxi_zone_coordinates = spark.read.option("mode", "DROPMALFORMED") \
    .csv("D:/GeoHash/taxizone/cordination.csv", schema=taxi_zone_schema, header=True)

# ✅ Define and register UDF for GeoHash encoding
def generate_geohash(lat, lon, precision=6):
    return None if lat is None or lon is None else pgh.encode(lat, lon, precision)

geohash_udf = udf(generate_geohash, StringType())

# ✅ Add GeoHash to dataset
taxi_zone_coordinates = taxi_zone_coordinates.withColumn(
    "GeoHash", geohash_udf(col("Latitude"), col("Longitude"))
)

# ✅ Reduce dataset size before converting to Pandas (adjust limit as needed)
taxi_zone_coordinates = taxi_zone_coordinates.limit(5000)  # Reduce to 5,000 for faster performance

# ✅ Convert to Pandas for visualization
taxi_geo_pd = taxi_zone_coordinates.toPandas()

# ✅ Drop NaN values to avoid Folium crash
taxi_geo_pd = taxi_geo_pd.dropna(subset=["Latitude", "Longitude"])

# 🗺️ Create a Folium map centered at NYC
nyc_map = folium.Map(location=[40.7128, -74.0060], zoom_start=11)

# 🔥 Use MarkerCluster for better performance
marker_cluster = MarkerCluster().add_to(nyc_map)

# 📌 Add markers to the cluster
for _, row in taxi_geo_pd.iterrows():
    folium.Marker(
        location=[row["Latitude"], row["Longitude"]],
        popup=f"GeoHash: {row['GeoHash']}",
        icon=folium.Icon(color="blue", icon="info-sign")
    ).add_to(marker_cluster)

# 💾 Save optimized map as HTML
map_path = "nyc_taxi_geohash_map2.html"
nyc_map.save(map_path)

# 🚦 Stop Spark session
spark.stop()

print(f"✅ GeoHash computation complete. Map saved at: {map_path}")
