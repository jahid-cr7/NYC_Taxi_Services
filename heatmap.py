from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql.functions import col
import folium
from folium.plugins import HeatMap

# 🚀 Initialize Spark Session with increased memory
spark = SparkSession.builder \
    .appName("NYC Taxi Heatmap") \
    .config("spark.driver.memory", "8g") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()

# ✅ Define correct schema for taxizone dataset
taxi_zone_schema = StructType([
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("trip_count", IntegerType(), True)
])

# 📥 Load taxi zone coordinate data
taxi_zone_coordinates = spark.read.option("mode", "DROPMALFORMED") \
    .csv("D:/GeoHash/taxizone/cordination.csv", schema=taxi_zone_schema, header=True)

# ✅ Reduce dataset size for better performance
taxi_zone_coordinates = taxi_zone_coordinates.limit(5000)

# 📍 Convert to Pandas for visualization
taxi_geo_pd = taxi_zone_coordinates.toPandas()

# ✅ Drop NaN values to prevent errors
taxi_geo_pd = taxi_geo_pd.dropna(subset=["Latitude", "Longitude"])

# 🗺️ Create a Folium map centered at NYC
nyc_map = folium.Map(location=[40.7128, -74.0060], zoom_start=11)

# 🔥 Add HeatMap layer
heat_data = taxi_geo_pd[["Latitude", "Longitude"]].values.tolist()
HeatMap(heat_data, radius=10, blur=15, max_zoom=1).add_to(nyc_map)

# 💾 Save map as HTML
nyc_map.save("nyc_taxi_heatmap.html")

# 🚦 Stop Spark session
spark.stop()

print("✅ Heatmap generated successfully! Saved as nyc_taxi_heatmap.html")
