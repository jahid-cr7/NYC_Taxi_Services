from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType
from pyspark.sql.functions import col
import pandas as pd
import folium
from folium.plugins import HeatMap

# 🚀 Initialize Spark Session with optimized settings
spark = SparkSession.builder \
    .appName("NYC Taxi Heatmap") \
    .config("spark.driver.memory", "8g") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()

# ✅ Define schema for taxi pickup/drop-off data
taxi_data_schema = StructType([
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("trip_count", IntegerType(), True)
])

# 📂 Load taxi trip coordinate data
taxi_data = spark.read.option("header", "true") \
    .csv("D:/GeoHash/taxizone/cordination.csv", schema=taxi_data_schema)

# ✅ Drop null values for safe processing
taxi_data = taxi_data.dropna()

# ✅ Limit data to 5000 points for performance optimization
taxi_data = taxi_data.limit(5000)

# 🏙 Convert to Pandas for HeatMap processing
taxi_pd = taxi_data.toPandas()

# ✅ Ensure only numerical values are passed to HeatMap
taxi_pd = taxi_pd.dropna(subset=["Latitude", "Longitude"])

# 🌍 Create a Folium Map centered in NYC
nyc_map = folium.Map(location=[40.7128, -74.0060], zoom_start=11)

# 🔥 Add HeatMap Layer
heat_data = taxi_pd[["Latitude", "Longitude"]].values.tolist()
HeatMap(heat_data, radius=10, blur=15).add_to(nyc_map)

# 💾 Save heatmap as HTML
nyc_map.save("nyc_taxi_heatmap2.html")

# 🚦 Stop Spark session
spark.stop()

print("✅ Heatmap generated successfully! Saved as nyc_taxi_heatmap.html")
