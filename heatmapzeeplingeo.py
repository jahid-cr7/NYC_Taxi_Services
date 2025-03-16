%pyspark

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import folium
import geohash2  # Install using %sh pip install geohash2

# Convert PySpark DataFrame to Pandas for visualization
result_pd = result.toPandas()

# Sort by trip count
result_pd = result_pd.sort_values(by="trip_count", ascending=False)

# -------------------------------
# 📊 1️⃣ BAR CHART (Top 20 GeoHash regions by trip count)
# -------------------------------
plt.figure(figsize=(12, 6))
plt.bar(result_pd["geohash"][:20], result_pd["trip_count"][:20], color="skyblue")
plt.xticks(rotation=90)
plt.xlabel("GeoHash")
plt.ylabel("Trip Count")
plt.title("Top 20 GeoHash Locations by Trip Count")
plt.show()

# -------------------------------
# 🔥 2️⃣ HEATMAP (Trip Density Heatmap)
# -------------------------------
plt.figure(figsize=(12, 6))
sns.heatmap(result_pd.pivot_table(values="trip_count", index="geohash", aggfunc="sum"), cmap="coolwarm")
plt.xlabel("GeoHash")
plt.ylabel("Trip Count")
plt.title("Trip Density Heatmap")
plt.show()

# -------------------------------
# 🌍 3️⃣ INTERACTIVE MAP (Folium)
# -------------------------------
# Create base map centered at NYC
nyc_map = folium.Map(location=[40.7128, -74.0060], zoom_start=12)

# Add trip density markers
for index, row in result_pd.iterrows():
    lat, lon = geohash2.decode(row["geohash"])  # Decode GeoHash
    folium.CircleMarker(
        location=[lat, lon],
        radius=row["trip_count"] / 1000000,  # Adjust size dynamically
        color="red",
        fill=True,
        fill_opacity=0.6
    ).add_to(nyc_map)

# Save and display map
map_path = "/data/nyc_taxi/output_map.html"
nyc_map.save(map_path)
displayHTML(f'<iframe src="{map_path}" width="100%" height="500px"></iframe>')

%pyspark

# Define HDFS output path
hdfs_output_path = "hdfs:///user/hadoop/taxi_geo_data"

# Write DataFrame to HDFS in CSV format
taxi_geo_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(hdfs_output_path)

print(f"✅ File successfully saved to {hdfs_output_path}")
