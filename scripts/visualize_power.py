#!/usr/bin/env python

import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import Point
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from matplotlib.colors import LinearSegmentedColormap

spark = (
    SparkSession.builder
        .appName("EV Charging: Hexbin Power Heatmap")
        .getOrCreate()
)

# Load stations with power
df = (
    spark.read.parquet("output/stations_with_power.parquet")
        .select("lon", "lat", "total_power_kw")
        .na.drop(subset=["lon","lat"])
)
pdf = df.toPandas()

# Build GeoDataFrame and 100 km buffer around Croatia
gpdf = gpd.GeoDataFrame(
    pdf, geometry=gpd.points_from_xy(pdf.lon, pdf.lat), crs="EPSG:4326"
)
world = gpd.read_file(
    "https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson"
)
cro = world[world.name=="Croatia"].to_crs(epsg=3857)
border = cro.geometry.union_all()
buf100 = gpd.GeoSeries([border.buffer(100_000)], crs="EPSG:3857")
buf_ll = buf100.to_crs(epsg=4326).iloc[0]

# Clip points to that buffer
gpdf = gpdf[gpdf.geometry.within(buf_ll)]

# Clip neighbour land and Croatia
neighbors = world[world.name.isin([
    "Slovenia","Bosnia and Herzegovina","Republic of Serbia",
    "Hungary","Montenegro","Italy","Austria",
    "North Macedonia","Kosovo","Albania"
])].to_crs(epsg=4326)
cro_clip   = cro.to_crs(epsg=4326).clip(buf_ll)
neigh_clip = neighbors.clip(buf_ll)

# Plot everything
fig, ax = plt.subplots(1,1, figsize=(10,10), dpi=150)

# sea
gpd.GeoSeries([buf_ll], crs="EPSG:4326") \
    .plot(ax=ax, color="lightblue", edgecolor="none")

# neighbours
neigh_clip.plot(ax=ax, color="lightgray", edgecolor="black", linewidth=0.5)

# croatia
cro_clip.plot(ax=ax, color="lightgreen", edgecolor="black", linewidth=1, alpha=0.5)

# hexbin heatmap: sum total_power_kw per hex, drop bins <1 kW
cmap = LinearSegmentedColormap.from_list("R2G", ["red","yellow","green"])
hb = ax.hexbin(
    gpdf.lon, gpdf.lat,
    C=gpdf.total_power_kw,
    reduce_C_function=np.sum,
    gridsize=18,
    cmap=cmap,
    mincnt=1,
    linewidths=0.2,
    edgecolors="white",
    alpha=0.8
)
cb = fig.colorbar(hb, ax=ax, shrink=0.6, pad=0.02)
cb.set_label("Total Installed Power (kW)")

# lon, lat for some big cities in Croatia
cities = {
    "Zagreb": (15.9819, 45.8150),
    "Split": (16.4402, 43.5081),
    "Rijeka": (14.4378, 45.3271),
    "Osijek": (18.6939, 45.5540),
    "Zadar": (15.2300, 44.1194),
    "Pula":  (13.8496, 44.8666),
    "Dubrovnik": (18.0864, 42.6507)
}

for name, (lon, lat) in cities.items():
    if buf_ll.contains(Point(lon, lat)):
        ax.scatter(lon, lat,
                   color="black", s=10, marker="o",
                   zorder=7)
        ax.text(
            lon, lat, " "+name,
            ha="left", va="center",
            color="black",
            fontsize=9,
            fontweight="bold",
            zorder=8
        )

ax.set_xlim(buf_ll.bounds[0], buf_ll.bounds[2])
ax.set_ylim(buf_ll.bounds[1], buf_ll.bounds[3])
ax.set_axis_off()
ax.set_title(
    "EV Charging: Total Installed Power\nHexbin Heatmap (100 km around Croatia)",
    pad=20
)

plt.tight_layout()
plt.show()

spark.stop()
