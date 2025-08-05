#!/usr/bin/env python

import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import Point
from pyspark.sql import SparkSession, functions as F
from matplotlib.colors import LinearSegmentedColormap

spark = (
    SparkSession.builder
        .appName("EV Charging Station Bubble Map")
        .getOrCreate()
)

# Load & clean via Spark 
df = (
    spark.read.parquet("output/croatia_charging_stations.parquet")
         .select("lon", "lat")
         .na.drop(subset=["lon","lat"])
)

# Compute Croatia bounding box & 100 km buffer
b = df.agg(
    F.min("lon").alias("xmin"), F.max("lon").alias("xmax"),
    F.min("lat").alias("ymin"), F.max("lat").alias("ymax")
).first()
xmin, xmax, ymin, ymax = b.xmin, b.xmax, b.ymin, b.ymax

world = gpd.read_file(
    "https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson"
)
# Croatia in metric CRS
cro = world[world.name=="Croatia"].to_crs(epsg=3857)
border = cro.geometry.unary_union.boundary
buf100 = gpd.GeoSeries([border.buffer(100_000)], crs="EPSG:3857")
buf_ll = buf100.to_crs(epsg=4326).iloc[0]
minx, miny, maxx, maxy = buf_ll.bounds

# toPandas & clip points to that buffer
pdf = df.toPandas()
gpdf = gpd.GeoDataFrame(
    pdf, geometry=gpd.points_from_xy(pdf.lon, pdf.lat), crs="EPSG:4326"
)
gpdf = gpdf[gpdf.geometry.within(buf_ll)]

# Clip neighbour land and Croatia
neighbors = world[world.name.isin([
    "Slovenia","Bosnia and Herzegovina","Republic of Serbia",
    "Hungary","Montenegro","Italy","Austria",
    "North Macedonia","Kosovo","Albania"
])].to_crs(epsg=4326)

cro_clip   = cro.to_crs(epsg=4326).clip(buf_ll)
neigh_clip = neighbors.clip(buf_ll)

# Build a 25×25 grid & count points per cell 
nx, ny = 14, 14
x_edges = np.linspace(minx, maxx, nx+1)
y_edges = np.linspace(miny, maxy, ny+1)
H, _, _ = np.histogram2d(
    gpdf.lon, gpdf.lat,
    bins=[x_edges, y_edges]
)
# compute cell centers
x_centers = (x_edges[:-1] + x_edges[1:]) / 2
y_centers = (y_edges[:-1] + y_edges[1:]) / 2
XC, YC = np.meshgrid(x_centers, y_centers)
counts = H.T.flatten()
pts   = np.vstack([XC.flatten(), YC.flatten()]).T

# keep only nonzero
mask = counts > 0
pts = pts[mask]
cts = counts[mask].astype(int)

# Plot everything 
fig, ax = plt.subplots(1,1, figsize=(10,10), dpi=150)

# sea
gpd.GeoSeries([buf_ll], crs="EPSG:4326") \
    .plot(ax=ax, color="lightblue", edgecolor="none")

# neighbours
neigh_clip.plot(ax=ax, color="lightgray", edgecolor="black", linewidth=0.5)

# croatia
cro_clip.plot(ax=ax, color="lightgreen", edgecolor="black", linewidth=1, alpha=0.5)

# bubbles
# size scale: tweak 20 for bigger/smaller circles
sizes = cts * 20
red2green = LinearSegmentedColormap.from_list("R2G", ["red","yellow","green"])
sc = ax.scatter(
     pts[:,0], pts[:,1],
     s=sizes,
     c=cts,
     cmap=red2green,        
     alpha=0.8,
     edgecolor="white",
     linewidth=0.5,
     zorder=5)
 # add a colorbar for the bubbles
cb2 = fig.colorbar(sc, ax=ax, shrink=0.6, pad=0.02)
cb2.set_label("Charging station number")

for x, y, count in zip(pts[:,0], pts[:,1], cts):
    ax.text(
        x, y, str(count),
        ha="center", va="center",
        color="white",        # <- direct override
        fontsize=8,
        fontfamily="arial",
        fontweight="bold",
        zorder=6
    )


ax.set_xlim(minx, maxx)
ax.set_ylim(miny, maxy)
ax.set_axis_off()
ax.set_title("EV Charging Station Map Croatia", pad=20)

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
    # only plot those inside our 100 km buffer
    if buf_ll.contains(Point(lon, lat)):
        ax.scatter(lon, lat, 
                   color="black", s=10, marker="o", 
                   zorder=7)
        ax.text(
            lon, lat, " ",
            ha="left", va="center",
            color="black",
            fontsize=9,
            fontweight="bold",
            zorder=8
        )
plt.tight_layout()
plt.show()

spark.stop()
