from pyrosm import OSM
import pyarrow.parquet as pq
import pyarrow as pa
import os

# Path to the downloaded .osm.pbf file
PBF_FILE = "croatia-latest.osm.pbf"

# Load OSM
osm = OSM(PBF_FILE)

# Filter for nodes and ways tagged as charging stations
charging = osm.get_pois(custom_filter={"amenity": ["charging_station"]})

if charging is None or charging.empty:
    print("No charging stations found.")
    exit()

df = charging.drop(columns="geometry")

# Save to Parquet
table = pa.Table.from_pandas(df)
os.makedirs("output", exist_ok=True)
pq.write_table(table, "output/croatia_charging_stations.parquet")

print(f"Saved {len(df)} records to 'output/croatia_charging_stations.parquet'")
