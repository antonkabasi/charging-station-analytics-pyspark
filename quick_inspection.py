import pyarrow.parquet as pq

table = pq.read_table("output/croatia_charging_stations.parquet")
print(table.schema)