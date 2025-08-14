#!/usr/bin/env python

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, regexp_extract, array, array_max, lit
)
import pyspark.sql.types as T

spark = (
    SparkSession.builder
        .appName("Extract Plug Types and Total Power")
        .getOrCreate()
)

df = spark.read.parquet("output/croatia_charging_stations.parquet")

# Infer and apply JSON schema
sample = spark.read.json(
    df.select("tags").rdd.map(lambda r: r.tags).filter(lambda x: x is not None)
)
schema = T.StructType(sample.schema.fields)
df = df.withColumn("t", from_json("tags", schema))

# Extract integer capacity or default to 0
df = df.withColumn(
    "capacity",
    regexp_extract(col("t.capacity"), r"(\d+)", 1).cast("int")
).fillna({"capacity": 0})

# Discover all socket base keys (no ":output")
all_keys = set(schema.fieldNames())
plug_keys = [k for k in all_keys if k.startswith("socket:") and not k.endswith(":output")]

# Build list of output‐tags that actually exist
candidates = [f"{k}:output" for k in plug_keys] + ["charging_station:output", "power"]
output_keys = [k for k in candidates if k in all_keys]

# Parse each to int
out_cols = [
    regexp_extract(col(f"t.`{k}`"), r"(\d+)", 1).cast("int")
    for k in output_keys
]

df = df.withColumn("outs", array(*out_cols))

# Take the maximum per‐plug power and compute total
df = df.withColumn("per_plug_kw", array_max("outs")) \
       .withColumn("total_power_kw", col("capacity") * col("per_plug_kw"))

# Write out slimmed table
df.select(
    "id", "lon", "lat",
    "capacity", "per_plug_kw", "total_power_kw",
    "name", "operator"
).write.mode("overwrite").parquet("output/stations_with_power.parquet")

spark.stop()
