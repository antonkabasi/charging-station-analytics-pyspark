#!/usr/bin/env python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode, map_keys
import pyspark.sql.types as T

spark = (SparkSession.builder
    .appName("Explore Charging Station Tags")
    .getOrCreate())

df = spark.read.parquet("output/croatia_charging_stations.parquet")

# Extract just the non-null JSON strings into an RDD
json_strings = (df
    .select("tags")
    .where("tags IS NOT NULL")
    .rdd
    .map(lambda row: row.tags)
)

# Read that RDD as JSON to infer schema
sample = spark.read.json(json_strings)
sample.printSchema()

# Build a MapType schema from it: every field→string
fields = [T.StructField(f.name, f.dataType, True) for f in sample.schema.fields]
tags_schema = T.StructType(fields)

# Parse tags column into a struct / map
df2 = df.withColumn("tag_struct", from_json("tags", tags_schema))

spark.stop()
