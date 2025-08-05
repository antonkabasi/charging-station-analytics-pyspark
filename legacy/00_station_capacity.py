#!/usr/bin/env python

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_extract
import pyspark.sql.types as T

spark = (
    SparkSession.builder
        .appName("Inspect EV Station Capacities")
        .getOrCreate()
)

df = spark.read.parquet("output/croatia_charging_stations.parquet")

json_strings = (
    df
    .select("tags")
    .where("tags IS NOT NULL")
    .rdd
    .map(lambda row: row.tags)
)
sample = spark.read.json(json_strings)
sample.printSchema()

fields = [T.StructField(f.name, T.StringType(), True) for f in sample.schema.fields]
tags_schema = T.StructType(fields)

# Parse the tags column into a struct 
df2 = df.withColumn("tag_struct", from_json(col("tags"), tags_schema))

#    malformed or non-numeric entries become NULL
df3 = df2.withColumn(
    "capacity_kw",
    regexp_extract(col("tag_struct.capacity"), r"(\d+)", 1).cast("int")
)

# How many stations report a parsable capacity?
print("Stations with an integer capacity (kW):")
df3.where(col("capacity_kw").isNotNull()) \
   .agg({"capacity_kw": "count"}) \
   .show()

# Distribution of those capacities
print("Capacity distribution (kW):")
df3.where(col("capacity_kw").isNotNull()) \
   .groupBy("capacity_kw") \
   .count() \
   .orderBy("capacity_kw") \
   .show(50)

spark.stop()
