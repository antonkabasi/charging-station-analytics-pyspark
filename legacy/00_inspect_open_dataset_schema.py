from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
      .appName("InspectChargingSchema")
      .getOrCreate()
)

# load the file
df = spark.read.parquet("output/croatia_charging_stations.parquet")

# show the schema 
df.printSchema()

spark.stop()
