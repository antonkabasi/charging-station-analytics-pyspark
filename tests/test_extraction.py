import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .appName("Test Extraction")
        .master("local[1]")
        .getOrCreate()
    )

def test_load_parquet(spark):
    df = spark.read.parquet("output/croatia_charging_stations.parquet")
    assert df.count() > 0
    assert "lon" in df.columns and "lat" in df.columns
