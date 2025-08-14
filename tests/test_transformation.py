import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .appName("Test Transformation")
        .master("local[1]")
        .getOrCreate()
    )

def test_total_power_column(spark):
    df = spark.read.parquet("output/stations_with_power.parquet")
    assert "total_power_kw" in df.columns
    assert df.filter(df.total_power_kw > 0).count() > 0
