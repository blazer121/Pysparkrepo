from pyspark.sql import SparkSession
from transform import run_transformation

def test_run_transformation():
    spark = SparkSession.builder.master("local").appName("CI/CD Test").getOrCreate()
    result = run_transformation(spark)
    assert result.count() == 2  # Should return 2 rows (Bob, Charlie)
