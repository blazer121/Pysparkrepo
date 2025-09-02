from pyspark.sql import SparkSession
from pyspark_repo.transform import run_transformation

def create_spark_session():
    spark = SparkSession.builder.master("local").appName("CI/CD Example").getOrCreate()
    return spark

def run_transformation(spark):
    data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
    df = spark.createDataFrame(data, ["Name", "Value"])
    df_transformed = df.filter(df["Value"] > 1)  # Example transformation
    return df_transformed

if __name__ == "__main__":
    spark = create_spark_session()
    result = run_transformation(spark)
    result.show()
