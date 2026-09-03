from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("AaysDataEngineering")
    .master("local[*]")
    .getOrCreate()
)

print("Spark version:", spark.version)

spark.stop()