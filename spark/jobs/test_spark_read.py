from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test-read").getOrCreate()
df = spark.read.option("header", True).csv("/opt/data/World-food-waste_cleaned_final.csv")
df.printSchema()
df.show(5, truncate=False)
print("rows:", df.count())
spark.stop()