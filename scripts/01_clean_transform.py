from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CleanTransform").getOrCreate()
df = spark.read.csv("s3://bigdata-retail-analytics-data/raw/online_retail.csv", header=True, inferSchema=True)
df = df.dropna().filter("Quantity > 0 AND UnitPrice > 0").withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))
df.write.mode("overwrite").parquet("s3://bigdata-retail-analytics-data/processed/cleaned_data.parquet")
spark.stop()
