from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, datediff

spark = SparkSession.builder.appName("RFMAnalysis").getOrCreate()
df = spark.read.parquet("s3://bigdata-retail-analytics-data/processed/cleaned_data.parquet")
max_date = df.agg(max("InvoiceDate")).collect()[0][0]
rfm = df.groupBy("CustomerID").agg(
    datediff(max_date, max("InvoiceDate")).alias("Recency"),
    count("*").alias("Frequency"),
    sum("TotalPrice").alias("Monetary")
)
rfm.show()
spark.stop()
