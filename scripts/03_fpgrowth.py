from pyspark.sql import SparkSession
from pyspark.ml.fpm import FPGrowth

spark = SparkSession.builder.appName("MarketBasketAnalysis").getOrCreate()
df = spark.read.parquet("s3://bigdata-retail-analytics-data/processed/cleaned_data.parquet")
baskets = df.groupBy("InvoiceNo").agg(collect_set("Description").alias("items"))
fpGrowth = FPGrowth(itemsCol="items", minSupport=0.005, minConfidence=0.2)
model = fpGrowth.fit(baskets)
model.freqItemsets.show()
model.associationRules.show()
spark.stop()
