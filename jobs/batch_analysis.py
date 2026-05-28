import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder.appName("BatchAnalysis").getOrCreate()

df = spark.read.csv("/opt/project/data/stock_prices_daily.csv", header=True, inferSchema=True)

df_volatility = df.withColumn("Volatility", col("High") - col("Low"))

df_result = df_volatility.groupBy("Sector").agg(
    avg("Volatility").alias("Average_Volatility")
).orderBy(col("Average_Volatility").desc())

results = [row.asDict() for row in df_result.collect()]

with open("/opt/project/dashboard_data/batch_result.json", "w") as f:
    json.dump(results, f, indent=4)

spark.stop()
