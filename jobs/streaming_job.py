import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    window,
    sum as spark_sum,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "stock-market-stream")
OUTPUT_DIR = os.getenv("DASHBOARD_DIR", "/opt/project/dashboard_data")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/opt/project/checkpoints/streaming_job")

LATEST_SNAPSHOT_PATH = os.path.join(OUTPUT_DIR, "latest_snapshot.json")
HISTORY_PATH = os.path.join(OUTPUT_DIR, "history.jsonl")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

spark = (
    SparkSession.builder.appName("StockMarketStreamProcessing")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Ticker", StringType(), True),
    StructField("Company_Name", StringType(), True),
    StructField("Sector", StringType(), True),
    StructField("Industry", StringType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Adj_Close", DoubleType(), True),
    StructField("Volume", LongType(), True),
])

raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed_stream = (
    raw_stream.selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .filter(col("Sector").isNotNull() & col("Volume").isNotNull())
    .withColumn("event_time", current_timestamp())
)

aggregated_stream = (
    parsed_stream
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("Sector")
    )
    .agg(
        spark_sum(col("Volume")).alias("total_volume")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("Sector"),
        col("total_volume")
    )
)

def write_json_outputs(batch_df, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    records = [row.asDict(recursive=True) for row in batch_df.collect()]

    with open(LATEST_SNAPSHOT_PATH, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2, default=str)

    with open(HISTORY_PATH, "a", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False, default=str))
            f.write("\n")

query = (
    aggregated_stream.writeStream
    .outputMode("update")
    .foreachBatch(write_json_outputs)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()