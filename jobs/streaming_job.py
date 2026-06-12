import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Memastikan folder dashboard_data ada agar tidak error saat start
os.makedirs("/opt/project/dashboard_data", exist_ok=True)

def process_batch(df, epoch_id):
    """
    Fungsi ini dieksekusi setiap 10 detik.
    Menangkap data harian (OHLCV) yang masuk dari Kafka dan menyimpannya.
    """
    rows = df.collect()
    
    if rows:
        records = []
        for r in rows:
            # Proteksi error jika ada data float yang terbaca NaN/None di Spark
            try:
                records.append({
                    "Date": str(r["Date"]),
                    "Ticker": str(r["Ticker"]),
                    "Company_Name": str(r["Company_Name"]),
                    "Sector": str(r["Sector"]),
                    "Open": float(r["Open"]) if r["Open"] else 0.0,
                    "High": float(r["High"]) if r["High"] else 0.0,
                    "Low": float(r["Low"]) if r["Low"] else 0.0,
                    "Close": float(r["Close"]) if r["Close"] else 0.0,
                    "Volume": float(r["Volume"]) if r["Volume"] else 0.0
                })
            except Exception as e:
                # Abaikan baris yang formatnya rusak agar tidak mematikan Spark
                continue 
            
        # Simpan ke history.jsonl dengan mode 'a' (append)
        with open("/opt/project/dashboard_data/history.jsonl", "a", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")

        print(f"✅ [Batch {epoch_id}] Berhasil menyimpan {len(records)} data saham harian (OHLCV) ke JSON!")

def main():
    print("Menginisialisasi Spark Streaming Job (Candlestick Mode)...")
    
    # 1. Membuat Spark Session
    spark = SparkSession.builder \
        .appName("LiveCandlestickStreaming") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # 2. Skema Data (Persis sesuai dengan yang dikirim Producer)
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
        StructField("Volume", DoubleType(), True)
    ])

    # 3. Membaca Streaming dari Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "stock-market-stream") \
        .option("startingOffsets", "latest") \
        .load()

    # 4. Parsing data JSON
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .filter(col("Ticker").isNotNull()) # Buang data kosong/error

    # 5. Menjalankan stream (Mode 'append' karena data hari-hari bertambah terus)
    print("Menunggu aliran Market Data dari Kafka...")
    query = parsed_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_batch) \
        .trigger(processingTime="3 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()