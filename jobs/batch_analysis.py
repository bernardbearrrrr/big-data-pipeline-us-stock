from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as _sum, year, to_date
import json
import os
import sys

def main():
    print("🚀 Menginisialisasi Spark Batch Analytics...")
    
    # 1. Membuat Spark Session
    spark = SparkSession.builder \
        .appName("HistoricalBatchAnalytics") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # ========================================================
    # 2. LOGIKA BARU: Membaca langsung dari HDFS Protocol
    # ========================================================
    hdfs_path = "hdfs://namenode:9000/data/stock_prices_daily.csv"
    print(f"📥 Membaca data historis langsung dari HDFS: {hdfs_path}")
    
    try:
        # Membaca data menggunakan path HDFS yang sudah pasti
        df = spark.read.csv(hdfs_path, header=True, inferSchema=True)
    except Exception as e:
        print(f"❌ ERROR: File tidak ditemukan di HDFS! Pastikan kamu sudah meng-uploadnya dengan perintah 'hdfs dfs -put ...'")
        print(f"Detail: {e}")
        spark.stop()
        sys.exit(1)

    # 3. Menghapus data kosong pada kolom kunci
    df_clean = df.filter(col("Sector").isNotNull() & col("Ticker").isNotNull())

    # 4. Parsing Tanggal & Tahun untuk Analisis Tren Pertumbuhan Jangka Panjang
    df_parsed = df_clean.withColumn("Parsed_Date", to_date(col("Date").substr(1, 10), "yyyy-MM-dd"))
    df_with_year = df_parsed.withColumn("Year", year(col("Parsed_Date")))

    # 5. Analisis 1: Volatilitas Rata-Rata (High - Low) & Total Volume per Sektor
    print("🧠 Sedang menghitung Volatilitas per Sektor...")
    sector_agg = df_with_year.withColumn("Volatility", col("High") - col("Low")) \
        .groupBy("Sector").agg(
            avg("Volatility").alias("Avg_Volatility"),
            _sum("Volume").alias("Total_Volume")
        )
    sector_results = sector_agg.collect()
    
    sector_analysis_data = []
    for r in sector_results:
        sector_analysis_data.append({
            "Sector": str(r["Sector"]),
            "Avg_Volatility": float(r["Avg_Volatility"]) if r["Avg_Volatility"] else 0.0,
            "Total_Volume": float(r["Total_Volume"]) if r["Total_Volume"] else 0.0
        })

    # 6. Analisis 2: Top 10 Gainers Historis (Rata-rata kenaikan Close - Open per hari)
    print("🧠 Sedang menghitung Top 10 Gainers Historis...")
    gainer_agg = df_with_year.withColumn("Daily_Gain", col("Close") - col("Open")) \
        .groupBy("Ticker", "Company_Name", "Sector").agg(
            avg("Daily_Gain").alias("Avg_Daily_Gain")
        ).orderBy(col("Avg_Daily_Gain").desc()).limit(10)
    gainer_results = gainer_agg.collect()

    top_gainers_data = []
    for r in gainer_results:
        top_gainers_data.append({
            "Ticker": str(r["Ticker"]),
            "Company_Name": str(r["Company_Name"]),
            "Sector": str(r["Sector"]),
            "Avg_Daily_Gain": float(r["Avg_Daily_Gain"]) if r["Avg_Daily_Gain"] else 0.0
        })

    # 7. Analisis 3: Top 10 Likuiditas Historis (Berdasarkan total volume)
    print("🧠 Sedang menghitung Top 10 Likuiditas Saham...")
    volume_agg = df_with_year.groupBy("Ticker", "Company_Name", "Sector").agg(
        _sum("Volume").alias("Cumulative_Volume"),
        avg("Close").alias("Avg_Close_Price")
    ).orderBy(col("Cumulative_Volume").desc()).limit(10)
    volume_results = volume_agg.collect()

    top_volume_data = []
    for r in volume_results:
        top_volume_data.append({
            "Ticker": str(r["Ticker"]),
            "Company_Name": str(r["Company_Name"]),
            "Sector": str(r["Sector"]),
            "Cumulative_Volume": float(r["Cumulative_Volume"]) if r["Cumulative_Volume"] else 0.0,
            "Avg_Close_Price": float(r["Avg_Close_Price"]) if r["Avg_Close_Price"] else 0.0
        })

    # 8. Analisis 4: Tren Pertumbuhan Jangka Panjang (Rata-rata harga Close sektor per Tahun)
    print("🧠 Sedang menghitung Tren Harga Tahunan per Sektor...")
    yearly_agg = df_with_year.groupBy("Sector", "Year").agg(
        avg("Close").alias("Avg_Close_Price")
    ).orderBy("Year", "Sector")
    yearly_results = yearly_agg.collect()

    yearly_performance_data = []
    for r in yearly_results:
        if r["Year"]:
            yearly_performance_data.append({
                "Sector": str(r["Sector"]),
                "Year": int(r["Year"]),
                "Avg_Close_Price": float(r["Avg_Close_Price"]) if r["Avg_Close_Price"] else 0.0
            })

    # 9. Menggabungkan Semua Analisis ke dalam Satu JSON Object
    final_output = {
        "sector_analysis": sector_analysis_data,
        "top_gainers": top_gainers_data,
        "top_volume": top_volume_data,
        "yearly_performance": yearly_performance_data
    }

    # 10. Menyimpan Hasil Akhir ke Folder Bersama
    out_dir = "/opt/project/dashboard_data"
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "batch_result.json")
    
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(final_output, f, indent=4)
        
    print(f"✅ BATCH SELESAI! Hasil analisis kaya data disimpan di: {out_file}")
    
    spark.stop()

if __name__ == "__main__":
    main()