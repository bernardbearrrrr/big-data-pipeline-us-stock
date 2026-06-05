from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as _sum, year, month, min as _min, max as _max, row_number, to_date
from pyspark.sql import Window
import json
import os
import sys
import argparse
import tempfile


def format_volume(vol):
    if vol >= 1_000_000_000:
        return f"${vol / 1_000_000_000:.1f}B"
    elif vol >= 1_000_000:
        return f"${vol / 1_000_000:.1f}M"
    elif vol >= 1_000:
        return f"${vol / 1_000:.1f}K"
    return f"${vol:.0f}"


def main():
    parser = argparse.ArgumentParser(description="Batch Analysis - US Stock Market")
    parser.add_argument("--start-date", default=None, help="Filter start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default=None, help="Filter end date (YYYY-MM-DD)")
    parser.add_argument("--hdfs-base", default="hdfs://namenode:9000", help="HDFS base URL")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("HistoricalBatchAnalytics").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.csv(f"{args.hdfs_base}/data/stock_prices_daily.csv", header=True, inferSchema=True)
    df_clean = df.filter(col("Sector").isNotNull() & col("Ticker").isNotNull())

    df_parsed = df_clean.withColumn("Parsed_Date", to_date(col("Date").substr(1, 10), "yyyy-MM-dd"))

    if args.start_date:
        df_parsed = df_parsed.filter(col("Parsed_Date") >= args.start_date)
    if args.end_date:
        df_parsed = df_parsed.filter(col("Parsed_Date") <= args.end_date)

    df_sorted = df_parsed.orderBy("Parsed_Date")

    df_with_year = df_sorted.withColumn("Year", year(col("Parsed_Date")))
    df_with_month = df_with_year.withColumn("Month", month(col("Parsed_Date")))

    # 1. Overall sector volatility (High - Low) - original problem statement
    sector_agg = df_with_month.withColumn("Volatility", col("High") - col("Low")) \
        .groupBy("Sector").agg(
            avg("Volatility").alias("Avg_Volatility"),
            _sum("Volume").alias("Total_Volume"),
            _sum("Volume").alias("Total_Volume_Formatted")
        )
    sector_overall = []
    for r in sector_agg.collect():
        vol_raw = float(r["Total_Volume"]) if r["Total_Volume"] else 0.0
        sector_overall.append({
            "Sector": r["Sector"],
            "Avg_Volatility": round(float(r["Avg_Volatility"]), 2) if r["Avg_Volatility"] else 0.0,
            "Total_Volume": vol_raw,
            "Total_Volume_Display": format_volume(vol_raw)
        })

    # 2. Per day per sector volatility (date filterable)
    daily_agg = df_with_month.withColumn("Volatility", col("High") - col("Low")) \
        .groupBy("Parsed_Date", "Sector").agg(
            avg("Volatility").alias("Avg_Volatility"),
            _sum("Volume").alias("Total_Volume")
        ).orderBy("Parsed_Date", "Sector")
    daily_volatility = []
    for r in daily_agg.collect():
        vol_raw = float(r["Total_Volume"]) if r["Total_Volume"] else 0.0
        daily_volatility.append({
            "Date": str(r["Parsed_Date"]),
            "Sector": r["Sector"],
            "Avg_Volatility": round(float(r["Avg_Volatility"]), 2) if r["Avg_Volatility"] else 0.0,
            "Total_Volume": vol_raw,
            "Total_Volume_Display": format_volume(vol_raw)
        })

    # 3. Monthly candlestick per company (Open, High, Low, Close, Volume)
    w_asc = Window.partitionBy("Ticker", "Year", "Month").orderBy("Parsed_Date")
    w_desc = Window.partitionBy("Ticker", "Year", "Month").orderBy(col("Parsed_Date").desc())

    df_wn = df_with_month \
        .withColumn("rn_asc", row_number().over(w_asc)) \
        .withColumn("rn_desc", row_number().over(w_desc))

    monthly_first = df_wn.filter(col("rn_asc") == 1) \
        .select("Ticker", "Company_Name", "Sector", "Year", "Month", col("Open").alias("Month_Open"))
    monthly_last = df_wn.filter(col("rn_desc") == 1) \
        .select("Ticker", "Year", "Month", col("Close").alias("Month_Close"))

    monthly_agg = df_with_month.groupBy("Ticker", "Company_Name", "Sector", "Year", "Month").agg(
        _max("High").alias("Month_High"),
        _min("Low").alias("Month_Low"),
        _sum("Volume").alias("Month_Volume")
    )

    monthly_candle_df = monthly_agg \
        .join(monthly_first, ["Ticker", "Year", "Month"]) \
        .join(monthly_last, ["Ticker", "Year", "Month"]) \
        .select(
            monthly_agg["Ticker"], monthly_agg["Company_Name"], monthly_agg["Sector"],
            monthly_agg["Year"], monthly_agg["Month"],
            monthly_first["Month_Open"], monthly_agg["Month_High"],
            monthly_agg["Month_Low"], monthly_last["Month_Close"],
            monthly_agg["Month_Volume"]
        ).orderBy("Year", "Month", "Ticker")

    monthly_data = []
    for r in monthly_candle_df.collect():
        vol_raw = float(r["Month_Volume"]) if r["Month_Volume"] else 0.0
        monthly_data.append({
            "Ticker": r["Ticker"],
            "Company_Name": r["Company_Name"],
            "Sector": r["Sector"],
            "Year": int(r["Year"]) if r["Year"] else 0,
            "Month": int(r["Month"]) if r["Month"] else 0,
            "Month_Open": round(float(r["Month_Open"]), 2) if r["Month_Open"] else 0.0,
            "Month_High": round(float(r["Month_High"]), 2) if r["Month_High"] else 0.0,
            "Month_Low": round(float(r["Month_Low"]), 2) if r["Month_Low"] else 0.0,
            "Month_Close": round(float(r["Month_Close"]), 2) if r["Month_Close"] else 0.0,
            "Month_Volume": vol_raw,
            "Month_Volume_Display": format_volume(vol_raw)
        })

    # 4. Top 10 gainers per year
    gainers_base = df_with_month.withColumn("Daily_Gain", col("Close") - col("Open")) \
        .groupBy("Ticker", "Company_Name", "Sector", "Year").agg(
            avg("Daily_Gain").alias("Avg_Daily_Gain"),
            avg("Close").alias("Avg_Close_Price")
        )
    top_gainers_by_year_raw = {}
    for r in gainers_base.orderBy(col("Avg_Daily_Gain").desc()).collect():
        yr = int(r["Year"]) if r["Year"] else 0
        if yr not in top_gainers_by_year_raw:
            top_gainers_by_year_raw[yr] = []
        if len(top_gainers_by_year_raw[yr]) < 10:
            top_gainers_by_year_raw[yr].append({
                "Ticker": r["Ticker"],
                "Company_Name": r["Company_Name"],
                "Sector": r["Sector"],
                "Avg_Daily_Gain": round(float(r["Avg_Daily_Gain"]), 2) if r["Avg_Daily_Gain"] else 0.0,
                "Avg_Close_Price": round(float(r["Avg_Close_Price"]), 2) if r["Avg_Close_Price"] else 0.0
            })

    # 5. Top 10 volume per year
    volume_base = df_with_month.groupBy("Ticker", "Company_Name", "Sector", "Year").agg(
        _sum("Volume").alias("Cumulative_Volume"),
        avg("Close").alias("Avg_Close_Price")
    )
    top_volume_by_year_raw = {}
    for r in volume_base.orderBy(col("Cumulative_Volume").desc()).collect():
        yr = int(r["Year"]) if r["Year"] else 0
        if yr not in top_volume_by_year_raw:
            top_volume_by_year_raw[yr] = []
        if len(top_volume_by_year_raw[yr]) < 10:
            vol_raw = float(r["Cumulative_Volume"]) if r["Cumulative_Volume"] else 0.0
            top_volume_by_year_raw[yr].append({
                "Ticker": r["Ticker"],
                "Company_Name": r["Company_Name"],
                "Sector": r["Sector"],
                "Cumulative_Volume": vol_raw,
                "Cumulative_Volume_Display": format_volume(vol_raw),
                "Avg_Close_Price": round(float(r["Avg_Close_Price"]), 2) if r["Avg_Close_Price"] else 0.0
            })

    # 6. Low 10 volume per year
    low_volume_by_year_raw = {}
    for r in volume_base.orderBy(col("Cumulative_Volume").asc()).collect():
        yr = int(r["Year"]) if r["Year"] else 0
        if yr not in low_volume_by_year_raw:
            low_volume_by_year_raw[yr] = []
        if len(low_volume_by_year_raw[yr]) < 10:
            vol_raw = float(r["Cumulative_Volume"]) if r["Cumulative_Volume"] else 0.0
            low_volume_by_year_raw[yr].append({
                "Ticker": r["Ticker"],
                "Company_Name": r["Company_Name"],
                "Sector": r["Sector"],
                "Cumulative_Volume": vol_raw,
                "Cumulative_Volume_Display": format_volume(vol_raw),
                "Avg_Close_Price": round(float(r["Avg_Close_Price"]), 2) if r["Avg_Close_Price"] else 0.0
            })

    final_output = {
        "metadata": {
            "analysis_type": "batch",
            "date_range": f"{args.start_date or 'all'} to {args.end_date or 'all'}",
            "total_records": df_with_month.count()
        },
        "sector_volatility_overall": sector_overall,
        "sector_volatility_daily": daily_volatility,
        "monthly_candlestick": monthly_data,
        "top_gainers_by_year": {str(k): v for k, v in top_gainers_by_year_raw.items()},
        "top_volume_by_year": {str(k): v for k, v in top_volume_by_year_raw.items()},
        "low_volume_by_year": {str(k): v for k, v in low_volume_by_year_raw.items()}
    }

    # Write to local filesystem (for dashboard)
    out_dir = "/opt/project/dashboard_data"
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "batch_result.json")
    with open(out_file, "w") as f:
        json.dump(final_output, f, indent=2)

    # Write to HDFS (for record keeping)
    hdfs_out_path = f"{args.hdfs_base}/dashboard_data/batch_result.json"
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp.write(json.dumps(final_output))
            tmp_path = tmp.name
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        hdfs_path_obj = spark._jvm.org.apache.hadoop.fs.Path(hdfs_out_path)
        fs.delete(hdfs_path_obj, True)
        local_path = spark._jvm.org.apache.hadoop.fs.Path(tmp_path)
        fs.copyFromLocalFile(False, True, local_path, hdfs_path_obj)
        os.unlink(tmp_path)
    except Exception:
        pass

    spark.stop()


if __name__ == "__main__":
    main()
