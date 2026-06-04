import json
import time
import os
import pandas as pd
from kafka import KafkaProducer
from hdfs import InsecureClient # LIBRARY BARU: Untuk terhubung ke Hadoop/HDFS via jaringan

# ==========================================
# KONFIGURASI ENVIRONMENT & KAFKA
# ==========================================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "stock-market-stream")
DELAY_PER_DAY = 3  # Detik jeda antar hari saat simulasi pengiriman berjalan

# ==========================================
# KONFIGURASI KONEKSI KE HDFS (NAMENODE)
# ==========================================
HDFS_URL = "http://namenode:9870" # Port WebHDFS bawaan Hadoop untuk komunikasi HTTP
HDFS_USER = "root" # User default di dalam container HDFS
HDFS_FILE_PATH = "/data/stock_prices_daily.csv" # Lokasi persis file CSV di dalam HDFS (bukan lokal)

def json_serializer(data):
    """Fungsi untuk mengubah data dictionary Python menjadi format JSON (bytes) yang bisa ditelan oleh Kafka"""
    return json.dumps(data).encode("utf-8")

def main():
    print(f"Mencoba terhubung ke Kafka di {KAFKA_BROKER}...")
    
    # 1. LOOP KONEKSI KAFKA: Memastikan script tidak crash jika Kafka sedang proses booting
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=json_serializer
            )
            print("[SUCCESS] Berhasil terhubung ke Apache Kafka!")
        except Exception as e:
            print(f"[WAIT] Kafka belum siap, mencoba lagi dalam 5 detik... Error: {e}")
            time.sleep(5)

    # 2. MEMBACA DATA DARI HDFS (Tidak lagi mencari di folder /app/data lokal)
    print(f"[INFO] Terhubung ke HDFS Namenode di {HDFS_URL}...")
    try:
        # Membuka koneksi ke sistem Hadoop
        hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)
        print(f"[INFO] Mendownload dan membaca {HDFS_FILE_PATH} dari HDFS...")
        
        # Membaca file CSV langsung dari HDFS melalui jaringan ke dalam memori Pandas
        with hdfs_client.read(HDFS_FILE_PATH, encoding='utf-8') as reader:
            df = pd.read_csv(reader)
        print("[SUCCESS] Dataset berhasil dibaca dari HDFS!")
        
    except Exception as e:
        # Jika gagal (misal: user lupa menjalankan perintah 'hdfs dfs -put ...')
        print(f"[ERROR] Gagal membaca dari HDFS! Pastikan kamu sudah meng-upload CSV ke HDFS. Error: {e}")
        return

    print("[PROCESS] Sedang mengurutkan dan mengelompokkan data berdasarkan tanggal...")
    
    # 3. PERBAIKAN DATA: Memotong string tanggal untuk membuang jam (00:00:00-05:00) 
    # agar grouping per hari 100% akurat. Asumsi format yyyy-mm-dd ada di 10 karakter pertama.
    df['Day_Only'] = df['Date'].astype(str).str[:10]
    
    # Urutkan berdasarkan tanggal agar pengiriman simulasi berjalan maju dari masa lalu ke masa depan
    df = df.sort_values('Day_Only')

    print(f"[START] Mulai simulasi Market Data per Hari ke topik '{TOPIC_NAME}'...")
    
    try:
        # 4. PROSES PENGIRIMAN: Group by berdasarkan Hari
        for day, group_df in df.groupby('Day_Only'):
            print(f"\n[MARKET OPEN] Mengirim data tanggal {day} ({len(group_df)} Perusahaan)...")
            
            # Kirim semua perusahaan yang beroperasi/memiliki data di hari tersebut
            for index, row in group_df.iterrows():
                # Pastikan kolom Date isinya sudah tanggal bersih (yyyy-mm-dd) agar gampang dibaca oleh Candlestick Streamlit
                clean_row = row.drop(labels=['Day_Only']).copy()
                clean_row['Date'] = day
                message = clean_row.to_dict()
                
                # Tembakkan 1 baris data (1 perusahaan) ke Kafka Topic
                producer.send(TOPIC_NAME, value=message)
                
            # Pastikan semua antrian data di hari itu benar-benar masuk ke Kafka sebelum lanjut ke hari esok
            producer.flush()
            print(f"[MARKET CLOSED] Data {day} sukses terkirim. Menunggu {DELAY_PER_DAY} detik untuk hari berikutnya...")
            
            # Waktu jeda agar stream terlihat seperti pergerakan live trading
            time.sleep(DELAY_PER_DAY)
            
    except KeyboardInterrupt:
        # Menangani interupsi jika kamu menekan Ctrl+C di terminal
        print("\n[STOP] Proses dihentikan secara manual oleh user.")
    finally:
        # Selalu pastikan Kafka ditutup dengan benar agar tidak terjadi Memory Leak
        producer.close()
        print("[CLOSED] Kafka Producer ditutup.")

if __name__ == "__main__":
    main()