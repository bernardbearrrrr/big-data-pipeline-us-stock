import csv
import json
import time
import os
from kafka import KafkaProducer

# ============================================================
# CONFIGURATION
# ============================================================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC  = "stock-market-stream"
CSV_PATH     = "/opt/project/data/stock_prices_daily.csv"
DELAY        = 0.5  # seconds between each message

# ============================================================
# PRODUCER SETUP
# ============================================================
def create_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
    """
    Try to connect to Kafka with retries.
    Kafka might take a few seconds to be ready inside Docker.
    """
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",           # wait for full acknowledgement
                retries=3,
            )
            print(f"[Producer] Connected to Kafka broker at {KAFKA_BROKER}")
            return producer
        except Exception as e:
            print(f"[Producer] Attempt {attempt}/{retries} failed: {e}")
            time.sleep(delay)
    raise RuntimeError("[Producer] Could not connect to Kafka after multiple attempts.")

# ============================================================
# MAIN STREAMING LOOP
# ============================================================
def stream_csv(producer: KafkaProducer) -> None:
    """
    Read stock_prices_daily.csv row by row and send each row
    as a JSON message to the Kafka topic.
    """
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"[Producer] CSV file not found at: {CSV_PATH}")

    print(f"[Producer] Reading from: {CSV_PATH}")
    print(f"[Producer] Streaming to Kafka topic: '{KAFKA_TOPIC}'")
    print(f"[Producer] Delay per message: {DELAY}s\n")

    count = 0
    with open(CSV_PATH, mode="r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            # --- Clean & cast numeric fields ---
            message = {
                "Date":    row.get("Date", ""),
                "Ticker":  row.get("Ticker", ""),
                "Company": row.get("Company", ""),
                "Sector":  row.get("Sector", ""),
                "Open":    safe_float(row.get("Open")),
                "High":    safe_float(row.get("High")),
                "Low":     safe_float(row.get("Low")),
                "Close":   safe_float(row.get("Close")),
                "Volume":  safe_int(row.get("Volume")),
            }

            # --- Send to Kafka ---
            producer.send(KAFKA_TOPIC, value=message)
            count += 1

            if count % 100 == 0:
                print(f"[Producer] Sent {count} messages... (latest: {message['Ticker']} | {message['Date']})")

            time.sleep(DELAY)

    producer.flush()
    print(f"\n[Producer] Done! Total messages sent: {count}")

# ============================================================
# HELPERS
# ============================================================
def safe_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0

def safe_int(value) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0

# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    producer = create_producer()
    try:
        stream_csv(producer)
    except KeyboardInterrupt:
        print("\n[Producer] Stopped manually.")
    finally:
        producer.close()
        print("[Producer] Connection closed.")
