import csv
import json
import time
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Environment variables
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_RAW = os.getenv("TOPIC_RAW", "airplane_crashes_raw")
CSV_PATH = os.getenv("CSV_PATH", "data/airplane_crashes.csv")

print("Connecting to Kafka at:", KAFKA_BOOTSTRAP)

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    request_timeout_ms=30000,
    api_version_auto_timeout_ms=30000,
)

def read_csv_and_publish(path):
    # Read all records from CSV first
    records = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(row)
    
    print(f"Total records to send: {len(records)}")
    
    # Send records in batches of 30 every 4 seconds
    batch_size = 30
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        
        print(f"\nSending batch {i//batch_size + 1}/{(len(records)-1)//batch_size + 1}")
        print(f"Sending records {i+1} to {min(i+batch_size, len(records))}")
        
        # Send each record in the current batch
        for j, row in enumerate(batch, start=1):
            producer.send(TOPIC_RAW, value=row)
            print(f'  Sent event_id: {row.get("event_id")} ({j}/{len(batch)})')
        
        # Flush to ensure batch is sent before waiting
        producer.flush()
        
        # Wait 4 seconds between batches (unless it's the last batch)
        if i + batch_size < len(records):
            print(f"Waiting 4 seconds before next batch...")
            time.sleep(4)
    
    print("\nAll messages sent!")

if __name__ == '__main__':
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV file not found: {CSV_PATH}")
    read_csv_and_publish(CSV_PATH)