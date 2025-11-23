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
    # Open CSV with comma delimiter (default)
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)  # comma is default delimiter
        for row in reader:
            # Convert latitude/longitude to float
            try:
                row['latitude'] = float(row.get('latitude') or 0)
            except ValueError:
                row['latitude'] = None
            try:
                row['longitude'] = float(row.get('longitude') or 0)
            except ValueError:
                row['longitude'] = None

            # Convert fatalities/injuries to int
            for int_field in ('fatalities', 'injuries'):
                try:
                    row[int_field] = int(row.get(int_field) or 0)
                except ValueError:
                    row[int_field] = 0

            # Send message to Kafka
            producer.send(TOPIC_RAW, value=row)
            print('Sent event_id:', row.get('event_id'))

            # Small delay to avoid flooding Kafka
            time.sleep(0.01)

    # Ensure all messages are sent
    producer.flush()
    print("All messages sent!")

if __name__ == '__main__':
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV file not found: {CSV_PATH}")
    read_csv_and_publish(CSV_PATH)
