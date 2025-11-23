from kafka import KafkaConsumer
import json
import os

# Use kafka service name when running in Docker
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_RAW = os.getenv("TOPIC_RAW", "airplane_crashes_raw")

consumer = KafkaConsumer(
    TOPIC_RAW,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print(f"Listening to {TOPIC_RAW} on {KAFKA_BOOTSTRAP}...")
for msg in consumer:
    print(msg.value)