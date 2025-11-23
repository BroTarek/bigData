# test_kafka_connection.py
import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
result = s.connect_ex(('127.0.0.1', 9092))  # <- use 127.0.0.1 instead of 'kafka'
print('Kafka connection result:', result)
s.close()
