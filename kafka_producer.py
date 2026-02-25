# kafka_producer.py

from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_audio_message(message: dict):
    producer.send("audio-topic", message)
    producer.flush()