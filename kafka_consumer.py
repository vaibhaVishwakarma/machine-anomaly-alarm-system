# kafka_consumer.py

from kafka import KafkaConsumer
import json
import threading
from analytics_store import add_prediction

consumer = KafkaConsumer(
    "prediction-topic",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True
)

def consume_predictions():
    for message in consumer:
        add_prediction(message.value)

def start_consumer():
    thread = threading.Thread(target=consume_predictions, daemon=True)
    thread.start()