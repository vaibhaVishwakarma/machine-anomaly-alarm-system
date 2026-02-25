import json
from kafka import KafkaConsumer
import threading
import time

predictions_store = []

def start_prediction_listener():
    consumer = KafkaConsumer(
        "prediction-topic",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="dashboard-group"
    )

    for message in consumer:
        data = message.value
        predictions_store.append(data)

        # Keep last 200 only
        if len(predictions_store) > 200:
            predictions_store.pop(0)

def run_consumer_thread():
    thread = threading.Thread(target=start_prediction_listener)
    thread.daemon = True
    thread.start()