import json
from kafka import KafkaConsumer
import threading

def start_consumer_thread(store):

    def consume():
        consumer = KafkaConsumer(
            "prediction_topic",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="latest",
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )

        for msg in consumer:
            store.append(msg.value)

            if len(store) > 200:
                store.pop(0)

    thread = threading.Thread(target=consume, daemon=True)
    thread.start()