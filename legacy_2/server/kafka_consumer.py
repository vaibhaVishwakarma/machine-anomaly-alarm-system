import json
import threading
from kafka import KafkaConsumer

def start_consumer_thread(store):

    def consume():
        consumer = KafkaConsumer(
            "inference_results_topic",
            bootstrap_servers="localhost:9092",
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )

        for msg in consumer:
            store.append(msg.value)
            if len(store) > 300:
                store.pop(0)

    thread = threading.Thread(target=consume, daemon=True)
    thread.start()