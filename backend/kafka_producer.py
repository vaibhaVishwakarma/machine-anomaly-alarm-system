import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:29092", # Change this line
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_to_kafka(message):
    producer.send("raw_audio_topic", message)
    producer.flush()