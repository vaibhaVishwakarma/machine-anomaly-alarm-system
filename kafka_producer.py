import json
import base64
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_audio_bytes(audio_bytes: bytes):
    message = {
        "audio": base64.b64encode(audio_bytes).decode("utf-8"),
        "size": len(audio_bytes)
    }
    producer.send("audio-topic", message)
    producer.flush()