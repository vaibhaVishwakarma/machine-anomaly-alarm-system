from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer
import json, threading

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

latest_clip = {}

def start_consumer():
    print("Starting Kafka consumer...")

    consumer = KafkaConsumer(
        "audio-topic",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="audio-group"
    )

    print("Consumer connected.")

    for msg in consumer:
        print("Message received")
        data = msg.value
        latest_clip["audio"] = data["audio"]
        latest_clip["size_kb"] = round(data["size"]/1024, 2)

@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=start_consumer, daemon=True)
    thread.start()

@app.get("/latest")
async def latest():
    return latest_clip