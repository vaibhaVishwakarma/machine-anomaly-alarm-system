from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer
import json, base64

app = FastAPI()

# ✅ CORS FIX
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # dev only
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

@app.post("/upload")
async def upload_audio(audio: UploadFile = File(...)):
    audio_bytes = await audio.read()

    message = {
        "audio": base64.b64encode(audio_bytes).decode(),
        "size": len(audio_bytes)
    }

    producer.send("audio-topic", message)
    print("Sent to Kafka:", len(audio_bytes))

    return {"status": "sent", "size": len(audio_bytes)}