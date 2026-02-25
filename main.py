# main.py

from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import torchaudio
import torch
import base64
import io
import datetime
from fastapi.responses import JSONResponse
from kafka_producer import send_audio_message
from kafka_consumer import start_consumer
from analytics_store import get_recent, get_summary
import asyncio
from kafka_producer import send_audio_message
import wave
from typing import List

app = FastAPI()

TARGET_SR = 16000
CHUNK_SAMPLES = 160000  # 10 seconds

# Allow frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Start Kafka consumer on startup
@app.on_event("startup")
def startup_event():
    start_consumer()




@app.post("/upload_multiple")
async def upload_multiple(files: List[UploadFile] = File(...)):

    sent_count = 0

    for file in files:
        contents = await file.read()

        message = {
            "audio": base64.b64encode(contents).decode("utf-8"),
            "size": len(contents),
            "filename": file.filename
        }

        send_audio_message(message)
        sent_count += 1

    return JSONResponse({
        "status": f"{sent_count} clips sent to Kafka"
    })


# ===============================
# Upload 10s Audio Endpoint
# ===============================

TARGET_SR = 16000
CHUNK_SECONDS = 10

@app.post("/upload_stream")
async def upload_stream(audio: UploadFile = File(...)):

    contents = await audio.read()

    # Use pure Python wave module (no torchaudio)
    wav_file = wave.open(io.BytesIO(contents), 'rb')

    sample_rate = wav_file.getframerate()
    n_channels = wav_file.getnchannels()
    sample_width = wav_file.getsampwidth()

    if sample_rate != TARGET_SR:
        return JSONResponse(
            {"error": "Please upload 16kHz WAV file."},
            status_code=400
        )

    frames_per_chunk = TARGET_SR * CHUNK_SECONDS

    while True:
        frames = wav_file.readframes(frames_per_chunk)

        if not frames:
            break

        # Create new WAV chunk in memory
        buffer = io.BytesIO()
        chunk_wav = wave.open(buffer, 'wb')
        chunk_wav.setnchannels(n_channels)
        chunk_wav.setsampwidth(sample_width)
        chunk_wav.setframerate(TARGET_SR)
        chunk_wav.writeframes(frames)
        chunk_wav.close()

        buffer.seek(0)
        chunk_bytes = buffer.read()

        message = {
            "audio": base64.b64encode(chunk_bytes).decode("utf-8"),
            "size": len(chunk_bytes)
        }

        send_audio_message(message)

        print("Sent 10s chunk")

        await asyncio.sleep(10)

    return JSONResponse({"status": "Streaming complete"})
# ===============================
# Dashboard Endpoints
# ===============================

@app.get("/dashboard/recent")
def dashboard_recent():
    return get_recent(50)


@app.get("/dashboard/summary")
def dashboard_summary():
    return get_summary()