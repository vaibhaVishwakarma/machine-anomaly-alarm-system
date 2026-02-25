from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import asyncio
import time
from kafka_producer import send_audio_bytes
from kafka_consumer import predictions_store, run_consumer_thread

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Start Kafka consumer thread
run_consumer_thread()

# -------------------------
# Upload Multiple Clips
# -------------------------
@app.post("/upload_clips")
async def upload_clips(files: List[UploadFile] = File(...)):
    for file in files:
        contents = await file.read()
        send_audio_bytes(contents)

    return {"status": "uploaded", "count": len(files)}

# -------------------------
# Dashboard Summary
# -------------------------
@app.get("/dashboard/summary")
def dashboard_summary():

    total = len(predictions_store)
    anomalies = sum(1 for p in predictions_store if p["classification"] == "anomalous")

    avg_prob = (
        sum(p["anomaly_probability"] for p in predictions_store) / total
        if total > 0 else 0
    )

    return {
        "total_clips": total,
        "anomalies": anomalies,
        "normal": total - anomalies,
        "average_probability": round(avg_prob, 3)
    }

# -------------------------
# Recent Predictions
# -------------------------
@app.get("/dashboard/recent")
def dashboard_recent():
    return predictions_store[-20:]