from fastapi import FastAPI, UploadFile, File
from kafka import KafkaProducer
import base64
import json
import torchaudio
import torch
import io

app = FastAPI()

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

@app.post("/upload")
async def upload(audio: UploadFile = File(...)):

    contents = await audio.read()

    signal, sr = torchaudio.load(io.BytesIO(contents))

    # 🔥 Resample to 16kHz
    if sr != 16000:
        resampler = torchaudio.transforms.Resample(sr, 16000)
        signal = resampler(signal)

    # 🔥 Ensure exactly 10 seconds
    expected = 160000
    if signal.shape[1] < expected:
        pad = expected - signal.shape[1]
        signal = torch.nn.functional.pad(signal, (0, pad))
    else:
        signal = signal[:, :expected]

    buffer = io.BytesIO()
    torchaudio.save(buffer, signal, 16000, format="wav")
    audio_bytes = buffer.getvalue()

    message = {
        "audio": base64.b64encode(audio_bytes).decode("utf-8"),
        "size": len(audio_bytes)
    }

    producer.send("audio-topic", message)

    return {"status": "sent"}