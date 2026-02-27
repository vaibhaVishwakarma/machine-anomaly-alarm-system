import base64
import time
import re
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import HTMLResponse
from kafka_producer import send_to_kafka
from kafka_consumer import start_consumer_thread

app = FastAPI()
predictions_store = []

start_consumer_thread(predictions_store)

# ID mapping
ID_MAP = {
    "00": 0,
    "02": 1,
    "04": 2
}

DEFAULT_ID = 2
DEFAULT_SOURCE = "00000090"


def parse_filename(filename):

    # Expected format:
    # <alphabets>_id_<id>_<source>.wav
    pattern = r"^[a-zA-Z]+_id_(\d{2})_(\d+)\.wav$"

    match = re.match(pattern, filename)

    if match:
        raw_id = match.group(1)
        source_id = match.group(2)

        machine_index = ID_MAP.get(raw_id, DEFAULT_ID)

        return machine_index, source_id

    return DEFAULT_ID, DEFAULT_SOURCE


@app.get("/", response_class=HTMLResponse)
def upload_page():
    return open("templates/upload.html").read()


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return open("templates/dashboard.html").read()


@app.get("/predictions")
def get_predictions():
    return predictions_store


@app.post("/upload")
async def upload(files: list[UploadFile] = File(...)):

    for file in files:

        audio_bytes = await file.read()
        encoded = base64.b64encode(audio_bytes).decode("utf-8")

        machine_id, source_id = parse_filename(file.filename)

        message = {
            "machine_id": machine_id,
            "source_id": source_id,
            "timestamp": int(time.time()),
            "audio": encoded,
            "size_kb": round(len(audio_bytes)/1024, 2)
        }

        send_to_kafka(message)

    return {"status": "queued"}