import re
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import HTMLResponse
from sequence_manager import start_sequence, stop_sequence, sequence_state
from kafka_consumer import start_consumer_thread

app = FastAPI()
predictions_store = []

start_consumer_thread(predictions_store)

ID_MAP = {"00":0, "02":1, "04":2}
DEFAULT_ID = 2
DEFAULT_SOURCE = "00000090"

def parse_filename(name):
    pattern = r"^[a-zA-Z]+_id_(\d{2})_(\d+)\.wav$"
    match = re.match(pattern, name)
    if match:
        raw_id = match.group(1)
        source_id = match.group(2)
        return ID_MAP.get(raw_id, DEFAULT_ID), source_id
    return DEFAULT_ID, DEFAULT_SOURCE


@app.get("/", response_class=HTMLResponse)
def upload_page():
    return open("../frontend/templates/upload.html").read()


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return open("../frontend/templates/dashboard.html").read()


@app.get("/predictions")
def get_predictions():
    return predictions_store


@app.get("/sequence_status")
def get_status():
    return sequence_state


@app.post("/start_sequence")
async def start(files: list[UploadFile] = File(...)):

    processed = []

    for file in files:
        bytes_data = await file.read()
        machine_id, source_id = parse_filename(file.filename)

        processed.append({
            "bytes": bytes_data,
            "machine_id": machine_id,
            "source_id": source_id
        })

    start_sequence(processed)
    return {"status": "started"}


@app.post("/stop_sequence")
def stop():
    stop_sequence()
    return {"status": "stopped"}