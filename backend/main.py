import re
import time
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import HTMLResponse
from sequence_manager import start_sequence, stop_sequence, sequence_state
from kafka_consumer import start_consumer_thread, last_alarm_timestamp

app = FastAPI()
predictions_store = []


ID_MAP = {"00":0, "02":1, "04":2}
DEFAULT_ID = 2
DEFAULT_SOURCE = "00000090"
alert_email_global = []
machine_alarm_states = dict()

start_consumer_thread(predictions_store, machine_alarm_states)


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
async def start(files: list[UploadFile] = File(...), alert_email: str = Form(...)):

    global alert_email_global
    alert_email_global = alert_email.strip()

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

@app.get("/alert_email")
def get_alert_email():
    return {"email": alert_email_global}

ALARM_WINDOW_SECONDS = 150

@app.get("/machine_states")
def get_machine_states():

    current_time = time.time()
    states = {}

    for machine_id, ts in last_alarm_timestamp.items():

        if current_time - ts <= ALARM_WINDOW_SECONDS:
            states[machine_id] = "ALARM"
        else:
            states[machine_id] = "NORMAL"

    return states