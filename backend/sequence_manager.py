import threading
import time
import base64
import io
import soundfile as sf
import numpy as np
from kafka_producer import send_to_kafka

EXPECTED_SR = 16000
CHUNK_SAMPLES = 16000  # 1 second

sequence_state = {
    "running": False,
    "current_clip": 0,
    "total_bytes": 0
}

def start_sequence(files):
    sequence_state["running"] = True
    sequence_state["current_clip"] = 0
    sequence_state["total_bytes"] = 0

    thread = threading.Thread(
        target=run_sequence,
        args=(files,),
        daemon=True
    )
    thread.start()


def stop_sequence():
    sequence_state["running"] = False


def run_sequence(files):

    for idx, file in enumerate(files):

        if not sequence_state["running"]:
            break

        sequence_state["current_clip"] = idx

        machine_id = file["machine_id"]
        source_id = file["source_id"]

        # Decode full WAV safely using soundfile
        data, sr = sf.read(io.BytesIO(file["bytes"]))

        if sr != EXPECTED_SR:
            raise ValueError("Audio must be 16kHz")

        if len(data.shape) > 1:
            data = data[:,0]  # mono

        total_samples = len(data)

        for i in range(0, total_samples, CHUNK_SAMPLES):

            if not sequence_state["running"]:
                break

            chunk = data[i:i + CHUNK_SAMPLES]

            if len(chunk) < CHUNK_SAMPLES:
                break

            buffer = io.BytesIO()
            sf.write(buffer, chunk, EXPECTED_SR, format="WAV")
            chunk_bytes = buffer.getvalue()

            encoded = base64.b64encode(chunk_bytes).decode()

            message = {
                "machine_id": machine_id,
                "source_id": source_id,
                "timestamp": int(time.time()),
                "audio": encoded,
                "size_kb": round(len(chunk_bytes)/1024, 2)
            }

            sequence_state["total_bytes"] += len(chunk_bytes)

            send_to_kafka(message)

            time.sleep(1)

    sequence_state["running"] = False