# Backend API & Web Dashboard Service

## 📋 Overview
The **Backend Service** is the central control plane, ingestion gateway, and visualization API of the Machine Anomaly Alarm System. Built with **FastAPI**, it serves the web dashboard, manages audio upload workflows, streams 1-second audio chunks to Kafka at real-time speeds, and maintains in-memory state of live predictions and alarms for dashboard telemetry.

---

## 🏗️ Architecture & Component Flow

```text
[ Web Browser ]
      │
      ├── GET /                 ──► Upload UI (upload.html)
      ├── GET /dashboard        ──► Live Monitoring Dashboard (dashboard.html)
      │
      └── POST /start_sequence  ──► Ingestion & Streaming
                                          │
                                          ▼
                                 [ sequence_manager.py ]
                                          │
                                          ├── 1. Audio Decoding & 16kHz Mono Check
                                          ├── 2. 1-Second Chunking (16,000 samples)
                                          ├── 3. Base64 Encoding
                                          └── 4. 1-Second Cadence (time.sleep(1))
                                                  │
                                                  ▼
                                      Kafka (raw_audio_topic)

Kafka [ prediction_topic, alert_event_topic ]
      │
      ▼
[ kafka_consumer.py ] (Background Thread)
      │
      ├── Updates predictions_store (Rolling buffer of 500)
      └── Updates machine_alarm_states & last_alarm_timestamp (150s window)
```

---

## ⚙️ Key Modules & File Breakdown

### 1. `main.py` (FastAPI Application & Endpoints)
- **Template Rendering**: Uses `pathlib.Path` to load `templates/upload.html` and `templates/dashboard.html`.
- **Filename Parser (`parse_filename`)**:
  - Extracts Machine ID and Source ID from filenames using regex:
    ```python
    pattern = r"^[a-zA-Z]+_id_(\d{2})_(\d+)\.wav$"
    # Maps '00' -> 0, '02' -> 1, '04' -> 2
    ```
- **Lifecycle Management**: Automatically launches the background Kafka consumer thread upon startup.

---

### 2. `sequence_manager.py` (Real-Time Audio Streamer)
Simulates an active industrial edge sensor transmitting audio in real-time:
- **Audio Validation**: Ensures audio is sampled at `16,000 Hz` and converts multi-channel audio to mono (`data[:, 0]`).
- **1-Second Chunking**:
  - Splits audio into slices of `CHUNK_SAMPLES = 16000`.
  - Encodes each 1-second slice into a standalone Base64-encoded WAV buffer.
- **Kafka Streaming**:
  - Emits one payload every `1.0` second (`time.sleep(1)`) to `raw_audio_topic`.
  - Tracks live execution state (`running`, `current_clip`, `total_bytes`).

---

### 3. `kafka_producer.py` (Kafka Message Publisher)
- Configures `KafkaProducer` connecting to `kafka:29092`.
- Serializes Python dictionaries into JSON bytes and delivers messages to `raw_audio_topic`.

---

### 4. `kafka_consumer.py` (Background State Consumer)
Runs a dedicated daemon thread to ingest Spark's output topics:
- **`prediction_topic`**: Appends latest inference results to a rolling buffer (capped at 500 entries) for live chart polling.
- **`alert_event_topic`**: Tracks `ALARM_TRIGGERED` events per machine and records `last_alarm_timestamp[machine_id]`.

---

## 🌐 REST API Endpoints

### UI & Template Routes

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/` | Serves the audio upload & configuration page (`upload.html`). |
| `GET` | `/dashboard` | Serves the real-time monitoring dashboard (`dashboard.html`). |

---

### Telemetry & State Routes

| Method | Endpoint | Description | Sample Response |
| :--- | :--- | :--- | :--- |
| `GET` | `/predictions` | Returns rolling list of recent predictions. | `[{"timestamp": 1771485600, "machine_id": 0, "prediction": "NORMAL", "anomaly_score": 12.3}]` |
| `GET` | `/sequence_status`| Returns sequence streaming status. | `{"running": true, "current_clip": 2, "total_bytes": 655360}` |
| `GET` | `/machine_states` | Returns per-machine alarm states (150s window).| `{"0": "ALARM", "2": "NORMAL", "4": "NORMAL"}` |
| `GET` | `/alert_email` | Returns recipient email for notification service.| `{"email": "engineer@factory.com"}` |

---

### Action Routes

| Method | Endpoint | Parameters | Description |
| :--- | :--- | :--- | :--- |
| `POST` | `/start_sequence` | `files: list[UploadFile]`<br>`alert_email: str` | Starts streaming uploaded audio chunks to Kafka in background. |
| `POST` | `/stop_sequence` | *None* | Stops active audio streaming immediately. |

---

## 📡 Kafka Topic Contracts

### Outgoing Payload (`raw_audio_topic`)
```json
{
  "machine_id": 0,
  "source_id": "00000090",
  "timestamp": 1771485600,
  "audio": "UklGRiQAAABXQVZFZm10IBAAAAABAAEA...",
  "size_kb": 31.25
}
```

---

## 🐳 Docker Deployment

### `Dockerfile.backend`
- **Base Image**: `python:3.11-slim`
- **System Dependencies**: `build-essential`, `ffmpeg`
- **Python Dependencies**: `fastapi`, `uvicorn`, `python-multipart`, `kafka-python`, `numpy`, `soundfile`
- **Command**:
  ```bash
  uvicorn main:app --host 0.0.0.0 --port 8000
  ```

### Docker Compose Configuration
```yaml
backend:
  build:
    context: ./backend
    dockerfile: ../Dockerfile.backend
  container_name: backend
  ports:
    - "8000:8000"
  depends_on:
    init-kafka:
      condition: service_completed_successfully
```

---

## 🔍 Local Inspection & Debugging

### View Backend Logs
```bash
docker logs -f backend
```

### Test Backend Endpoints Locally
```bash
# Check sequence status
curl http://localhost:8000/sequence_status

# Check machine states
curl http://localhost:8000/machine_states

# Check configured alert email
curl http://localhost:8000/alert_email
```
