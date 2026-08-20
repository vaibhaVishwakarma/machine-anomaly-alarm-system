# Spark Stream Inference Service

## 📋 Overview
The **Spark Stream Inference Service** is the real-time analytical engine of the Machine Anomaly Alarm System. It consumes streaming audio chunks from Kafka, reconstructs multi-second audio windows, runs neural inference using a pre-trained **SW-WaveNet (TorchScript)** model, evaluates a multi-stage **temporal decision filter**, and emits predictions and high-severity alarm events back to Kafka.

---

## 🏗️ Architecture & Data Flow

```text
Kafka (raw_audio_topic)
        │
        ▼ (Every 5s micro-batch)
[ Spark Structured Streaming ]
        │
        ├── 1. Base64 Decode & Resample (16 kHz)
        ├── 2. Per-Machine Audio Buffering (10s Window / 5s Step)
        ├── 3. TorchScript SW-WaveNet Inference
        ├── 4. Raw Anomaly Scoring (-log(P_target))
        │
        ├──► Publishes to Kafka: [ prediction_topic ]
        │
        └── 5. Two-Stage Temporal Decision Layer
                ├── Stage 1: Window Event (>= 3 of 5 predictions)
                └── Stage 2: Alarm State Transition (>= 3 of 4 windows)
                        │
                        └──► Publishes to Kafka: [ alert_event_topic ]
```

---

## ⚙️ Key Components & Code Logic (`stream_inference.py`)

### 1. Ingestion & Preprocessing
- **Kafka Consumer**: Subscribes to `raw_audio_topic` using Spark Structured Streaming.
- **Input Schema**:
  ```python
  StructType([
      StructField("machine_id", IntegerType()),
      StructField("source_id", StringType()),
      StructField("timestamp", LongType()),
      StructField("audio", StringType()),      # Base64 encoded audio
      StructField("size_kb", DoubleType())
  ])
  ```
- **Audio Decoding**: Decodes Base64 audio into raw PCM bytes and resamples to `16,000 Hz` using `torchaudio`.

---

### 2. Audio Buffering & Windowed Inference (`run_inference`)
- **Sliding Buffer**: Maintains an in-memory buffer (`buffer_store`) of raw audio chunks per machine.
- **Windowing Parameters**:
  - `WINDOW_CHUNKS = 10` (10 seconds total audio = 160,000 samples).
  - `STEP_CHUNKS = 5` (Inference cadence runs every 5 seconds).
- **Inference Execution**:
  - Concatenates the last 10 chunks: `full_waveform = torch.cat(window_chunks, dim=1)`
  - Feeds shape `(1, 1, 160000)` into `sw_wavenet_traced_cpu.pt` under `torch.no_grad()`.
  - Calculates target machine probability and anomaly score:
    $$\text{Score} = -\log(P_{\text{target}} + 10^{-9})$$
  - Emits `ANOMALY` if $\text{Score} \ge \text{THRESHOLD}$, else `NORMAL`.

---

### 3. Decision & Alerting Layer (`update_event_state`)
To eliminate false alarms caused by transient factory noises, the service implements a **2-stage persistence filter**:

```text
Predictions (25s):      [ Pred 1 ] [ Pred 2 ] [ Pred 3 ] [ Pred 4 ] [ Pred 5 ]
                                      │
                                      ▼
                            Window Event (>= 3 / 5)
                                      │
                                      ▼
Window Events (100s):   [ Win 1 ]  [ Win 2 ]  [ Win 3 ]  [ Win 4 ]
                                      │
                                      ▼
                         State: NORMAL ──► ALARM_TRIGGERED 🚨
```

1. **Stage 1 (Window Event)**:
   - Evaluates the last `WINDOW_EVENT_SIZE = 5` predictions (25s span).
   - If $\ge 3$ predictions are `ANOMALY` $\to$ `window_event = 1`.
2. **Stage 2 (Alarm Trigger)**:
   - Tracks the last `ALARM_WINDOW_SIZE = 4` window events (100s span).
   - If $\ge 3$ window events are active $\to$ triggers `ALARM_TRIGGERED`.
   - **Severity**: `HIGH` if 4/4 window events are anomalous; `MEDIUM` if 3/4.
3. **State Recovery**:
   - Automatically transitions from `ALARM` back to `NORMAL` when `window_event_count == 0`.

---

## 📡 Kafka Topic Contracts

### 1. Published to `prediction_topic`
Emitted on every 5-second inference step:
```json
{
  "timestamp": 1771485600,
  "machine_id": 0,
  "prediction": "ANOMALY",
  "anomaly_score": 24.582,
  "size_kb": 156.4
}
```

### 2. Published to `alert_event_topic`
Emitted when the 2-stage decision threshold triggers an active alarm:
```json
{
  "timestamp": 1771485625,
  "machine_id": 0,
  "alarm_state": "ALARM_TRIGGERED",
  "window_event_count": 3,
  "model_version": "v1",
  "severity": "MEDIUM",
  "confidence_score": 25.14
}
```

---

## 🐳 Docker & Runtime Environment

### Docker Configuration (`Dockerfile.spark`)
- **Base Image**: `public.ecr.aws/bitnami/spark:latest`
- **Dependencies**:
  - Python packages: `torch` (CPU), `torchaudio`, `numpy`, `torchcodec`, `soundfile`
  - System packages: `ffmpeg`, `libavcodec-dev`, `libavformat-dev`, `libavutil-dev`
- **User Permissions**: Configured to run under UID `1001` (`sparkuser`) with custom Ivy cache paths.

### Spark Submit Command (via `docker-compose.yml`)
```bash
spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
  /spark-app/stream_inference.py
```

---

## 🔍 Local Inspection & Debugging

### View Spark Streaming Logs
```bash
docker logs -f spark
```

### Inspect Output Kafka Topics
```bash
# View real-time predictions
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic prediction_topic \
  --from-beginning

# View alarm events
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic alert_event_topic \
  --from-beginning
```
