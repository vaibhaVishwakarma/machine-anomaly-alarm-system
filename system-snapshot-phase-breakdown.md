# 📦 SYSTEM SNAPSHOT & PHASE BREAKDOWN v3

Current working state and phase progress. Use this as a checkpoint reference and roadmap for the latest deployment shape.

---

# 📦 SYSTEM OVERVIEW

### Architecture (Current)

```text
Upload UI → FastAPI Backend (port 8000)
   ↓
Kafka: raw_audio_topic
   ↓
Spark Structured Streaming (window + inference + decision logic)
   ↓
Kafka: prediction_topic  ──→  Backend consumer → Dashboard
   ↓
Kafka: alert_event_topic  ──→  Notification Service → Email (SMTP)
```

---

# 📁 DIRECTORY STRUCTURE (Current)

```text
project-root/
│
├── docker-compose.yml
├── Dockerfile.spark
├── Dockerfile.backend
├── Dockerfile.notification
├── requirements.txt
│
├── backend/
│   ├── main.py
│   ├── kafka_producer.py
│   ├── kafka_consumer.py
│   ├── sequence_manager.py
│   └── requirements.txt
│
├── frontend/
│   └── templates/
│        ├── upload.html
│        └── dashboard.html
│
├── notification_service/
│   ├── consumer.py
│   ├── config.py
│   ├── state_manager.py
│   ├── notifier.py
│   └── requirements.txt
│
├── spark-app/
│   ├── stream_inference.py
│   └── model/
│        └── sw_wavenet_traced_cpu.pt
│
└── system-snapshot-phase-breakdown.md
```

---

# 🧠 MODEL CONFIGURATION

- **Model:** SW_WaveNet (multi-class anomaly detection)
- **Input:** 16 kHz, 10-second audio windows
- **Buffering:** `WINDOW_CHUNKS = 10`, `STEP_CHUNKS = 5`
- **Scoring:** `logits → softmax → score = -log(prob + 1e-9)`
- **Threshold:** `THRESHOLD = 1.192093e-07`

---

# ⚙️ SPARK STREAM CONFIGURATION

- **Source:** `raw_audio_topic` with bootstrap `kafka:29092`
- **Sinks:** `prediction_topic` and `alert_event_topic`
- **Trigger:** `processingTime="5 seconds"`
- **Checkpoint:** `/tmp/checkpoint_audio_v3`
- **Config:** `spark.sql.streaming.metricsEnabled = false`

---

# 📡 KAFKA CONFIGURATION

| Topic | Purpose |
| ----- | ------- |
| raw_audio_topic | Incoming audio packets |
| prediction_topic | Inference results and scores |
| alert_event_topic | Alarm-level events emitted by the decision layer |

- **Inside Docker:** `kafka:29092`
- **Outside Docker:** `localhost:9092`

**Topics are created during startup through the init-kafka service.**

---

# 🖥 BACKEND CONFIGURATION

- **Framework:** FastAPI
- **Port:** `8000`
- **Endpoints:**

| Endpoint | Purpose |
| -------- | ------- |
| `/` | Upload UI |
| `/dashboard` | Monitoring dashboard |
| `/predictions` | Latest predictions |
| `/sequence_status` | Sequence state |
| `/alert_email` | Alert recipient email |
| `/machine_states` | Per-machine alarm state |
| `/start_sequence` | Start processing sequence |
| `/stop_sequence` | Stop processing sequence |

- **Kafka consumer:** Subscribes to `prediction_topic` and `alert_event_topic` to update dashboard and alarm state.

---

# 📬 NOTIFICATION SERVICE

- **Role:** Consume `alert_event_topic`, deduplicate alerts, and send email notifications.
- **Deduplication:** Cooldown-based suppression for repeated alerts from the same machine.
- **Backoff:** Exponential backoff for retrying failed notification deliveries.
- **Flow:** Filter `alarm_state == "ALARM_TRIGGERED"` → deduplication → recipient lookup → email send.
- **State:** In-memory cooldown state per machine.

---

# 🔁 EVENT RELIABILITY LAYER

- **Window event:** 3 anomalies in 5 consecutive inferences
- **Alarm rule:** 3 window events in 4 consecutive windows
- **State:** Per-machine state with recent predictions and recent window events
- **Alert trigger:** Emits an alert event when a machine transitions from NORMAL to ALARM
- **Recovery:** Returns to NORMAL when the anomaly pattern clears

---

# 🐳 DOCKER STATE

- **Compose services:** zookeeper, kafka, init-kafka, backend, spark, notification
- **Backend:** Runs as a containerized service in Compose
- **Spark:** Starts automatically with a container command and submits the streaming job
- **Notification:** Uses `KAFKA_BOOTSTRAP_SERVERS=kafka:29092` and reaches the backend over the Docker network

---

# 🚦 CURRENT SYSTEM STATUS

| Component | Status |
| --------- | ------ |
| Upload + Backend | ✔ |
| Kafka topics | ✔ |
| Spark inference | ✔ |
| Decision logic (3/5 and 3/4) | ✔ |
| Alert publish to Kafka | ✔ |
| Notification service | ✔ |
| Deduplication + backoff | ✔ |
| Dashboard state updates | ✔ |

---

# 📈 PHASE DIAGRAM — DONE vs REMAINING

## 🔷 PHASE 0 — Baseline Streaming Inference — DONE
- Upload → FastAPI → Kafka → Spark → Kafka → Backend/Dashboard

## 🔷 PHASE 1 — Event-Level Reliability — DONE
- Windowed anomaly logic and alarm state transition implemented in Spark

## 🔷 PHASE 2 — Notification & Alert Service — DONE
- Notification service consumes alert events, deduplicates, and sends email alerts with backoff

## 🔷 PHASE 3 — Simulation & Resilience Testing — IN PROGRESS / REMAINING
- Stress test duplicate prevention, intermittent anomaly handling, and recovery behavior

## 🔷 PHASE 4 — Scaling & Autoscaling — REMAINING
- Scale notification workers and inference workers based on throughput

## 🔷 PHASE 5 — Drift Monitoring & Retraining — REMAINING
- Add model health monitoring and retraining pipelines

## 🔷 PHASE 6 — Production Hardening — REMAINING
- Add persistence, dead-letter handling, stronger observability, and security controls

---

# 🛑 FAILOVER CHECKLIST (v3)

1. **Model:** `spark-app/model/sw_wavenet_traced_cpu.pt`
2. **Kafka topics:** `raw_audio_topic`, `prediction_topic`, `alert_event_topic`
3. **Spark:** Use the current windowing, threshold, and event logic settings
4. **Backend:** Ensure the API is reachable on port `8000`
5. **Notification:** Configure SMTP and backend email endpoint correctly
6. **Docker:** Restart the stack with `docker compose up --build` after configuration changes

---

## Summary of changes in v3

- The system snapshot now reflects the current Compose-based deployment with backend, Kafka, Spark, and notification services.
- Spark is documented as the component that owns the decision layer and alert publication.
- The notification service is documented as using both deduplication and exponential backoff.
- The architecture is aligned with the latest runtime behavior and Kafka topic setup.
