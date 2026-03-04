
# 📦 SYSTEM SNAPSHOT & PHASE BREAKDOWN v2

Current working state and phase progress. Use as checkpoint reference and roadmap.

---

# 📦 SYSTEM OVERVIEW

### Architecture (Current)

```
Upload UI → FastAPI Backend (port 8000)
   ↓
Kafka: raw_audio_topic
   ↓
Spark Structured Streaming (window + inference + event logic)
   ↓
Kafka: prediction_topic  ──→  Backend consumer → Dashboard (Chart.js)
   ↓
Kafka: alert_event_topic  ──→  Notification Service → Email (SMTP)
```

---

# 📁 DIRECTORY STRUCTURE (Current)

```
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
├── system-snapshot-phase-breakdown.md   # legacy snapshot
└── snapshot-and-phase-breakdown-v2.md  # this file
```

---

# 🧠 MODEL CONFIGURATION

- **Model:** SW_WaveNet (multi-class, anomaly detection)
- **Input:** `[1, 1, 160000]`, 16 kHz, 10 s; audio in 1 s chunks (16000 samples)
- **Buffering:** WINDOW_CHUNKS = 10, STEP_CHUNKS = 5
- **Scoring:** `logits → softmax → prob(machine_id) → score = -log(prob + 1e-9)`
- **Threshold:** `THRESHOLD = 1.192093e-07` → ANOMALY if score ≥ threshold, else NORMAL

---

# ⚙️ SPARK STREAM CONFIGURATION

- **Source:** `raw_audio_topic`, bootstrap `kafka:29092`
- **Sinks:** `prediction_topic`, `alert_event_topic` (same bootstrap)
- **Trigger:** `processingTime="5 seconds"`
- **Checkpoint:** `/tmp/checkpoint_audio_v3`
- **Config:** `spark.sql.streaming.metricsEnabled = false`

---

# 📡 KAFKA CONFIGURATION

| Topic              | Purpose                          |
| ------------------ | -------------------------------- |
| raw_audio_topic    | 1-second audio packets           |
| prediction_topic   | Inference results (score, label) |
| alert_event_topic  | Alarm-level events for alerts   |

- **Inside Docker:** `kafka:29092`
- **Outside Docker:** `localhost:9092`

**Create topics (one line):**
```bash
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --create --topic raw_audio_topic --partitions 1 --replication-factor 1; \
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --create --topic prediction_topic --partitions 1 --replication-factor 1; \
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --create --topic alert_event_topic --partitions 1 --replication-factor 1
```

---

# 🖥 BACKEND CONFIGURATION

- **Framework:** FastAPI, port **8000** (run separately; not in docker-compose)
- **Endpoints:**

| Endpoint           | Purpose                          |
| ------------------ | -------------------------------- |
| `/`                | Upload page                      |
| `/dashboard`       | Monitoring dashboard             |
| `/predictions`     | Inference results                |
| `/sequence_status` | Upload progress                  |
| `/alert_email`     | Get alert recipient email(s)     |
| `/machine_states`  | Per-machine NORMAL/ALARM state   |
| `/start_sequence`  | Start upload (files + alert_email) |
| `/stop_sequence`   | Stop upload                      |

- **Kafka consumer:** Subscribes to `prediction_topic` and `alert_event_topic`; updates `machine_alarm_states` and `last_alarm_timestamp` (150 s window).

---

# 📬 NOTIFICATION SERVICE (New)

- **Role:** Consume `alert_event_topic`, deduplicate by cooldown, send email.
- **Config:** `config.py` — `KAFKA_BOOTSTRAP`, `ALERT_TOPIC`, `COOLDOWN_SECONDS` (150 s), `BACKEND_URL` (e.g. `http://host.docker.internal:8000`), SMTP settings.
- **Flow:** Filter `alarm_state == "ALARM_TRIGGERED"` → `state_manager.should_send(machine_id)` (cooldown) → fetch recipients from `GET /alert_email` → `send_email(alert_payload, recipients)`.
- **Alert payload:** `timestamp`, `machine_id`, `alarm_state`, `window_event_count`, `model_version`, `severity`, `confidence_score`.
- **State:** In-memory cooldown per `machine_id`; no DB persistence yet.

---

# 🔁 EVENT RELIABILITY LAYER (Phase 1 — Implemented in Spark)

- **Window event:** Last 5 inferences → **≥3 anomalies** → window event = 1. (25 s window.)
- **Alarm rule:** Last 4 window events → **≥3 window events** → alarm triggered.
- **State:** Per-machine `event_state`: `last_predictions`, `last_window_events`, `current_state` (NORMAL / ALARM).
- **Alert:** On transition NORMAL → ALARM, build alert payload and write to `alert_event_topic`. Severity: HIGH if 4/4 window events, else MEDIUM. Recovery: ALARM → NORMAL when window_event_count == 0.

---

# 🐳 DOCKER STATE

- **Compose services:** zookeeper, kafka, spark, **notification**
- **Backend:** Not in compose; run locally (e.g. `uvicorn`) or via `Dockerfile.backend` elsewhere.
- **Notification:** Uses `KAFKA_BOOTSTRAP_SERVERS=kafka:29092`, connects to backend at `host.docker.internal:8000` for `/alert_email`.

---

# 🚦 CURRENT SYSTEM STATUS

| Component            | Status |
| -------------------- | ------ |
| Upload + Backend     | ✔      |
| Kafka (3 topics)     | ✔      |
| Spark inference      | ✔      |
| Sliding 10 s / step 5 s | ✔   |
| Event logic (3/5, 3/4) | ✔   |
| Alert publish to Kafka | ✔   |
| Notification service | ✔ (email, cooldown) |
| Dashboard (150 s)    | ✔      |
| Backend alarm states | ✔      |

---

# 🧩 NOT YET INCLUDED (Remaining from phase diagram)

- No Airflow
- No DB persistence for predictions or alert audit log
- No retry / dead-letter for failed emails
- No Slack / webhook / SMS
- No formal drift monitoring or retraining pipeline
- No alert storm / escalation tests

---

# 📈 PHASE DIAGRAM — DONE vs REMAINING

## 🔷 PHASE 0 — Baseline Streaming Inference — **DONE**

- Upload → FastAPI → Kafka → Spark (window + inference) → Kafka → Backend → Dashboard.
- One-class style scoring, 10 s window, 5 s step, threshold-based decision.

---

## 🔷 PHASE 1 — Event-Level Reliability — **DONE**

- Window event: ≥3 anomalies in 5 inferences (25 s).
- Alarm: ≥3 window events in 4 (100 s).
- State machine NORMAL ↔ ALARM in Spark; alert payload published to `alert_event_topic`.

---

## 🔷 PHASE 2 — Notification & Alert Service — **PARTIALLY DONE**

- **Done:** Dedicated notification service; consumes `alert_event_topic`; cooldown per machine; email via SMTP; recipients from backend `/alert_email`; only `ALARM_TRIGGERED` sent.
- **Remaining:** Audit logging (DB), retry + dead-letter, Slack/webhook/SMS, severity-based routing/escalation.

---

## 🔷 PHASE 3 — Simulation & Alert Testing — **REMAINING**

- Macro: sustained anomaly → single alert; intermittent → no alert; cooldown; notification service kill/retry.
- Micro: duplicate prevention, idempotent send, state reset after recovery.

---

## 🔷 PHASE 4 — Scaling & Autoscaling — **REMAINING**

- Scale notification by alert throughput; inference by pump count; optional GPU inference service.

---

## 🔷 PHASE 5 — Drift Monitoring & Retraining — **REMAINING**

- Drift/model health alerts; retraining triggers; separate from operational anomaly alerts.

---

## 🔷 PHASE 6 — Production Hardening — **REMAINING**

- Alert storm, multi-machine concurrent alerts, escalation tests, email provider failure, Kafka recovery.

---

# 🛑 FAILOVER CHECKLIST (v2)

1. **Model:** `spark-app/model/sw_wavenet_traced_cpu.pt`
2. **Kafka topics:** `raw_audio_topic`, `prediction_topic`, `alert_event_topic`
3. **Spark:** WINDOW_CHUNKS=10, STEP_CHUNKS=5, THRESHOLD=1.192093e-07, trigger 5 s, event 3/5 and 3/4
4. **Backend:** Run on port 8000; Kafka consumer to `localhost:9092` (or kafka:29092 if backend in same Docker network)
5. **Notification:** Build/run via Dockerfile.notification; set `KAFKA_BOOTSTRAP_SERVERS`, ensure backend reachable at BACKEND_URL
6. **Dashboard:** 150 s window
7. **Notification config:** Set SMTP and (optional) move secrets to env vars
---

Summary of what’s in v2 vs the old snapshot:

- **Architecture:** Adds `alert_event_topic` and the Notification Service; backend still on 8000, not in compose.
- **Directory:** Adds `notification_service/`, `Dockerfile.backend`, `Dockerfile.notification`.
- **Kafka:** Third topic `alert_event_topic` and create command.
- **Backend:** Documents `/alert_email` and `/machine_states`, and consumer subscribing to both prediction and alert topics.
- **New section:** Notification Service (config, flow, payload, cooldown).
- **Event logic:** Documents actual implementation (≥3 of 5 for window event, ≥3 of 4 for alarm), not the earlier “≥4 of 5” from the doc.
- **Docker:** Only zookeeper, kafka, spark, notification in compose; backend separate.
- **Phase diagram:** Phase 0 and 1 done; Phase 2 partially done with clear remaining work; Phases 3–6 marked as remaining with short bullets.

---
docker-compose.yml
```yml 
services:

  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:latest
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_NODE_ID: 1
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093

      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,PLAINTEXT_INTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://kafka:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER

      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0

  spark:
    build:
      context: .
      dockerfile: Dockerfile.spark
    container_name: spark
    depends_on:
      - kafka
    ports:
      - "7077:7077"
      - "8081:8080"
    volumes:
      - ./spark-app:/spark-app


  notification:
    build:
      context: .
      dockerfile: Dockerfile.notification
    container_name: notification
    depends_on:
      - kafka
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:29092
```