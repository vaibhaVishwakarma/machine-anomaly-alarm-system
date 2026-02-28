Below is a **clear system snapshot document** of your current working state.
You can save this as:

```
SYSTEM_STATE.md
```

This acts as a **failover checkpoint reference** for rebuilding the system exactly as it works now.

---

# 📦 SYSTEM OVERVIEW

### Architecture

```
Upload UI
   ↓
FastAPI Backend
   ↓
Kafka topic: raw_audio_topic
   ↓
Spark Structured Streaming
   ↓
Kafka topic: prediction_topic
   ↓
Backend Kafka Consumer
   ↓
Dashboard (Chart.js)
```

---

# 📁 DIRECTORY STRUCTURE

```
project-root/
│
├── docker-compose.yml
├── Dockerfile.spark
├── requirements.txt
│
├── backend/
│   ├── main.py
│   ├── kafka_producer.py
│   ├── kafka_consumer.py
│   └── sequence_manager.py
│
├── frontend/
│   └── templates/
│        ├── upload.html
│        └── dashboard.html
│
├── spark-app/
│   ├── stream_inference.py
│   └── model/
│        └── sw_wavenet_traced_cpu.pt
```

---

# 🧠 MODEL CONFIGURATION

### Model Type

SW_WaveNet (multi-class classifier used for anomaly detection)

### Input

* Shape: `[1, 1, 160000]`
* Sample rate: 16000 Hz
* Duration: 10 seconds
* Audio sent in 1-second chunks (16000 samples)

### Inference Logic

1. Buffer 10 chunks (10 sec window)
2. Perform inference every 5 chunks

   * WINDOW_CHUNKS = 10
   * STEP_CHUNKS = 5

### Scoring Method (DCASE style)

```
logits → softmax → probability(machine_id)
score = -log(probability + 1e-9)
```

### Threshold

```
THRESHOLD = 1.192093e-07  # Youden Index
	   + 75e-5       # margin added to avoid misclassification of normal to anomaly
```

Prediction rule:

```
ANOMALY if score >= THRESHOLD
NORMAL otherwise
```

---

# ⚙️ SPARK STREAM CONFIGURATION

### Kafka Source

```
topic: raw_audio_topic
bootstrap: kafka:29092
```

### Kafka Sink

```
topic: prediction_topic
bootstrap: kafka:29092
```

### Trigger

```
.trigger(processingTime="5 seconds")
```

### Checkpoint Location

```
/tmp/checkpoint_audio_v3
```

### Spark Config

```
spark.sql.streaming.metricsEnabled = false
```

---

# 📡 KAFKA CONFIGURATION

### Topics

| Topic            | Purpose                         |
| ---------------- | ------------------------------- |
| raw_audio_topic  | Receives 1-second audio packets |
| prediction_topic | Receives inference results      |

### Networking

Inside Docker:

```
kafka:29092
```

Outside Docker:

```
localhost:9092
```

---

# 🖥 BACKEND CONFIGURATION

### Framework

FastAPI

### Port

```
8000
```

### Endpoints

| Endpoint           | Purpose                   |
| ------------------ | ------------------------- |
| `/`                | Upload page               |
| `/dashboard`       | Monitoring dashboard      |
| `/predictions`     | Returns inference results |
| `/sequence_status` | Returns upload progress   |

---

# 📊 DASHBOARD CONFIGURATION

### Chart 1 — Inference Results

* Type: Bar chart
* Window: 150 seconds
* Resolution: 5 seconds
* 30 inference points

Value Mapping:

| Value | Meaning   |
| ----- | --------- |
| 0     | No result |
| 1     | NORMAL    |
| 2     | ANOMALY   |

---

### Chart 2 — Data Throughput

* Type: Line chart
* Shows KB per inference
* Window: 150 seconds

---

### Circle

* Displays total KB uploaded
* Derived from `/sequence_status`

---

# 🔁 AUDIO SEQUENCE SYSTEM

### Upload Behavior

* User selects files
* Files broken into 1-second chunks
* Chunks sent sequentially
* Order determined by UI sequence

### Filename Convention

```
<alphabets>_id_<id>_<sourceid>.wav
```

ID Mapping:

| ID    | Internal Class |
| ----- | -------------- |
| 00    | 0              |
| 02    | 1              |
| 04    | 2              |
| Other | default        |

---

# 🧵 BUFFERING LOGIC (Spark)

For each machine_id:

```
buffer_store[machine_id] = []
```

Rules:

* Append each 1-second chunk
* When buffer length ≥ 10
* Run inference when:
  (buffer_length - 10) % 5 == 0
* Do NOT clear buffer
* Sliding step = 5

---

# 🐳 DOCKER STATE

Services:

* Zookeeper
* Kafka
* Backend
* Spark

All services communicate via Docker network.

Environment variable used:

```
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
```

---

# ⚠️ KNOWN STABILITY NOTES

1. Must delete checkpoint if stream corrupted:

   ```
   rm -rf /tmp/checkpoint_audio_v3
   ```

2. Trigger must be ≥ inference time
   Currently:

   ```
   5 seconds
   ```

3. Spark falling behind warning indicates heavy model load.

4. TorchScript model must accept:

   ```
   model(waveform)
   ```

   NOT:

   ```
   model(waveform, None)
   ```

---

# 📈 PERFORMANCE CHARACTERISTICS

* Inference time ≈ 2–3 seconds
* Batch interval = 5 seconds
* 1 inference every 5 seconds
* 150-second dashboard window

---

# 🚦 CURRENT SYSTEM STATUS

✔ Upload working
✔ Kafka topics functioning
✔ Spark inference stable
✔ Sliding 10s window, step 5s
✔ Dashboard 150-second view
✔ Dockerized services operational

---

# 🧩 WHAT IS NOT INCLUDED

* No Airflow
* No alert system
* No DB persistence
* Predictions stored in memory
* No model retraining pipeline

---

# 🛑 FAILOVER CHECKPOINT SUMMARY

To rebuild system:

1. Ensure model file exists:

   ```
   spark-app/model/sw_wavenet_traced_cpu.pt
   ```

2. Kafka topics exist:

   ```
   raw_audio_topic
   prediction_topic
   ```

3. Spark uses:

   * WINDOW_CHUNKS = 10
   * STEP_CHUNKS = 5
   * THRESHOLD = 1.192093e-07
   * trigger = 5 seconds

4. Dashboard window = 150 seconds

5. Backend connected to kafka:29092 inside Docker

---

## Current Dockerfile.spark
```yml
# ------------------------------------------------------------
# Base Image (AWS Bitnami Spark)
# ------------------------------------------------------------
FROM public.ecr.aws/bitnami/spark:latest

USER root

# ------------------------------------------------------------
# Install pip (Bitnami Python already exists)
# ------------------------------------------------------------
RUN /opt/bitnami/python/bin/python -m pip install --upgrade pip

# ------------------------------------------------------------
# Install PyTorch CPU + dependencies
# ------------------------------------------------------------
RUN /opt/bitnami/python/bin/pip install torch torchaudio numpy torchcodec soundfile


# ------------------------------------------------------------
# Fix missing passwd entry (Invalid UID error fix)
# ------------------------------------------------------------
RUN echo "sparkuser:x:1001:1001::/home/sparkuser:/bin/bash" >> /etc/passwd && \
    mkdir -p /home/sparkuser && \
    chown -R 1001:1001 /home/sparkuser

# Install FFmpeg and the required shared library headers
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    libavcodec-dev \
    libavformat-dev \
    libavutil-dev \
    libswresample-dev \
    libswscale-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV HOME=/home/sparkuser

# ------------------------------------------------------------
# Fix Ivy cache issue
# ------------------------------------------------------------
ENV SPARK_SUBMIT_OPTS="-Divy.home=/home/sparkuser -Divy.cache.dir=/home/sparkuser/.ivy2"

# ------------------------------------------------------------
# Force Spark to use Bitnami Python (IMPORTANT)
# ------------------------------------------------------------
ENV PYSPARK_PYTHON=/opt/bitnami/python/bin/python
ENV PYSPARK_DRIVER_PYTHON=/opt/bitnami/python/bin/python

USER 1001
```




### current version of docker-compose.yml
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

# ======== Docker Helper commands =============



  # docker-compose up -d
  # docker-compose down -v
  # docker-compose build --no-cache

  # docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --list

  # docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --create --topic raw_audio_topic  --partitions 1 --replication-factor 1
  # docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --create --topic inference_results_topic  --partitions 1 --replication-factor 1


  # docker exec -it spark spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 /spark-app/stream_inference.py
  # docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic prediction-topic --from-beginning
  

```







Below is the extended **phase-wise system development report**, now including a properly engineered **Notification & Alerting Service layer** integrated into the architecture, scaling plan, reliability logic, and testing strategy.

---

# REAL-TIME ACOUSTIC ANOMALY DETECTION PLATFORM

## Phase-Wise Development, Reliability & Notification Architecture Blueprint

---

# 🔷 PHASE 0 — Baseline Streaming Inference System

## Objective

Deploy a stable real-time anomaly inference pipeline.

### Current Architecture

Upload → FastAPI → Kafka → Spark (window + inference) → Kafka → Backend → Dashboard

### Characteristics

* One-class model
* 10 sec window
* 5 sec step
* 3.3 sec inference
* Threshold-based decision

---

# 🔷 PHASE 1 — Event-Level Reliability Layer

## Objective

Convert model-level noise into reliable alarms.

### Event Rules

Window event (25 sec):
≥4 anomalies out of 5 inferences

Alarm rule (100 sec):
≥3 window events out of 4

### Mathematical Suppression

If model FPR = p,

Window false probability:

[
P(X \ge 4) = \binom{5}{4}p^4(1-p) + p^5
]

Alarm false probability:

[
P(C \ge 3) \approx \binom{4}{3}q^3(1-q) + q^4
]

Result: Industrial-grade false alarm reduction.

---

# 🔷 PHASE 2 — Notification & Alert Service Layer

This is now a dedicated architecture component.

---

## 🎯 Objective

Deliver reliable, non-duplicated, rate-controlled alerts to concerned stakeholders (email, SMS, webhook, etc.).

---

## Updated Architecture

Upload → Kafka → Spark → Event Logic
→ **Alert Event Topic (Kafka)**
→ Notification Service
→ Email / SMS / Webhook

---

## Why Separate Notification Service?

Do NOT send email from Spark or backend directly.

Reasons:

* Prevent blocking inference
* Retry logic required
* Alert deduplication
* Rate limiting
* Multi-channel support
* Audit logging

---

## Notification Service Responsibilities

1. Consume alert events from Kafka
2. Validate alarm state
3. Deduplicate alerts
4. Apply cooldown
5. Send notification
6. Persist notification log

---

## Alert Payload Structure

Store structured payload:

* machine_id
* timestamp_start
* timestamp_end
* event_count
* model_version
* dataset_version
* severity_level
* confidence_score

Never send raw model decision only.

---

## Alert Deduplication Logic

Maintain state:

If machine already in ALERT state:

Do not resend unless:

* Alarm cleared and re-triggered
* Or escalation threshold crossed

---

## Cooldown Strategy

After alert:

Enter COOLDOWN state for X minutes.

Prevents flooding.

---

## Severity Levels

Based on event strength:

| Event Windows | Severity |
| ------------- | -------- |
| 3/4           | Medium   |
| 4/4           | High     |

Optional escalation if persists beyond 5 minutes.

---

## Delivery Channels

Phase-wise rollout:

Phase A:
Email (AWS SES or SMTP)

Phase B:
Slack / Webhook

Phase C:
SMS (SNS)

---

## Failure Handling

If email fails:

* Retry with exponential backoff
* Dead-letter queue for failed alerts

---

## Audit Logging

Persist in DB:

* Alert ID
* Sent time
* Delivery status
* Acknowledged status
* Escalation level

This is critical for industrial compliance.

---

# 🔷 PHASE 3 — Simulation & Alert Testing

## Notification Testing Strategy

### Macro Tests

1. Inject sustained anomaly → verify single alert
2. Inject intermittent anomaly → verify no alert
3. Inject repeated anomaly → verify cooldown works
4. Kill notification service → verify retry behavior

---

### Micro Tests

* Duplicate event prevention
* Idempotent email send
* Retry counter logic
* State reset after normal recovery

---

# 🔷 PHASE 4 — Scaling & Autoscaling (CPU / GPU)

## Updated Architecture with GPU Option

Spark → Alert Kafka
→ Notification Service (CPU)
→ GPU Inference Service (if used)

Notification remains lightweight and separate.

---

## Scaling Strategy

Notification service:

* Low CPU requirement
* Scales based on alert throughput

Inference service:

* Scales based on pump count

---

## Cost Impact

Notification cost minimal compared to inference.

Example:

100 alerts/day
SES cost negligible (<$1/month)

SNS SMS:
$0.006–0.01 per SMS

---

# 🔷 PHASE 5 — Drift Monitoring & Retraining

Notification layer also integrates:

* Drift warning alerts
* Model degradation alerts
* Retraining trigger alerts

This separates:

Operational anomaly alerts
vs
System health alerts

---

# 🔷 PHASE 6 — Production Hardening with Notification

## Additional Tests

* Alert storm simulation
* Multi-machine concurrent alerts
* Escalation policy test
* Email provider failure test
* Kafka topic corruption recovery

---

# 🔷 State Machine Including Notification

NORMAL
→ WINDOW_EVENT
→ EVENT_ACCUMULATING
→ ALARM_TRIGGERED
→ ALERT_SENT
→ COOLDOWN
→ NORMAL

Each transition logged.

---

# 🔷 Data Storage Structure (Final Form)

Never overwrite labels.

Store layered structure:

* raw_score
* model_label
* window_event
* alarm_flag
* alert_sent_flag
* acknowledged_flag
* model_version
* dataset_version

---

# 🔷 Long-Term Enhancements

1. Alert acknowledgment portal
2. Multi-recipient routing per machine
3. Escalation chain (if not acknowledged)
4. Predictive warning before alarm
5. Alert quality analytics (precision of alerts)

---

# 🔷 Final Integrated System Architecture

Pump → Kafka → Spark Windowing → Model Inference
→ Event Logic → Alert Topic → Notification Service
→ Email / SMS / Slack

Parallel:

Metrics → Drift Monitor → Health Alerts

---

# 🔷 Phase-Wise Summary Table

| Phase | Focus        | Output                    |
| ----- | ------------ | ------------------------- |
| 0     | Inference    | Raw anomaly score         |
| 1     | Event Logic  | Reliable alarm flag       |
| 2     | Notification | Controlled alert delivery |
| 3     | Simulation   | Verified reliability      |
| 4     | Scaling      | Horizontal scalability    |
| 5     | Drift        | Model health monitoring   |
| 6     | Hardening    | Production resilience     |

---

# Final System Philosophy

Reliability comes from:

Model

* Temporal aggregation
* State machine
* Notification control
* Drift awareness
* Autoscaling

Not from model alone.

---
