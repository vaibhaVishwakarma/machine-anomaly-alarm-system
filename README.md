# Real-Time Machine Anomaly Alarm System v3

## Overview
This project implements a real-time anomaly detection pipeline for industrial machine audio. Audio is uploaded through a web interface, streamed through Kafka, processed by Spark Structured Streaming, and analyzed with a pretrained WaveNet-style model to detect abnormal machine behavior.

When sustained anomalies are detected, the system emits alert events that are routed to a notification service for email delivery. The design is modular, containerized, and orchestrated via Docker Compose.

- 📦 **Pre-trained Model on Kaggle**: **[vaibhavishwakarma/sw-wavenet-arcface-pump-machines-only](https://www.kaggle.com/models/vaibhavishwakarma/sw-wavenet-arcface-pump-machines-only/)**

---

## Configuration notes
- Internal Docker services use `kafka:29092` for Kafka communication
- The host port `localhost:9092` is exposed for manual inspection or debugging, but the application services themselves use `kafka:29092`
- Spark consumes from `raw_audio_topic` and publishes to `prediction_topic` and `alert_event_topic`
- Notification service reads from `alert_event_topic` and uses the backend endpoint for recipient lookup

---

## Running the project

### Prerequisites
- Docker and Docker Compose installed.
- (Optional) Download dataset audio for testing:
  ```bash
  python Dataset/download_dataset.py
  ```

### Start the full stack
```bash
docker compose up --build
```

This starts:

- Kafka and Zookeeper
- Backend
- Spark 
- Notification service

### Access the application
- Upload UI: http://localhost:8000/
- Dashboard: http://localhost:8000/dashboard

---

## 🎯 Model Evaluation & Processing Layer Advantage

This system uses a **SW-WaveNet** architecture with **ArcFace** representation learning to perform anomalous sound detection directly from raw machine audio and log-mel spectrograms. (Pretrained weights available on **[Kaggle Models](https://www.kaggle.com/models/vaibhavishwakarma/sw-wavenet-arcface-pump-machines-only/)**; see the complete **[Training & Evaluation Guide](Training%20And%20Evaluation/README.md)** for architecture deep dive and training scripts).

To bridge the gap between noisy instantaneous AI predictions and reliable industrial operations, the system integrates a **Temporal Processing Layer (Binomial Consensus Filter)** that dramatically reduces false alarms while maximizing true anomaly detection.

![Evaluation Plots](assets/Plots.png)

---

### 1. Frame-Level Detection Performance

Every 5 seconds, the model evaluates the latest 10-second audio window (160,000 samples at 16 kHz) and computes an anomaly score from the target machine logits ($-\text{logit}_{\text{target}}$).

Thresholds ($\tau_m$) are calibrated dynamically per machine to guarantee high sensitivity:

| Machine ID | Threshold ($\tau_m$) | Target Logit | Frame Detection Rate (TPR) | Frame Miss Rate (FNR) | Frame False Alarm Rate (FPR) | Frame Accuracy |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Machine ID 00** | `-24.0857` | `24.09` | **74.83%** (107/143) | 25.17% (36/143) | 16.00% (16/100) | 78.60% |
| **Machine ID 02** | `-25.4341` | `25.43` | **73.87%** (82/111) | 26.13% (29/111) | 21.00% (21/100) | 76.30% |
| **Machine ID 04** | `-18.2705` | `18.27` | **90.00%** (90/100) | 10.00% (10/100) | 0.00% (0/100) | 95.00% |

---

### 2. The Power of the Processing Layer (Temporal Binomial Consensus)

In real factory environments, instantaneous single-frame predictions are prone to noise (e.g., dropping a tool, momentary electrical spikes, or passing vehicles). Triggering alarms on isolated frames leads to alert fatigue and false shut-downs.

```text
Stream:         [ 5s Chunk ]  [ 5s Chunk ]  [ 5s Chunk ]  [ 5s Chunk ]  [ 5s Chunk ]
Model Input:    <------- 10s Window (Last 2 Chunks) ------->
Buffer (25s):   [ Pred 1 ]    [ Pred 2 ]    [ Pred 3 ]    [ Pred 4 ]    [ Pred 5 ]
```

#### The Stability Decision Rule ($\ge 3$ of 5 Consensus)
- **$0\times, 1\times,$ or $2\times$ out of 5 Anomalies**: Classified as **unstable / transient noise** $\to$ **Ignored (No Alarm)**.
- **$3\times, 4\times,$ or $5\times$ out of 5 Anomalies**: Classified as a **stable, persistent breakdown** $\to$ **ALARM IS TRIGGERED! 🚨**.

#### Binomial Consensus Formula
The probability of triggering an alarm over the 5-frame window is modeled as:

$$P(\text{Alarm}) = P(k \ge 3 \text{ of } 5) = \sum_{k=3}^{5} \binom{5}{k} p^k (1 - p)^{5-k} = 10 p^3(1-p)^2 + 5 p^4(1-p) + p^5$$

- **Factor 1: MISSED ALARM RATE** $= 1 - P(\text{Alarm} \mid p = \text{Frame Detection Rate})$
- **Factor 2: FALSE ALARM RATE** $= P(\text{Alarm} \mid p = \text{Frame False Alarm Rate})$

---

### 3. Processing Layer Results: Massive False Alarm Suppression

Applying the 3-of-5 consensus rule transforms noisy single-frame predictions into robust, enterprise-grade alarm reliability:

| Machine ID | Single Frame Detection | Single Frame Miss (FNR) | Single Frame False Alarm | Layer True Alarm Rate ($\ge 3/5$) | **MISSED ALARM RATE** ($1 - \text{True Alarm}$) | **FALSE ALARM RATE** ($P(\text{Alarm} \mid \text{Normal})$) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Machine ID 00** | 74.83% | 25.17% | 16.00% | **89.463%** | **10.537%** | **3.176%** *(down from 16.0%)* |
| **Machine ID 02** | 73.87% | 26.13% | 21.00% | **88.425%** | **11.575%** | **6.589%** *(down from 21.0%)* |
| **Machine ID 04** | 90.00% | 10.00% | 0.00% | **99.144%** | **0.856%** | **0.000%** *(zero false alarms)* |

#### Key Benefits:
1. **False Alarm Collapse**: On Machine ID 00 & 02, false alarms drop from **$16\% \text{--} 21\%$ down to $3.1\% \text{--} 6.6\%$**, eliminating up to **80% of spurious alerts**.
2. **Detection Amplification**: True alarm reliability increases from $\sim 74\%$ up to **$88.4\% \text{--} 99.1\%$** because sustained anomalous acoustics consistently satisfy the 3-of-5 persistence criteria.

![Paper Benchmarks](assets/paper-benchmarks.png)

---

## Architecture

The system is composed of the following modular services:

- **[Backend API & Web Dashboard](backend/README.md)**
  - Serves the upload page and live monitoring dashboard
  - Receives uploaded audio files and streams 1-second chunks to Kafka
  - Exposes monitoring and alert-related telemetry endpoints

- **Kafka & Zookeeper**
  - Central event backbone for asynchronous streaming communication
  - Connects the backend, Spark streaming, and notification service

- **[Spark Structured Streaming](spark-app/README.md)**
  - Ingests audio streams from Kafka in micro-batches
  - Runs sliding-window inference with pre-trained SW-WaveNet
  - Evaluates the temporal decision layer and publishes predictions & alert events

- **[Notification Service](notification_service/README.md)**
  - Consumes alert events from Kafka
  - Deduplicates repeated alerts using per-machine cooldown windows
  - Retrieves active recipients and delivers alert emails via SMTP (TLS)

```text
Upload UI → Backend → Kafka → Spark → Kafka → Backend / Notification Service
```

The main Kafka topics are:

- raw_audio_topic
  - Incoming machine audio payloads

- prediction_topic
  - Inference outputs from Spark

- alert_event_topic
  - Alarm-level events emitted by the decision layer

---

## Spark role
Spark is responsible for the streaming inference pipeline.

It performs:

- Kafka input consumption
- Audio decoding and preprocessing
- Windowed inference over time-based audio chunks
- Prediction generation
- Decision-layer evaluation for sustained anomalies
- Publishing of predictions and alerts back to Kafka

The Spark job uses Structured Streaming and runs in a container so it can work alongside Kafka and the backend.

---

## Kafka role
Kafka acts as the communication backbone between services.

It enables:

- decoupled communication between producers and consumers
- reliable buffering of streaming events
- fan-out to the backend and notification service
- future scalability for additional consumers or pipelines

Inside Docker, services communicate with Kafka using the internal address `kafka:29092`.

---

## Decision layer
The decision layer is designed to reduce false positives and avoid noisy alerts.

It evaluates a sequence of predictions over time and only triggers alerts when anomaly behavior becomes sustained.

The implemented logic is:

- Window event: at least 3 anomalies in 5 consecutive inferences
- Alarm trigger: at least 3 window events in 4 consecutive window events
- State transition: NORMAL → ALARM when the threshold is crossed
- Recovery: ALARM → NORMAL when the anomaly pattern clears

This logic is applied in Spark before an alert event is published to `alert_event_topic`.

---

## Notification behavior
The notification service consumes alert events from Kafka and handles delivery carefully.

Features include:

- Deduplication
  - Suppresses repeated alerts for the same machine within a cooldown window
- Exponential backoff
  - Retries failed notifications with increasing delays to avoid hammering the email service
- Recipient lookup
  - Pulls configured email recipients from the backend endpoint
- Email delivery
  - Sends notifications using SMTP

This makes the alerting flow more stable and reduces alert storms.

---

## Backend endpoints
The backend exposes the following endpoints:

### UI endpoints

| Endpoint | Purpose |
| --- | --- |
| GET / | Renders the upload page |
| GET /dashboard | Renders the monitoring dashboard |

### Data endpoints

| Endpoint | Purpose |
| --- | --- |
| GET /predictions | Returns the latest prediction stream data |
| GET /sequence_status | Returns the current sequence execution state |
| GET /machine_states | Returns per-machine alarm state information |
| GET /alert_email | Returns the configured alert email address |

### Action endpoints

| Endpoint | Purpose |
| --- | --- |
| POST /start_sequence | Starts the processing sequence for uploaded files and accepts an alert email value |
| POST /stop_sequence | Stops the active processing sequence |

---

## Features
- Real-time audio ingestion
- Streaming anomaly inference
- Kafka-based event handling
- Spark Structured Streaming processing
- Dashboard-based monitoring
- Alert generation from sustained anomalies
- Deduplicated notifications
- Exponential backoff for notification retries
- Dockerized deployment

---

## Project wrap-up
This project demonstrates a complete streaming machine-learning pipeline for anomaly detection. It combines a FastAPI backend, Apache Kafka for event streaming, Apache Spark for real-time inference, and a notification layer for actionable alerts.

The architecture is designed to be practical, extensible, and production-ready in spirit, with room for future hardening such as persistent storage, better observability, and more advanced retry and delivery handling.

---

## For contributors and developers
If you are adding new features, new services, or new alert logic, use the current system snapshot as a reference point.

The snapshot in [system-snapshot-phase-breakdown.md](system-snapshot-phase-breakdown.md) is intended to help contributors quickly understand:

- the current architecture and service boundaries
- the Kafka topic flow
- the Spark decision layer and alert publication path
- the notification-service behavior and current limitations

This makes it easier to extend the system without breaking the existing streaming flow or alerting model.

---

## Future enhancements
Possible improvements include:

- Persistent storage for alerts and predictions
- Dead-letter queues for failed notifications
- More advanced alert routing
- Metrics and monitoring dashboards
- Model version tracking and retraining support
- Horizontal scaling for Spark and notification workers
