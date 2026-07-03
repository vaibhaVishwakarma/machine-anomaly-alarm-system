# Real-Time Machine Anomaly Alarm System v3

## Overview
This project implements a real-time anomaly detection pipeline for industrial machine audio. Audio is uploaded through a web interface, streamed through Kafka, processed by Spark Structured Streaming, and analyzed with a pretrained WaveNet-style model to detect abnormal machine behavior.

When sustained anomalies are detected, the system emits alert events that are routed to a notification service for email delivery. The design is modular, containerized, and orchestrated via Docker Compose.

---

## Configuration notes
- Internal Docker services use `kafka:29092` for Kafka communication
- The host port `localhost:9092` is exposed for manual inspection or debugging, but the application services themselves use `kafka:29092`
- Spark consumes from `raw_audio_topic` and publishes to `prediction_topic` and `alert_event_topic`
- Notification service reads from `alert_event_topic` and uses the backend endpoint for recipient lookup

---

## Running the project

### Prerequisites
Make sure Docker and Docker Compose are installed.

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

## Architecture

The system is composed of the following services:

- Backend API
  - Serves the upload page and dashboard
  - Receives uploaded audio files
  - Publishes audio events to Kafka
  - Exposes monitoring and alert-related endpoints

- Kafka
  - Acts as the central event backbone for asynchronous communication
  - Connects the backend, Spark, and notification service

- Spark Structured Streaming
  - Reads audio from Kafka
  - Runs inference on streaming audio windows
  - Publishes prediction and alert events back to Kafka

- Notification Service
  - Consumes alert events from Kafka
  - Deduplicates repeated alerts
  - Applies exponential backoff for failed notifications
  - Sends email alerts through SMTP

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
- GET /
  - Renders the upload page

- GET /dashboard
  - Renders the monitoring dashboard

### Data endpoints
- GET /predictions
  - Returns the latest prediction stream data

- GET /sequence_status
  - Returns the current sequence execution state

- GET /machine_states
  - Returns per-machine alarm state information

- GET /alert_email
  - Returns the configured alert email address

### Action endpoints
- POST /start_sequence
  - Starts the processing sequence for uploaded files
  - Accepts audio files and an alert email value

- POST /stop_sequence
  - Stops the active processing sequence

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
