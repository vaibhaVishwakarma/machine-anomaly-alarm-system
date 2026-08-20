# Notification Service

## 📋 Overview
The **Notification Service** is the alerting and dispatch component of the Machine Anomaly Alarm System. It listens for verified alarm events on Kafka, prevents alert storms through per-machine cooldown deduplication, retrieves dynamic recipient lists from the backend, and securely dispatches email alerts via SMTP.

---

## 🏗️ Architecture & Alert Flow

```text
Kafka [ alert_event_topic ]
         │
         ▼
[ Kafka Consumer (consumer.py) ]
         │
         ├── 1. Filter: Checks alarm_state == "ALARM_TRIGGERED"
         │
         ├── 2. Cooldown & Deduplication (state_manager.py)
         │       └── Is (now - last_sent) >= 150s for machine_id?
         │             ├── NO  ──► Suppress Alert (Cooldown Active)
         │             └── YES ──► Proceed
         │
         ├── 3. Dynamic Recipient Lookup (GET /alert_email from Backend)
         │       └── Regex validation & deduplication
         │
         └── 4. SMTP Dispatch (notifier.py)
                 └── Formats alert payload & sends via Gmail TLS (Port 587)
```

---

## ⚙️ Key Modules & File Breakdown

### 1. `consumer.py` (Main Kafka Event Consumer)
- **Kafka Subscription**: Consumes JSON payloads from `alert_event_topic` at `kafka:29092`.
- **Event Filter**: Ignores non-alarm messages, processing only payloads where `alarm_state == "ALARM_TRIGGERED"`.
- **Recipient Resolution**: Queries the backend API (`GET /alert_email`) to dynamically fetch recipients configured via the UI dashboard.
- **Email Regex Sanitization**:
  ```python
  EMAIL_REGEX = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"
  ```

---

### 2. `state_manager.py` (Alert Deduplication & Cooldown)
In industrial settings, an active breakdown might emit continuous alarm events. The `AlertStateManager` prevents mailbox spamming:
- **Per-Machine Tracking**: Stores the timestamp of the last dispatched email for each `machine_id`.
- **Cooldown Window**:
  - `COOLDOWN_SECONDS = 150` (Configurable in `config.py`).
  - If a new alarm for the same machine arrives within 150 seconds of the previous email, it is suppressed.

---

### 3. `notifier.py` (Email Formatting & SMTP Dispatch)
- **Timezone Conversion**: Converts raw UNIX timestamps into human-readable Indian Standard Time (IST, `UTC+5:30`):
  ```python
  def get_datestring_UNIX_to_IST(t) -> str:
      IST = timezone(timedelta(hours=5, minutes=30))
      return datetime.fromtimestamp(t, tz=IST).strftime('%Y-%m-%d %H:%M:%S')
  ```
- **Email Structure**:
  - **Subject**: `ALERT: Machine <machine_id>`
  - **Body Content**:
    ```text
    Alarm Triggered

    Machine ID: 0
    Severity: HIGH
    Confidence Score: 25.14
    String Timestamp: 2026-08-20 23:45:00
    Integer Timestamp: 1771485600
    ```
- **Security & Transport**: Uses `smtplib` with `STARTTLS` on `smtp.gmail.com:587`.

---

### 4. `config.py` (Service Configuration)
```python
import os

KAFKA_BOOTSTRAP = "kafka:29092"
ALERT_TOPIC = "alert_event_topic"
BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")

COOLDOWN_SECONDS = 150  # Deduplication window

EMAIL_HOST = "smtp.gmail.com"
EMAIL_PORT = 587
EMAIL_USER = "example@gmail.com"
EMAIL_PASSWORD = "your-16-character-app-password"
```

---

## 📡 Incoming Kafka Contract (`alert_event_topic`)

```json
{
  "timestamp": 1771485625,
  "machine_id": 0,
  "alarm_state": "ALARM_TRIGGERED",
  "window_event_count": 4,
  "model_version": "v1",
  "severity": "HIGH",
  "confidence_score": 25.14
}
```

---

## 🔐 Gmail App Password Setup

To enable email dispatch with Gmail:
1. Enable **2-Step Verification** on your Google account.
2. Go to **Manage Google Account > Security > 2-Step Verification > App Passwords**.
3. Create a new App Password (e.g. named `MachineAlarmSystem`).
4. Copy the 16-character generated password and place it in `notification_service/config.py`:
   ```python
   EMAIL_USER = "your_email@gmail.com"
   EMAIL_PASSWORD = "xxxx xxxx xxxx xxxx"
   ```

---

## 🐳 Docker Deployment

### `Dockerfile.notification`
```dockerfile
FROM python:3.10-slim
WORKDIR /app
COPY notification_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY notification_service/ .
CMD ["python", "consumer.py"]
```

### Docker Compose Service Definition
```yaml
notification:
  build:
    context: .
    dockerfile: Dockerfile.notification
  container_name: notification
  depends_on:
    init-kafka:
      condition: service_completed_successfully
  environment:
    - KAFKA_BOOTSTRAP_SERVERS=kafka:29092
    - BACKEND_URL=http://backend:8000
  restart: on-failure
```

---

## 🔍 Local Inspection & Debugging

### View Notification Logs
```bash
docker logs -f notification
```

### Manually Test Alert Ingestion via Kafka
```bash
docker exec -it kafka kafka-console-producer \
  --bootstrap-server kafka:9092 \
  --topic alert_event_topic

# Paste sample alert payload:
{"timestamp": 1771485600, "machine_id": 0, "alarm_state": "ALARM_TRIGGERED", "window_event_count": 3, "severity": "HIGH", "confidence_score": 24.5}
```
