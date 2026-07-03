# Real-time Anomaly Detection System for Machine Audio

## Project Description
This project implements a real-time anomaly detection system for industrial machine audio, designed to identify abnormal operational patterns and trigger immediate alerts. Leveraging a robust streaming architecture, it processes audio data, performs machine learning inference, and manages a sophisticated event-based alerting mechanism, all orchestrated through Docker Compose.

## Architecture Overview
The system is built upon a microservices architecture, integrating several key components:

1.  **Upload UI**: A web interface for users to upload machine audio files.
2.  **FastAPI Backend**: Acts as the central API gateway, handling audio uploads, publishing raw audio chunks to Kafka, serving the monitoring dashboard, and providing endpoints for alert configuration and machine state management.
3.  **Apache Kafka**: Serves as the high-throughput message broker, facilitating real-time data flow between services through three main topics: `raw_audio_topic`, `prediction_topic`, and `alert_event_topic`.
4.  **Apache Spark Structured Streaming**: Consumes raw audio data from Kafka, performs windowed processing, applies a pre-trained WaveNet deep learning model for anomaly inference, and implements a multi-stage event reliability layer to detect sustained anomalies before publishing predictions and critical alerts back to Kafka.
5.  **Notification Service**: A dedicated Python service that subscribes to critical alert events from Kafka. It deduplicates alerts based on a cooldown period, fetches recipient emails from the FastAPI backend, and dispatches email notifications via SMTP.
6.  **Monitoring Dashboard**: An interactive web dashboard (served by the FastAPI backend) that visualizes real-time inference results, machine states (NORMAL/ALARM), and alert timelines.

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

## Features
*   **Real-time Audio Ingestion**: Upload 10-second audio files, which are chunked into 1-second segments and streamed via Kafka.
*   **Deep Learning Anomaly Detection**: Utilizes a pre-trained `SW_WaveNet` model for multi-class anomaly detection on streaming audio data.
*   **Sophisticated Event Reliability**: Implements a multi-stage anomaly detection logic:
    *   **Window Event**: Triggers if ≥3 anomalies are detected within 5 consecutive 1-second inferences (a 25-second window).
    *   **Alarm Trigger**: Activates if ≥3 window events occur within 4 consecutive window events (a 100-second window), transitioning a machine from NORMAL to ALARM state.
*   **Kafka-based Messaging**: Decoupled services communicating efficiently through Kafka topics.
*   **Real-time Monitoring Dashboard**: Provides a dynamic overview of machine statuses and inference outcomes.
*   **Deduplicated Email Notifications**: Sends alert emails to configurable recipients with a built-in cooldown mechanism to prevent alert storms.
*   **Containerized Deployment**: Services are containerized using Docker and orchestrated with Docker Compose for easy setup and scalability.

## Technologies Used
*   **Backend**: Python, FastAPI, Uvicorn
*   **Messaging**: Apache Kafka
*   **Stream Processing & ML**: Apache Spark Structured Streaming, PyTorch
*   **Containerization**: Docker, Docker Compose
*   **Frontend**: HTML, JavaScript (Chart.js)
*   **Development Environment**: Python 3.11 (Backend), Python 3.10 (Notification Service)

## Setup and Reproduction
To set up and run the project, ensure you have Docker and Docker Compose installed on your system.

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd mlproject
    ```
    (Assuming the user has already cloned the repo, this step is conceptual for the README)

2.  **Build and Start Core Services (Kafka, Zookeeper, Spark, Notification):**
    Navigate to the project root and run:
    ```bash
    docker-compose up --build -d
    ```
    This will bring up the Kafka, Zookeeper, Spark, and Notification services in detached mode.

3.  **Create Kafka Topics:**
    Once Kafka is running, create the necessary topics by executing the following command in your terminal. This command will connect to the Kafka container and create `raw_audio_topic`, `prediction_topic`, and `alert_event_topic`.
    ```bash
    docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --create --topic raw_audio_topic --partitions 1 --replication-factor 1; \
    docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --create --topic prediction_topic --partitions 1 --replication-factor 1; \
    docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --create --topic alert_event_topic --partitions 1 --replication-factor 1
    ```

4.  **Install Backend Dependencies and Run FastAPI Service:**
    The FastAPI backend is designed to run outside the `docker-compose` network for development flexibility.
    ```bash
    cd backend
    pip install -r requirements.txt
    uvicorn main:app --host 0.0.0.0 --port 8000
    ```
    The backend will be accessible at `http://localhost:8000`.

5.  **Run Spark Structured Streaming Application:**
    After the Kafka and Spark containers are up, submit the Spark application. This command runs the `stream_inference.py` script on the Spark cluster, connecting to Kafka.
    ```bash
    docker exec -it spark spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 /spark-app/stream_inference.py
    ```

6.  **Access the Application:**
    *   **Upload UI**: Open your browser and navigate to `http://localhost:8000/`.
    *   **Monitoring Dashboard**: Open your browser and navigate to `http://localhost:8000/dashboard`.

## Configuration Notes
*   **Kafka**: Inside Docker, services communicate with Kafka at `kafka:29092`. From outside Docker (e.g., the FastAPI backend), use `localhost:9092`.
*   **Notification Service**: Connects to Kafka via `KAFKA_BOOTSTRAP_SERVERS=kafka:29092` and to the FastAPI backend for alert recipient emails via `BACKEND_URL` (e.g., `http://host.docker.internal:8000` from within the Docker container).
*   **Model**: The `SW_WaveNet` model is located at `spark-app/model/sw_wavenet_traced_cpu.pt`.

## Future Enhancements (Roadmap)
Based on the project's phase breakdown, the following are planned future enhancements:
*   **Persistent Storage**: Implement database persistence for predictions and an audit log for alerts.
*   **Enhanced Notifications**: Add support for retry mechanisms, dead-letter queues for failed emails, and alternative notification channels (e.g., Slack, webhooks, SMS).
*   **Simulation & Testing**: Develop comprehensive testing for alert storm scenarios, intermittent anomalies, cooldown mechanisms, and notification service resilience.
*   **Scaling & Autoscaling**: Strategies for scaling notification services, inference workloads (potentially with GPU support), and other components based on throughput.
*   **Drift Monitoring & Retraining**: Integrate capabilities for monitoring model drift, triggering retraining pipelines, and managing model versions.
*   **Production Hardening**: Focus on robust error handling, security, and resilience against various failure modes in a production environment.
