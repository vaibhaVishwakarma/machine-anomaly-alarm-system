import json
import re
import requests
from kafka import KafkaConsumer
from config import KAFKA_BOOTSTRAP, ALERT_TOPIC, COOLDOWN_SECONDS, BACKEND_URL
from notifier import send_email
from state_manager import AlertStateManager

EMAIL_REGEX = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"

state_manager = AlertStateManager(COOLDOWN_SECONDS)

consumer = KafkaConsumer(
    ALERT_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

print("Notification Service Started...")


def get_recipients():
    try:
        response = requests.get(f"{BACKEND_URL}/alert_email", timeout=5)
        data = response.json()
        raw_string = data.get("email", "")
        recipients = re.findall(EMAIL_REGEX, raw_string)
        return list(set(recipients))  # remove duplicates
    except Exception as e:
        print("Failed to fetch recipients:", e)
        return []


for message in consumer:

    alert = message.value
    machine_id = alert["machine_id"]

    print("Received alert:", alert)

    if alert.get("alarm_state") != "ALARM_TRIGGERED":
        continue

    if state_manager.should_send(machine_id):

        recipients = get_recipients()

        if not recipients:
            print("No valid recipients found.")
            continue

        print("Sending notification to:", recipients)

        try:
            send_email(alert, recipients)
            print("Email sent successfully.")
        except Exception as e:
            print("Email failed:", e)

    else:
        print("Cooldown active. Alert suppressed.")