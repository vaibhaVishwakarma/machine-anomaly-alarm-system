import json
from kafka import KafkaConsumer
from config import KAFKA_BOOTSTRAP, ALERT_TOPIC, COOLDOWN_SECONDS
from notifier import send_email
from state_manager import AlertStateManager

state_manager = AlertStateManager(COOLDOWN_SECONDS)

consumer = KafkaConsumer(
    ALERT_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

print("Notification Service Started...")

for message in consumer:

    alert = message.value
    machine_id = alert["machine_id"]

    print("Received alert:", alert)

    if state_manager.should_send(machine_id):

        print("Sending notification...")
        try:
            send_email(alert)
            print("Email sent successfully.")
        except Exception as e:
            print("Email failed:", e)

    else:
        print("Cooldown active. Alert suppressed.")