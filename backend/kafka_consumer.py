import json
from kafka import KafkaConsumer
import threading

def start_consumer_thread(predictions_store, machine_alarm_states):

    def consume():
        consumer = KafkaConsumer(
            "prediction_topic",
            "alert_event_topic",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="latest",
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )

        for message in consumer:
            topic = message.topic
            data = message.value

            if topic == "prediction_topic":
                predictions_store.append(data)

                # keep store size limited
                if len(predictions_store) > 500:
                    predictions_store.pop(0)

            elif topic == "alert_event_topic":

                machine_id = str(data["machine_id"])
                alarm_state = data.get("alarm_state")

                if alarm_state == "ALARM_TRIGGERED":
                    machine_alarm_states[machine_id] = "ALARM"

                elif alarm_state == "RECOVERY":
                    machine_alarm_states[machine_id] = "NORMAL"

    thread = threading.Thread(target=consume, daemon=True)
    thread.start()