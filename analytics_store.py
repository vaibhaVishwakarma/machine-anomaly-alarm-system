# analytics_store.py

from collections import deque

# Keep last 1000 events
predictions = deque(maxlen=1000)

def add_prediction(data):
    predictions.append(data)

def get_recent(n=50):
    return list(predictions)[-n:]

def get_summary():
    total = len(predictions)
    anomalous = sum(1 for p in predictions if p["classification"] == "anomalous")
    normal = total - anomalous

    return {
        "total_events": total,
        "anomalous_count": anomalous,
        "normal_count": normal
    }