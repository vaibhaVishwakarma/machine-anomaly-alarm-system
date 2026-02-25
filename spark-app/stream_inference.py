from pyspark.sql import SparkSession
import json
import base64
import io
import torch
import torchaudio
import datetime

# ===============================
# Spark Session
# ===============================

spark = SparkSession.builder \
    .appName("AudioAnomalyDetection") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ===============================
# Read Kafka Stream
# ===============================

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "audio-topic") \
    .option("startingOffsets", "latest") \
    .load()

json_df = raw_df.selectExpr("CAST(value AS STRING) as json_string")

# ===============================
# Batch Inference Function
# ===============================

def process_batch(batch_df, batch_id):

    if batch_df.count() == 0:
        return

    from model_definition import SW_WaveNet_Pipeline

    device = torch.device("cpu")

    model = SW_WaveNet_Pipeline(num_classes=41)
    model.load_state_dict(
        torch.load("/spark-app/model/sw_wavenet.pth", map_location=device)
    )
    model.eval()

    rows = batch_df.collect()
    results = []

    for row in rows:
        try:
            data = json.loads(row.json_string)

            audio_bytes = base64.b64decode(data["audio"])
            size = data["size"]

            signal, sr = torchaudio.load(io.BytesIO(audio_bytes))

            if signal.shape[1] < 160000:
                pad = 160000 - signal.shape[1]
                signal = torch.nn.functional.pad(signal, (0, pad))
            else:
                signal = signal[:, :160000]

            signal = signal.unsqueeze(0)

            score = model.get_anomaly_score(signal, target_machine_id=0)
            classification = "anomalous" if score > 3.0 else "normal"

            results.append({
                "timestamp": datetime.datetime.utcnow().isoformat(),
                "anomaly_score": float(score),
                "classification": classification,
                "size_kb": float(size / 1024.0)
            })

        except:
            results.append({
                "timestamp": datetime.datetime.utcnow().isoformat(),
                "anomaly_score": 0.0,
                "classification": "error",
                "size_kb": 0.0
            })

    if results:
        spark.createDataFrame(results) \
            .selectExpr("to_json(struct(*)) AS value") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("topic", "prediction-topic") \
            .save()

# ===============================
# Start Streaming
# ===============================

query = json_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .start()

query.awaitTermination()