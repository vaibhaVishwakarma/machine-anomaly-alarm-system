from base64 import b64decode
import io
import json
import torch
import torchaudio
import torch.nn.functional as F

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# ---------------------------
# Spark Session
# ---------------------------
spark = SparkSession.builder \
    .appName("PumpInference") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------
# Load Model
# ---------------------------
device = torch.device("cpu")
model = torch.jit.load("/spark-app/model/sw_wavenet_traced.pt", map_location=device)
model.eval()

EXPECTED_SAMPLES = 160000
THRESHOLD = 2.10

# ---------------------------
# Inference Function
# ---------------------------
def run_inference(audio_b64, machine_id):

    audio_bytes = b64decode(audio_b64)
    waveform, sr = torchaudio.load(io.BytesIO(audio_bytes))

    if waveform.shape[1] < EXPECTED_SAMPLES:
        pad = EXPECTED_SAMPLES - waveform.shape[1]
        waveform = F.pad(waveform, (0, pad))
    else:
        waveform = waveform[:, :EXPECTED_SAMPLES]

    waveform = waveform.unsqueeze(0).to(device)

    with torch.no_grad():
        logits = model(waveform)   # ← FIXED HERE
        probs = F.softmax(logits, dim=1)

        score = -torch.log(
            probs[0, int(machine_id)] + 1e-9
        ).item()

    prediction = "ANOMALY" if score >= THRESHOLD else "NORMAL"

    return float(score), prediction


# ---------------------------
# foreachBatch Processing
# ---------------------------
def process_batch(batch_df, batch_id):

    rows = batch_df.collect()
    results = []

    for row in rows:
        score, pred = run_inference(row.audio, row.machine_id)

        results.append({
            "machine_id": row.machine_id,
            "source_id": row.source_id,
            "timestamp": row.timestamp,
            "anomaly_score": score,
            "prediction": pred,
            "size_kb": row.size_kb
        })

    if results:
        out_df = spark.createDataFrame(results)

        out_df.selectExpr(
            "CAST(machine_id AS STRING) AS key",
            "to_json(struct(*)) AS value"
        ).write \
         .format("kafka") \
         .option("kafka.bootstrap.servers", "kafka:29092") \
         .option("topic", "inference_results_topic") \
         .save()


# ---------------------------
# Read from Kafka
# ---------------------------
input_schema = StructType([
    StructField("machine_id", IntegerType()),
    StructField("source_id", StringType()),
    StructField("timestamp", LongType()),
    StructField("audio", StringType()),
    StructField("size_kb", DoubleType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "raw_audio_topic") \
    .load()

json_df = df.select(
    from_json(col("value").cast("string"),
              input_schema).alias("data")
).select("data.*")

query = json_df.writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()