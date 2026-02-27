import base64
import io
import json
import time
from collections import defaultdict

import torch
import torchaudio
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType

# ============================
# CONFIG
# ============================

EXPECTED_SR = 16000
WINDOW_CHUNKS = 10     # 10 seconds
STEP_CHUNKS = 5        # inference every 5 seconds
THRESHOLD = 1.192093e-07 + 75e-5

MODEL_PATH = "/spark-app/model/sw_wavenet_traced_cpu.pt"

# ============================
# LOAD MODEL
# ============================

device = torch.device("cpu")
model = torch.jit.load(MODEL_PATH, map_location=device)
model.eval()

print("Model loaded successfully")

# ============================
# BUFFER STORE
# ============================

buffer_store = defaultdict(list)

# ============================
# INFERENCE FUNCTION
# ============================

def run_inference(machine_id):

    buffer = buffer_store[machine_id]
    buffer_length = len(buffer)

    if buffer_length < WINDOW_CHUNKS:
        return None

    # Run only at step intervals
    if (buffer_length - WINDOW_CHUNKS) % STEP_CHUNKS != 0:
        return None

    # Take last 10 chunks
    window_chunks = buffer[-WINDOW_CHUNKS:]

    full_waveform = torch.cat(window_chunks, dim=1)

    if full_waveform.dim() == 2:
        full_waveform = full_waveform.unsqueeze(0)

    print("Inference input shape:", full_waveform.shape)

    try:
        with torch.no_grad():
            logits = model(full_waveform)
            probs = torch.softmax(logits, dim=1)

        machine_index = int(machine_id)

        if machine_index >= probs.shape[1]:
            print("Invalid machine id")
            return None

        prob = probs[0, machine_index]
        score = -torch.log(prob + 1e-9).item()

        prediction = "ANOMALY" if score >= THRESHOLD else "NORMAL"

        return score, prediction

    except Exception as e:
        print("Inference error:", e)
        return None


# ============================
# SPARK STREAM
# ============================

spark = SparkSession.builder \
    .appName("AudioStreamInference") \
    .config("spark.sql.streaming.metricsEnabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("machine_id", IntegerType()),
    StructField("source_id", StringType()),
    StructField("timestamp", LongType()),
    StructField("audio", StringType()),
    StructField("size_kb", DoubleType())
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "raw_audio_topic") \
    .load()

json_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


# ============================
# PROCESS BATCH
# ============================

def process_batch(batch_df, batch_id):

    print(f"\nProcessing batch {batch_id} | Rows: {batch_df.count()}")

    rows = batch_df.collect()

    for row in rows:

        machine_id = row.machine_id
        audio_b64 = row.audio

        try:
            audio_bytes = base64.b64decode(audio_b64)
            waveform, sr = torchaudio.load(io.BytesIO(audio_bytes))

            if sr != EXPECTED_SR:
                waveform = torchaudio.functional.resample(waveform, sr, EXPECTED_SR)

            buffer_store[machine_id].append(waveform)

            print(f"Machine {machine_id} buffer size: {len(buffer_store[machine_id])}")

            result = run_inference(machine_id)

            if result is None:
                continue

            score, prediction = result

            output = {
                "timestamp": int(time.time()),
                "machine_id": machine_id,
                "prediction": prediction,
                "anomaly_score": score,
                "size_kb": row.size_kb
            }

            print("Publishing prediction:", output)

            spark.createDataFrame(
                [(json.dumps(output),)],
                ["value"]
            ).write \
             .format("kafka") \
             .option("kafka.bootstrap.servers", "kafka:29092") \
             .option("topic", "prediction_topic") \
             .save()

        except Exception as e:
            print("Error processing row:", e)


# ============================
# START STREAM
# ============================

query = json_df.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime="5 seconds") \
    .option("checkpointLocation", "/tmp/checkpoint_audio_v3") \
    .start()

query.awaitTermination()