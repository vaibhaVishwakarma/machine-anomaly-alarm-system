# ==========================================
# stream_inference.py
# Spark Structured Streaming + Kafka
# ==========================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json
import base64

from model_definition import predict_from_bytes


# ==========================================
# Spark Session
# ==========================================

spark = SparkSession.builder \
    .appName("IndustrialAudioXGBoost") \
    .config("spark.sql.streaming.metricsEnabled", "false") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# ==========================================
# Read Kafka Stream
# ==========================================
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "audio-topic") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

json_df = raw_df.selectExpr(
    "CAST(value AS STRING) as json_string"
)


# ==========================================
# foreachBatch Processing
# ==========================================

def process_batch(batch_df, batch_id):

    if batch_df.count() == 0:
        return

    rows = batch_df.collect()

    results = []

    for row in rows:
        try:
            data = json.loads(row.json_string)

            audio_base64 = data["audio"]
            size = data.get("size", 0)

            audio_bytes = base64.b64decode(audio_base64)

            prob, classification = predict_from_bytes(audio_bytes)

            results.append({
                "anomaly_probability": prob,
                "classification": classification,
                "size_kb": float(size / 1024.0)
            })

        except Exception as e:
            results.append({
                "anomaly_probability": 0.0,
                "classification": "error",
                "size_kb": 0.0
            })

    if results:
        output_df = spark.createDataFrame(results)

        output_df.selectExpr(
            "to_json(struct(*)) AS value"
        ).write \
         .format("kafka") \
         .option("kafka.bootstrap.servers", "kafka:29092") \
         .option("topic", "prediction-topic") \
         .save()


# ==========================================
# Start Streaming Query
# ==========================================

query = json_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()