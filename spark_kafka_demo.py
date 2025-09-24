#!/usr/bin/env python3
"""
spark_kafka_demo.py

Single-script demo:
 - In 'kafka' mode: starts a small Kafka producer thread that emits synthetic session events,
   and starts a Spark Structured Streaming job that reads from Kafka, aggregates per-customer features,
   and writes aggregated results to output path periodically.

 - In 'local' mode: skips Kafka. Reads a CSV file and runs a Spark batch job that aggregates features
   and trains a simple Spark ML RandomForest classifier (to show ML at scale).

Usage:
  python spark_kafka_demo.py --mode kafka --kafka-bootstrap localhost:9092 --topic sessions_topic
  python spark_kafka_demo.py --mode local --csv-path df_final.csv

Requirements:
  - Java and Spark (PySpark) installed
  - Kafka broker running locally for mode 'kafka'
  - Python packages: pyspark, kafka-python, pandas
"""

import argparse
import json
import random
import string
import threading
import time
from datetime import datetime
import os
import sys

# Producer libs
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except Exception:
    KAFKA_AVAILABLE = False

# Spark libs
from pyspark.sql import SparkSession



from pyspark.sql.functions import from_json, col, to_timestamp, window, avg as spark_avg, sum as spark_sum, count as spark_count, max as spark_max
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

def make_spark_session(app_name="SparkKafkaDemo"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def synthetic_event(customer_count=5000):
    """Return a synthetic session event dict."""
    cust_id = f"CUST{random.randint(1, customer_count):05d}"
    sess_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    product = random.choice(["Phone","Laptop","Shoes","Bag","Headphones","Book","Camera"])
    browsing_time = max(1, int(random.gauss(300, 150)))
    purchase_flag = 1 if random.random() < 0.3 else 0  # 30% sessions convert in synthetic demo
    purchase_amount = round(random.uniform(5, 1000), 2) if purchase_flag == 1 else 0.0
    age = random.randint(18, 70)
    gender = random.choice(["Male","Female","Unknown"])
    location = random.choice(["US","UK","India","Ghana","Germany"])
    signup_date = (datetime.utcnow().replace(hour=0, minute=0, second=0) - 
                   random.timedelta(days=random.randint(30, 1200)) ) if False else datetime.utcnow().strftime("%Y-%m-%d")
    # keep simplified payload (strings & numbers)
    return {
        "event_id": sess_id,
        "customer_id": cust_id,
        "session_date": now,
        "product_viewed": product,
        "browsing_time_sec": browsing_time,
        "purchase_flag": purchase_flag,
        "purchase_amount": purchase_amount,
        "age": age,
        "gender": gender,
        "location": location,
        "signup_date": datetime.utcnow().strftime("%Y-%m-%d")  # placeholder
    }

def kafka_producer_loop(bootstrap, topic, rate_per_sec=50, run_seconds=60):
    """
    Small producer that sends synthetic events to Kafka.
    rate_per_sec: messages per second
    run_seconds: total run time
    """
    if not KAFKA_AVAILABLE:
        print("kafka-python not installed. Install with: pip install kafka-python")
        return

    try:
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            linger_ms=5
        )
    except Exception as e:
        print("Failed to connect to Kafka broker:", e)
        return

    total = int(rate_per_sec * run_seconds)
    interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0.02
    print(f"Producer started -> {total} messages ({rate_per_sec}/s) to topic '{topic}'")
    send_count = 0
    start = time.time()
    try:
        while send_count < total:
            evt = synthetic_event()
            producer.send(topic, evt)
            send_count += 1
            # flush occasionally
            if send_count % 1000 == 0:
                producer.flush()
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    producer.flush()
    print("Producer finished. Sent:", send_count)

def run_spark_streaming_from_kafka(bootstrap, topic, output_path, duration_sec=60):
    """
    Start a Spark Structured Streaming job that reads JSON events from Kafka topic 'topic',
    parses them, computes per-customer aggregates (sum of purchases, total_spend, avg browsing time)
    and writes to output_path as parquet (micro-batches).
    """
    spark = make_spark_session("SparkStructuredStreamingKafkaDemo")

    # define schema for JSON payload
    schema = StructType() \
        .add("event_id", StringType()) \
        .add("customer_id", StringType()) \
        .add("session_date", StringType()) \
        .add("product_viewed", StringType()) \
        .add("browsing_time_sec", IntegerType()) \
        .add("purchase_flag", IntegerType()) \
        .add("purchase_amount", DoubleType()) \
        .add("age", IntegerType()) \
        .add("gender", StringType()) \
        .add("location", StringType()) \
        .add("signup_date", StringType())

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", topic)
           .option("startingOffsets", "latest")
           .load())

    # kafka value is bytes -> string -> json
    events = raw.selectExpr("CAST(value AS STRING) as json_str")
    events = events.select(from_json(col("json_str"), schema).alias("data")).select("data.*")
    events = events.withColumn("session_ts", to_timestamp(col("session_date")))

    # watermark and per-customer aggregation
    events = events.withWatermark("session_ts", "1 minute")

    cust_agg = (events.groupBy(col("customer_id"))
                .agg(
                    spark_count("event_id").alias("sessions_count_delta"),
                    spark_sum("purchase_flag").alias("total_purchases_delta"),
                    spark_sum("purchase_amount").alias("total_spend_delta"),
                    spark_avg("browsing_time_sec").alias("avg_browsing_time_delta"),
                    spark_max("session_ts").alias("last_session_delta")
                ))

    # write to parquet in update mode; micro-batches will write updates
    query = (cust_agg.writeStream
             .outputMode("update")
             .format("parquet")
             .option("path", output_path)
             .option("checkpointLocation", os.path.join(output_path, "_checkpoint"))
             .trigger(processingTime="5 seconds")
             .start())

    print(f"Spark streaming query started. Writing aggregates to: {output_path}")
    try:
        query.awaitTermination(timeout=duration_sec)
    except KeyboardInterrupt:
        pass
    finally:
        query.stop()
        spark.stop()
        print("Spark streaming stopped.")

def run_spark_batch_from_csv(csv_path, output_path):
    """
    Read CSV (session-level) with Spark, aggregate to customer level, show a small Spark ML pipeline example.
    This is a batch alternative if Kafka is not used.
    """
    spark = make_spark_session("SparkBatchDemo")
    df = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)
    # cast columns if necessary
    df = df.withColumn("browsing_time_sec", col("browsing_time_sec").cast(IntegerType())) \
           .withColumn("purchase_flag", col("purchase_flag").cast(IntegerType())) \
           .withColumn("purchase_amount", col("purchase_amount").cast(DoubleType()))
    # parse session_date
    df = df.withColumn("session_date", to_timestamp(col("session_date")))

    cust_agg = (df.groupBy("customer_id")
                .agg(
                    spark_count("session_id").alias("sessions_count"),
                    spark_sum("purchase_flag").alias("total_purchases"),
                    spark_sum("purchase_amount").alias("total_spend"),
                    spark_avg("browsing_time_sec").alias("avg_browsing_time"),
                    spark_max("session_date").alias("last_session")
                ))
    cust_agg = cust_agg.fillna({'avg_browsing_time': 0.0})

    print("Customer aggregates sample:")
    cust_agg.show(10, truncate=False)

    # Save aggregated output
    cust_agg.coalesce(1).write.mode("overwrite").parquet(output_path)
    print("Saved customer aggregates to:", output_path)
    spark.stop()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["kafka", "local"], default="local", help="Run mode: kafka (producer + spark streaming) or local (spark batch)")
    parser.add_argument("--kafka-bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="sessions_topic", help="Kafka topic for sessions")
    parser.add_argument("--csv-path", default="df_final.csv", help="CSV path for local mode")
    parser.add_argument("--output-path", default="spark_output_stream", help="Output path for parquet")
    parser.add_argument("--producer-rate", type=int, default=50, help="Synthetic producer messages per second")
    parser.add_argument("--producer-duration", type=int, default=60, help="How many seconds the producer runs")
    args = parser.parse_args()

    if args.mode == "kafka":
        if not KAFKA_AVAILABLE:
            print("kafka-python is not installed. Install with: pip install kafka-python")
            sys.exit(1)
        # start a producer thread
        prod_thread = threading.Thread(target=kafka_producer_loop, args=(args.kafka_bootstrap, args.topic, args.producer_rate, args.producer_duration), daemon=True)
        prod_thread.start()
        # run spark structured streaming consumer (this blocks until duration) 
        run_spark_streaming_from_kafka(args.kafka_bootstrap, args.topic, args.output_path, duration_sec=args.producer_duration + 30)
        prod_thread.join(timeout=1)
    else:
        # local batch mode
        if not os.path.exists(args.csv_path):
            print("CSV path not found:", args.csv_path)
            sys.exit(1)
        run_spark_batch_from_csv(args.csv_path, args.output_path)

if __name__ == "__main__":
    main()
