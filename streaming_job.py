"""
PySpark Structured Streaming Pipeline for Real-Time Log Analytics.

Ingests simulated web server logs from the input directory, applies watermarking,
computes micro-batch rolling metrics (traffic volume, error rates, endpoint latency),
runs rule-based anomaly detection, and writes live metrics to both console and JSON sinks.
"""
import os
import sys
import json
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, when, count, avg, max as spark_max, sum as spark_sum,
    round as spark_round, desc
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)
import config


def create_spark_session():
    """
    Initializes a local SparkSession optimized for streaming micro-batches.
    """
    return SparkSession.builder \
        .appName("PySpark-RealTime-LogAnalytics") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()


def define_log_schema():
    """
    Explicit schema for the incoming web server JSON logs.
    """
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("http_method", StringType(), True),
        StructField("endpoint", StringType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("response_time_ms", IntegerType(), True),
        StructField("bytes_sent", IntegerType(), True),
        StructField("user_agent", StringType(), True),
    ])


def evaluate_anomalies(batch_stats):
    """
    Rule-Based Anomaly and Alert Detection Engine.
    Evaluates streaming batch statistics against predefined thresholds.
    """
    alerts = []
    total_reqs = batch_stats["total_requests"]
    error_rate = batch_stats["error_rate_pct"]
    count_5xx = batch_stats["status_5xx"]
    count_404 = batch_stats["status_404"]

    # Rule 1: High Overall Error Rate (> 15%)
    if error_rate >= config.ANOMALY_THRESHOLDS["HIGH_ERROR_RATE_PERCENT"]:
        severity = "CRITICAL" if error_rate >= 30.0 else "WARNING"
        alerts.append({
            "type": "HIGH_ERROR_RATE",
            "severity": severity,
            "metric": f"{error_rate:.1f}%",
            "threshold": f"{config.ANOMALY_THRESHOLDS['HIGH_ERROR_RATE_PERCENT']}%",
            "message": f"Elevated error rate detected across recent requests ({error_rate:.1f}% errors)."
        })

    # Rule 2: Traffic Spike Surge
    if total_reqs >= config.ANOMALY_THRESHOLDS["TRAFFIC_SPIKE_COUNT"]:
        alerts.append({
            "type": "TRAFFIC_SPIKE",
            "severity": "WARNING",
            "metric": f"{total_reqs} req/batch",
            "threshold": f"{config.ANOMALY_THRESHOLDS['TRAFFIC_SPIKE_COUNT']}",
            "message": f"Sudden influx of traffic detected ({total_reqs} requests in single micro-batch)."
        })

    # Rule 3: Severe Backend Server Failures (5xx errors)
    if count_5xx >= config.ANOMALY_THRESHOLDS["SERVER_FAILURE_5XX_COUNT"]:
        alerts.append({
            "type": "SERVER_OUTAGE_5XX",
            "severity": "CRITICAL",
            "metric": f"{count_5xx} failures",
            "threshold": f"{config.ANOMALY_THRESHOLDS['SERVER_FAILURE_5XX_COUNT']}",
            "message": f"Backend microservices returning 5xx internal server errors ({count_5xx} occurrences)."
        })

    # Rule 4: Potential Vulnerability Scan or Brute Force (404 spikes)
    if count_404 >= config.ANOMALY_THRESHOLDS["BRUTE_FORCE_404_COUNT"]:
        alerts.append({
            "type": "VULNERABILITY_SCAN_404",
            "severity": "WARNING",
            "metric": f"{count_404} not-found errors",
            "threshold": f"{config.ANOMALY_THRESHOLDS['BRUTE_FORCE_404_COUNT']}",
            "message": f"High volume of 404 responses ({count_404}) indicating potential route scanning."
        })

    return alerts


def process_micro_batch(df, batch_id):
    """
    Micro-batch processing handler called by foreachBatch sink.
    Computes aggregations, detects anomalies, prints to console, and saves JSON metrics.
    """
    total_count = df.count()
    if total_count == 0:
        return

    timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Aggregate Overall Traffic & Latency Metrics
    traffic_summary = df.agg(
        count("*").alias("total_requests"),
        spark_sum(when(col("status_code").between(200, 299), 1).otherwise(0)).alias("status_2xx"),
        spark_sum(when(col("status_code").between(300, 399), 1).otherwise(0)).alias("status_3xx"),
        spark_sum(when(col("status_code").between(400, 499), 1).otherwise(0)).alias("status_4xx"),
        spark_sum(when(col("status_code").between(500, 599), 1).otherwise(0)).alias("status_5xx"),
        spark_sum(when(col("status_code") == 404, 1).otherwise(0)).alias("status_404"),
        spark_round(avg("response_time_ms"), 2).alias("avg_response_time_ms"),
        spark_max("response_time_ms").alias("max_response_time_ms"),
        spark_sum("bytes_sent").alias("total_bytes_sent")
    ).collect()[0]

    status_2xx = int(traffic_summary["status_2xx"] or 0)
    status_3xx = int(traffic_summary["status_3xx"] or 0)
    status_4xx = int(traffic_summary["status_4xx"] or 0)
    status_5xx = int(traffic_summary["status_5xx"] or 0)
    status_404 = int(traffic_summary["status_404"] or 0)
    avg_latency = float(traffic_summary["avg_response_time_ms"] or 0.0)
    max_latency = int(traffic_summary["max_response_time_ms"] or 0)
    total_bytes = int(traffic_summary["total_bytes_sent"] or 0)

    total_errors = status_4xx + status_5xx
    error_rate_pct = round((total_errors / total_count) * 100, 2) if total_count > 0 else 0.0

    # Aggregate Top 5 Requested Endpoints
    top_endpoints_df = df.groupBy("endpoint") \
        .agg(
            count("*").alias("hits"),
            spark_round(avg("response_time_ms"), 1).alias("avg_latency_ms")
        ) \
        .orderBy(desc("hits")) \
        .limit(5)

    top_endpoints = [
        {"endpoint": row["endpoint"], "hits": row["hits"], "avg_latency_ms": row["avg_latency_ms"]}
        for row in top_endpoints_df.collect()
    ]

    # Aggregate Recent Raw Samples for Live Feed
    recent_samples = [
        {
            "timestamp": row["timestamp"],
            "ip_address": row["ip_address"],
            "http_method": row["http_method"],
            "endpoint": row["endpoint"],
            "status_code": row["status_code"],
            "response_time_ms": row["response_time_ms"]
        }
        for row in df.limit(6).collect()
    ]

    # Package current batch stats
    batch_stats = {
        "batch_id": batch_id,
        "timestamp": timestamp_str,
        "total_requests": total_count,
        "status_2xx": status_2xx,
        "status_3xx": status_3xx,
        "status_4xx": status_4xx,
        "status_5xx": status_5xx,
        "status_404": status_404,
        "error_rate_pct": error_rate_pct,
        "avg_response_time_ms": avg_latency,
        "max_response_time_ms": max_latency,
        "total_bytes_sent": total_bytes,
        "top_endpoints": top_endpoints,
        "recent_logs": recent_samples
    }

    # Evaluate Anomaly Rules
    alerts = evaluate_anomalies(batch_stats)
    batch_stats["alerts"] = alerts
    batch_stats["status"] = "CRITICAL" if any(a["severity"] == "CRITICAL" for a in alerts) else (
        "WARNING" if len(alerts) > 0 else "HEALTHY"
    )

    # 1. Print Summary to Console
    print("\n" + "=" * 70)
    print(f" [SPARK STREAMING] BATCH #{batch_id:04d} | {timestamp_str}")
    print("=" * 70)
    print(f" Requests: {total_count:4d} | Avg Latency: {avg_latency:6.1f}ms | Max: {max_latency:4d}ms")
    print(f" Status  : 2xx={status_2xx} | 3xx={status_3xx} | 4xx={status_4xx} | 5xx={status_5xx}")
    print(f" Health  : Error Rate = {error_rate_pct:5.2f}% [{'ALERT: ' + batch_stats['status'] if alerts else 'NORMAL'}]")

    if alerts:
        print("-" * 70)
        print(" >> ACTIVE ALERTS TRIGGERED <<")
        for alert in alerts:
            print(f"    [{alert['severity']}] {alert['type']}: {alert['message']} (Val: {alert['metric']}, Limit: {alert['threshold']})")

    print("-" * 70)
    print(" Top Endpoints:")
    for ep in top_endpoints[:3]:
        print(f"    {ep['endpoint']:<22} -> Hits: {ep['hits']:3d} | Avg Latency: {ep['avg_latency_ms']:5.1f}ms")
    print("=" * 70)

    # 2. Write Output Metrics JSON for Dashboard
    latest_metrics_path = os.path.join(config.OUTPUT_METRICS_DIR, "latest_metrics.json")
    history_metrics_path = os.path.join(config.OUTPUT_METRICS_DIR, "metrics_history.jsonl")

    # Atomic write for latest_metrics.json
    temp_path = latest_metrics_path + ".tmp"
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(batch_stats, f, indent=2)
    os.replace(temp_path, latest_metrics_path)

    # Append to history log
    with open(history_metrics_path, "a", encoding="utf-8") as f:
        f.write(json.dumps({
            "batch_id": batch_id,
            "timestamp": timestamp_str,
            "total_requests": total_count,
            "error_rate_pct": error_rate_pct,
            "avg_response_time_ms": avg_latency,
            "status_2xx": status_2xx,
            "status_4xx": status_4xx,
            "status_5xx": status_5xx,
            "alerts_count": len(alerts),
            "status": batch_stats["status"]
        }) + "\n")


def main():
    print("=" * 70)
    print("  PYSPARK STRUCTURED STREAMING - REAL-TIME LOG ANALYTICS PIPELINE")
    print("=" * 70)
    print(f"Monitoring Input Directory : {config.INPUT_LOGS_DIR}")
    print(f"Output Metrics Directory   : {config.OUTPUT_METRICS_DIR}")
    print(f"Checkpoint Directory       : {config.CHECKPOINT_DIR}")
    print(f"Trigger Interval           : {config.TRIGGER_PROCESSING_TIME}")
    print(f"Watermark Window           : {config.WATERMARK_DURATION}")
    print("=" * 70)

    spark = create_spark_session()
    # Suppress verbose INFO logs for clean console viewing
    spark.sparkContext.setLogLevel("WARN")

    log_schema = define_log_schema()

    # Read streaming data from input JSON logs directory
    raw_stream = spark.readStream \
        .schema(log_schema) \
        .option("maxFilesPerTrigger", 5) \
        .json(config.INPUT_LOGS_DIR)

    # Add Event Timestamp and Watermark
    parsed_stream = raw_stream \
        .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withWatermark("event_time", config.WATERMARK_DURATION)

    # Write Stream using foreachBatch micro-batch sink
    query = parsed_stream.writeStream \
        .trigger(processingTime=config.TRIGGER_PROCESSING_TIME) \
        .option("checkpointLocation", config.CHECKPOINT_DIR) \
        .foreachBatch(process_micro_batch) \
        .start()

    print("[+] PySpark Streaming query initialized successfully. Waiting for log files...")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[*] Stopping PySpark streaming pipeline gracefully...")
        query.stop()
        spark.stop()
        print("[+] PySpark pipeline stopped.")


if __name__ == "__main__":
    main()
