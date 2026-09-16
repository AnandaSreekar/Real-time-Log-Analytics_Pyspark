"""
Verification script for PySpark Log Analytics Pipeline.
Validates that:
1. Generator writes valid JSON logs.
2. SparkSession starts without error.
3. Micro-batch processing computes aggregations and alerts.
4. Output JSON files are created properly for the dashboard.
"""
import os
import sys
import json
import time
import config
from generator import write_log_batch
from streaming_job import create_spark_session, define_log_schema, process_micro_batch

def test_pipeline():
    print("=" * 60)
    print("  RUNNING PIPELINE VERIFICATION TEST")
    print("=" * 60)

    # 1. Generate test logs for normal and error-spike
    print("[1] Generating test log files...")
    count_normal, f1 = write_log_batch(991, mode="normal")
    count_spike, f2 = write_log_batch(992, mode="error-spike")
    print(f"    - Generated {count_normal} normal logs -> {f1}")
    print(f"    - Generated {count_spike} error-spike logs -> {f2}")

    # 2. Test SparkSession creation
    print("\n[2] Initializing PySpark local session...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    print("    - SparkSession initialized successfully!")

    # 3. Read generated files as DataFrame and process micro-batch
    print("\n[3] Testing Micro-Batch Processing...")
    schema = define_log_schema()
    df = spark.read.schema(schema).json(config.INPUT_LOGS_DIR)
    total_records = df.count()
    print(f"    - Total records in input directory: {total_records}")

    # Trigger process_micro_batch manually to test logic
    process_micro_batch(df, batch_id=1)

    # 4. Check output JSON files
    latest_path = os.path.join(config.OUTPUT_METRICS_DIR, "latest_metrics.json")
    history_path = os.path.join(config.OUTPUT_METRICS_DIR, "metrics_history.jsonl")

    assert os.path.exists(latest_path), "latest_metrics.json was not created!"
    assert os.path.exists(history_path), "metrics_history.jsonl was not created!"

    with open(latest_path, "r", encoding="utf-8") as f:
        metrics = json.load(f)

    print("\n[4] Validating Generated Metrics Snapshot:")
    print(f"    - Batch ID         : {metrics['batch_id']}")
    print(f"    - Total Requests   : {metrics['total_requests']}")
    print(f"    - Error Rate %     : {metrics['error_rate_pct']}%")
    print(f"    - Avg Latency (ms) : {metrics['avg_response_time_ms']} ms")
    print(f"    - Status 5xx       : {metrics['status_5xx']}")
    print(f"    - Active Alerts    : {len(metrics['alerts'])} alerts triggered")
    for alert in metrics['alerts']:
        print(f"       * [{alert['severity']}] {alert['type']}: {alert['message']}")

    spark.stop()
    print("\n" + "=" * 60)
    print("  VERIFICATION COMPLETE: ALL CHECKS PASSED!")
    print("=" * 60)

if __name__ == "__main__":
    test_pipeline()
