"""
Configuration module for the PySpark Real-Time Log Analytics Pipeline.
Defines directory paths, streaming parameters, and anomaly detection thresholds.
"""
import os

# Base Directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
INPUT_LOGS_DIR = os.path.join(DATA_DIR, "input_logs")
OUTPUT_METRICS_DIR = os.path.join(DATA_DIR, "output_metrics")
CHECKPOINT_DIR = os.path.join(DATA_DIR, "checkpoints")

# Ensure required directories exist
for directory in [DATA_DIR, INPUT_LOGS_DIR, OUTPUT_METRICS_DIR, CHECKPOINT_DIR]:
    os.makedirs(directory, exist_ok=True)

# Windows Hadoop configuration
HADOOP_DIR = os.path.join(BASE_DIR, "hadoop")
HADOOP_BIN = os.path.join(HADOOP_DIR, "bin")
if os.name == "nt" and os.path.exists(HADOOP_BIN):
    os.environ["HADOOP_HOME"] = HADOOP_DIR
    if HADOOP_BIN not in os.environ.get("PATH", ""):
        os.environ["PATH"] = HADOOP_BIN + os.pathsep + os.environ.get("PATH", "")

# Streaming Configuration
WATERMARK_DURATION = "1 minute"
WINDOW_DURATION = "30 seconds"
SLIDE_DURATION = "10 seconds"
TRIGGER_PROCESSING_TIME = "5 seconds"

# Anomaly Detection Rule-Based Thresholds
ANOMALY_THRESHOLDS = {
    "HIGH_ERROR_RATE_PERCENT": 15.0,  # Alert if (4xx + 5xx) / total > 15%
    "TRAFFIC_SPIKE_COUNT": 60,         # Alert if requests per window > 60
    "SERVER_FAILURE_5XX_COUNT": 5,     # Alert if 5xx errors per window >= 5
    "BRUTE_FORCE_404_COUNT": 15,       # Alert if 404 count per window >= 15
}

# Log Generator Settings
GENERATOR_CONFIG = {
    "BATCH_INTERVAL_SEC": 2.0,         # Write a new log file every 2 seconds
    "LOGS_PER_BATCH_NORMAL": (10, 25), # Random range of logs per batch
    "LOGS_PER_BATCH_SPIKE": (80, 140), # Range during traffic surge
}

# Web Dashboard Settings
DASHBOARD_HOST = "0.0.0.0"
DASHBOARD_PORT = 5000
