"""
Synthetic Web Server Log Generator for PySpark Structured Streaming.

Simulates real-time web server access logs and writes them as micro-batch JSON files
into the streaming input directory. Supports dynamic anomaly injection (error spikes,
traffic surges, and vulnerability scans) to test rule-based alerting.
"""
import os
import time
import json
import random
import argparse
from datetime import datetime, timezone
import config

# Simulated endpoint catalog
ENDPOINTS = [
    ("/", 0.25),
    ("/login", 0.15),
    ("/api/products", 0.20),
    ("/checkout", 0.10),
    ("/api/cart", 0.12),
    ("/api/user/profile", 0.08),
    ("/static/css/main.css", 0.05),
    ("/static/js/bundle.js", 0.05),
]

# Vulnerability scan target endpoints for 404 scan mode
SCAN_ENDPOINTS = [
    "/admin.php",
    "/wp-login.php",
    "/.env",
    "/api/v1/debug",
    "/phpmyadmin",
    "/config.json",
    "/backup.zip",
    "/.git/config"
]

HTTP_METHODS = ["GET", "POST", "PUT", "DELETE"]

IP_POOL = [
    f"192.168.1.{i}" for i in range(10, 50)
] + [
    f"10.0.0.{i}" for i in range(5, 30)
] + [
    f"172.16.0.{i}" for i in range(1, 20)
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
    "Googlebot/2.1 (+http://www.google.com/bot.html)",
]


def weighted_choice(choices_with_weights):
    items, weights = zip(*choices_with_weights)
    return random.choices(items, weights=weights, k=1)[0]


def generate_single_log(mode="normal"):
    """
    Generate a single synthetic web server log entry based on the active simulation mode.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    if mode == "normal":
        endpoint = weighted_choice(ENDPOINTS)
        status_code = random.choices(
            [200, 201, 304, 400, 401, 404, 500],
            weights=[70, 15, 8, 3, 2, 1.5, 0.5],
            k=1
        )[0]
        response_time = int(random.gauss(120, 40))
        response_time = max(15, min(response_time, 1500))

    elif mode == "error-spike":
        # Simulates backend microservice database / gateway failure
        endpoint = random.choice(["/checkout", "/api/products", "/api/cart"])
        status_code = random.choices([500, 503, 504, 200], weights=[50, 25, 10, 15], k=1)[0]
        response_time = int(random.gauss(1800, 300))  # High latency on server failures
        response_time = max(500, response_time)

    elif mode == "traffic-spike":
        # Simulates sudden viral rush or DDoS flash crowd
        endpoint = random.choice(["/", "/login", "/api/products", "/checkout"])
        status_code = random.choices([200, 201, 429, 503], weights=[75, 10, 10, 5], k=1)[0]
        response_time = int(random.gauss(350, 120))
        response_time = max(50, response_time)

    elif mode == "404-scan":
        # Simulates malicious vulnerability scan
        endpoint = random.choice(SCAN_ENDPOINTS)
        status_code = random.choices([404, 403, 401], weights=[85, 10, 5], k=1)[0]
        response_time = random.randint(20, 90)

    else:
        endpoint = "/"
        status_code = 200
        response_time = 100

    # HTTP method heuristics
    if endpoint == "/login" or endpoint == "/checkout":
        http_method = random.choice(["POST", "GET"])
    elif endpoint == "/api/cart":
        http_method = random.choice(["POST", "PUT", "DELETE", "GET"])
    else:
        http_method = "GET"

    bytes_sent = random.randint(200, 15000) if status_code < 400 else random.randint(50, 300)

    return {
        "timestamp": now,
        "ip_address": random.choice(IP_POOL),
        "http_method": http_method,
        "endpoint": endpoint,
        "status_code": status_code,
        "response_time_ms": response_time,
        "bytes_sent": bytes_sent,
        "user_agent": random.choice(USER_AGENTS)
    }


def write_log_batch(batch_num, mode="normal"):
    """
    Generate a batch of logs and write them atomically to a JSON file in the input directory.
    Uses .tmp suffix then atomic rename so PySpark doesn't read a partially written file.
    """
    if mode == "traffic-spike":
        count = random.randint(*config.GENERATOR_CONFIG["LOGS_PER_BATCH_SPIKE"])
    else:
        count = random.randint(*config.GENERATOR_CONFIG["LOGS_PER_BATCH_NORMAL"])

    logs = [generate_single_log(mode) for _ in range(count)]

    filename = f"logs_{int(time.time())}_{batch_num}.json"
    temp_filepath = os.path.join(config.INPUT_LOGS_DIR, f"{filename}.tmp")
    final_filepath = os.path.join(config.INPUT_LOGS_DIR, filename)

    # Write newline-delimited JSON (JSON Lines format)
    with open(temp_filepath, "w", encoding="utf-8") as f:
        for log_entry in logs:
            f.write(json.dumps(log_entry) + "\n")

    # Atomic rename
    os.replace(temp_filepath, final_filepath)
    return count, final_filepath


def main():
    parser = argparse.ArgumentParser(description="Synthetic Web Log Generator for PySpark Streaming")
    parser.add_argument(
        "--mode",
        choices=["normal", "error-spike", "traffic-spike", "404-scan"],
        default="normal",
        help="Simulation mode: normal traffic, error-spike (500s), traffic-spike (high load), or 404-scan (vulnerability scanning)"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=config.GENERATOR_CONFIG["BATCH_INTERVAL_SEC"],
        help="Seconds between log batch writes (default: 2.0)"
    )
    parser.add_argument(
        "--batches",
        type=int,
        default=-1,
        help="Number of batches to write (-1 for infinite stream)"
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clean previous log files in input_logs directory before starting"
    )

    args = parser.parse_args()

    if args.clean:
        print(f"[*] Cleaning existing logs from {config.INPUT_LOGS_DIR}...")
        for fname in os.listdir(config.INPUT_LOGS_DIR):
            fpath = os.path.join(config.INPUT_LOGS_DIR, fname)
            if os.path.isfile(fpath):
                os.remove(fpath)
        print("[+] Directory cleaned.")

    print("=" * 65)
    print("  PYSPARK REAL-TIME LOG ANALYTICS - SYNTHETIC DATA GENERATOR")
    print("=" * 65)
    print(f"Target Directory : {config.INPUT_LOGS_DIR}")
    print(f"Simulation Mode  : {args.mode.upper()}")
    print(f"Batch Interval   : {args.interval}s")
    print(f"Total Batches    : {'Infinite (Ctrl+C to stop)' if args.batches == -1 else args.batches}")
    print("-" * 65)

    batch_count = 0
    try:
        while args.batches == -1 or batch_count < args.batches:
            batch_count += 1
            num_records, filepath = write_log_batch(batch_count, mode=args.mode)
            timestamp_str = datetime.now().strftime("%H:%M:%S")
            print(f"[{timestamp_str}] Batch #{batch_count:04d} -> Generated {num_records:3d} logs in {os.path.basename(filepath)} (Mode: {args.mode})")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n[*] Generator stopped by user.")


if __name__ == "__main__":
    main()
