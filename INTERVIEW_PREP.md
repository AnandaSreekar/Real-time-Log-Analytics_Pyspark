# PySpark Structured Streaming: Technical Interview & Viva Guide

A comprehensive cheat-sheet and technical question bank designed to help you ace data engineering interviews and viva examinations based on this real-time log analytics project.

---

## 🎯 1. Project Elevator Pitch (30-Second Summary)

> *"I designed and implemented an end-to-end real-time log analytics and anomaly detection system using PySpark Structured Streaming. The pipeline continuously ingests high-throughput web server logs using a file-based stream with schema validation, applies event-time watermarking to handle out-of-order data, and performs sliding-window aggregations across traffic volume, endpoint latency, and HTTP status codes. It features a rule-based anomaly detection engine that flags operational hazards like 5xx server outages, error surges, and brute-force scans in sub-second micro-batches, surfacing live health metrics and alerts onto a responsive glassmorphic web dashboard."*

---

## 🧠 2. Core Concepts Deep Dive

### 1. Structured Streaming vs Legacy Spark Streaming (DStreams)
| Feature | Spark Streaming (DStreams) | Structured Streaming |
|---|---|---|
| **Underlying Abstraction** | RDD (Resilient Distributed Datasets) | DataFrames & Datasets |
| **Engine** | Separate streaming engine | Built on top of Spark SQL engine & Catalyst Optimizer |
| **Time Semantics** | Processing time only (when data arrived at Spark) | Event time (timestamp embedded inside the event data) |
| **Watermarking Support** | Manual & complex | Native via `.withWatermark()` |
| **API Uniformity** | Different APIs for batch vs stream | Identical DataFrame API for both batch and streaming |

### 2. Event Time vs Processing Time
- **Event Time**: The exact moment when the event occurred on the client/server (e.g., the timestamp field inside the HTTP request log).
- **Processing Time**: The wall-clock time on the Spark cluster executor when the record is being processed.
- **Why Event Time Matters**: Network lag, mobile retries, and system restarts cause logs to arrive out-of-order. Analyzing metrics using event time guarantees business accuracy regardless of ingestion delays.

### 3. Watermarking: Handling Late-Arriving Data
- **What is a Watermark?** A threshold that moves behind the maximum event time seen so far:  
  $$\text{Watermark} = \max(\text{Event Time}) - \text{Allowed Latency}$$
- **How it Works**: If allowed latency is `1 minute`, and Spark sees an event with timestamp `12:05:00`, the watermark moves to `12:04:00`. Any subsequent event with timestamp prior to `12:04:00` is dropped.
- **State Store Eviction**: Without watermarks, stateful aggregations (like window counts) keep accumulating state in RAM indefinitely, eventually causing an `OutOfMemoryError` (OOM). Watermarks tell Spark when an aggregation window is finalized and can be evicted from memory.

### 4. Windowing Types
- **Tumbling Window**: Fixed, non-overlapping intervals (e.g., every 5 minutes: `[12:00-12:05]`, `[12:05-12:10]`).
- **Sliding Window**: Overlapping intervals defined by duration and slide interval (e.g., a 30-second window that evaluates every 10 seconds). Provides rolling moving averages and smoothed metrics.
- **Session Window**: Dynamic windows defined by periods of activity separated by gaps of inactivity (e.g., user browsing sessions).

### 5. Output Modes in Structured Streaming
Spark Structured Streaming supports three output modes:
1. **Append Mode**: Only newly finalized rows are output. For stateful aggregations, rows are emitted only after the watermark passes the window end.
2. **Complete Mode**: The entire updated aggregation table is written to the sink on every micro-batch. (Useful for small dashboards or top-N lists).
3. **Update Mode**: Only the rows that were modified or added in the latest micro-batch are emitted. Ideal for downstream key-value stores.

### 6. Fault Tolerance & Exactly-Once Semantics
- **Checkpointing**: Spark Structured Streaming maintains checkpoint directories containing:
  - **Offsets**: Write-Ahead Log (WAL) recording the exact range of data processed in each micro-batch.
  - **Commit Log**: Records which micro-batches successfully committed to the sink.
  - **State Store**: Serialized state (e.g., running window counts) saved to durable storage (HDFS/S3/local disk).
- **Recovery**: If a node or job crashes, restarting the query re-reads the latest uncommitted offset from the checkpoint and recomputes the micro-batch, guaranteeing **End-to-End Exactly-Once Processing** when paired with an idempotent sink.

---

## 🛠️ 3. Architecture & Engineering Design Decisions

### Q: Why did you use `foreachBatch` instead of standard file/console sinks?
**Answer**:
> *"Standard streaming sinks like `FileSink` or `ConsoleSink` are single-purpose and restrictive—e.g., you cannot easily update a JSON dashboard payload, write metrics history, and print formatted alerts simultaneously from one sink. `foreachBatch` allows us to apply arbitrary batch logic on each micro-batch DataFrame: we compute windowed statistics, run our rule-based anomaly engine, write a lightweight snapshot for the web dashboard, and append history logs concurrently."*

### Q: How did you prevent race conditions when Spark reads JSON files written by the generator?
**Answer**:
> *"In file-based streaming, if Spark reads a file while the generator is still writing it, Spark will parse half-written JSON, resulting in a MalformedLineException or silent corruption. We solved this by using **Atomic Writes**: the generator writes records to a `.tmp` file first, flushes and closes it, and then renames it to `.json`. In modern operating systems, file renaming within the same filesystem is an atomic filesystem metadata operation."*

### Q: Why set `spark.sql.shuffle.partitions = 2`?
**Answer**:
> *"By default, Apache Spark sets `spark.sql.shuffle.partitions` to 200. In a production cluster with hundreds of cores and petabytes of data, 200+ partitions enable parallelism. However, in local streaming micro-batches with small datasets, 200 partitions cause excessive task scheduling overhead and thread contention. Lowering it to 2 matches local CPU cores, cutting micro-batch execution latency from several seconds down to ~200 milliseconds."*

### Q: What was the Windows `NativeIO$Windows.access0` UnsatisfiedLinkError, and how did you resolve it?
**Answer**:
> *"Hadoop was built primarily for POSIX environments. When running PySpark on Windows, Hadoop relies on native C++ helper libraries (`winutils.exe` and `hadoop.dll`) to check Windows filesystem permissions via Win32 APIs. Without these binaries, JVM's JNI loader fails with `UnsatisfiedLinkError: NativeIO$Windows.access0`. We resolved this by bundling compatible Hadoop Windows native binaries in a local `hadoop/bin` directory and programmatically setting `HADOOP_HOME` and system `PATH` in `config.py` upon initialization."*

---

## 💡 4. Top 10 Technical Interview Questions & Answers

#### Q1: What is a Micro-Batch in PySpark Structured Streaming?
**A:** PySpark Structured Streaming operates by default on a micro-batch model. It periodically queries the source for any new data that arrived since the last offset, plans an optimized batch execution using Catalyst, processes the records as a transient DataFrame, commits the output to the sink, and checkpoints the progress. The cycle repeats based on the configured trigger interval.

#### Q2: Can PySpark Structured Streaming achieve sub-millisecond latency?
**A:** Micro-batch mode typically yields latencies between 100ms to 500ms. For true sub-millisecond latencies, Spark offers **Continuous Processing Mode** (available for select operators and sinks), which runs long-running tasks continuously rather than launching batches, trading some rich stateful SQL operators for ~1ms latency.

#### Q3: How would you scale this pipeline to handle 100,000 requests/sec in production?
**A:**
1. **Ingestion**: Replace local file streaming with a distributed message broker like **Apache Kafka** or **AWS Kinesis**, partitioned by user or server hash.
2. **Compute**: Deploy the PySpark pipeline on a distributed cluster managed by **Kubernetes**, **Databricks**, or **AWS EMR** with auto-scaling worker nodes.
3. **Partitioning**: Tune `spark.sql.shuffle.partitions` to match the number of Kafka topic partitions (e.g., 32 to 64 partitions).
4. **Sink**: Replace single-node JSON snapshots with a scalable distributed time-series database or Lakehouse storage such as **Delta Lake**, **Apache Iceberg**, or **ClickHouse**.
5. **Dashboard**: Connect Grafana or Apache Superset directly to ClickHouse or Delta Lake for enterprise-scale dashboarding.

#### Q4: What happens if an anomaly threshold is breached repeatedly?
**A:** In our current implementation, each micro-batch evaluates incoming traffic against static thresholds (e.g., 5xx errors $\ge 5$). In a high-frequency production system, we would incorporate an **alert de-duplication and cooldown window** (e.g., via Redis or state store) to suppress alert fatigue and trigger incident alerts (PagerDuty/Slack) only when anomalies persist for 3 consecutive evaluation windows.

#### Q5: What is the difference between Stateless and Stateful transformations?
**A:**
- **Stateless**: Operations where each record is evaluated in isolation without remembering previous records (e.g., `filter(status_code == 500)`, `select()`, `withColumn()`).
- **Stateful**: Operations that require retaining historical information across micro-batches (e.g., `groupBy(window("timestamp", "1 minute")).count()`, stream-stream joins). Stateful operations require watermarking and checkpointing to maintain and evict state.

#### Q6: How do you monitor PySpark Structured Streaming performance in production?
**A:** Using Spark's `StreamingQueryListener` or `query.lastProgress` and `query.status`. Key metrics to watch:
- `inputRowsPerSecond`: Ingestion rate.
- `processedRowsPerSecond`: Processing throughput.
- `batchDuration`: If batch duration exceeds trigger interval, the stream will experience lag/backpressure.
- `numInputRows`: Volume of data per micro-batch.

#### Q7: What are the risks of using `df.collect()` or `df.toPandas()` inside a streaming pipeline?
**A:** `collect()` and `toPandas()` pull all distributed partition data across executors back into the Driver node's JVM and Python process memory. If the data volume in a batch surges unexpectedly, this will immediately trigger a Driver Out-Of-Memory (`java.lang.OutOfMemoryError`) crash. In our design, aggregations (`count`, `avg`, `groupBy`) condense the data down to a handful of summarized metrics before any local serialization occurs.

#### Q8: What are Spark Triggers and what types exist?
**A:** Triggers define the timing of streaming computation:
1. `Trigger.Unspecified()`: Default behavior (runs next micro-batch as soon as previous completes).
2. `Trigger.ProcessingTime("5 seconds")`: Executes micro-batches at fixed time intervals.
3. `Trigger.Once()`: Executes a single micro-batch to process all available data and then shuts down (useful for cost-effective hourly batch-as-stream cron jobs).
4. `Trigger.AvailableNow()`: Similar to `Once`, but splits large backlogs into multiple micro-batches instead of one monolithic batch.
5. `Trigger.Continuous("1 second")`: Low-latency continuous processing mode.

#### Q9: How do you handle schema evolution in streaming pipelines?
**A:** Structured Streaming requires an explicit schema when reading file streams (`.schema(schema)`) to avoid expensive schema inference on every micro-batch. For streaming sources like Kafka or JSON with evolving schemas, we can either:
- Read raw JSON as strings and use `from_json()` with permissive schema mode (`PERMISSIVE`), routing malformed records to a `_corrupt_record` dead-letter queue.
- Use Schema Registries (e.g., Confluent Schema Registry with Avro/Protobuf) to enforce backward/forward compatibility.

#### Q10: How does PySpark handle memory management in long-running streaming jobs?
**A:**
- **Off-Heap & JVM Memory**: Garbage Collection (G1GC) tuning helps avoid stop-the-world pauses.
- **State Store**: Uses HDFS-backed state store or RocksDB state store provider (`spark.sql.streaming.stateStore.providerClass`) to spill large state to local disk rather than exhausting JVM heap space.
- **Checkpoints**: Periodic checkpoint cleanup removes old offset files to conserve disk space.

---

## 🏆 5. Quick Reference Vocabulary
- **Micro-Batch**: A discrete chunk of streaming data processed as an RDD/DataFrame.
- **Event-Time**: The true timestamp when the log event happened.
- **Watermark**: The boundary defining how late data can arrive before being discarded.
- **Tumbling Window**: Non-overlapping time bucket.
- **Sliding Window**: Moving time bucket with overlap.
- **Checkpointing**: Durable state & offset tracking for fault tolerance.
- **foreachBatch**: Custom sink method to execute arbitrary batch operations per micro-batch.
- **Idempotence**: An operation that produces the same result even when executed multiple times.
