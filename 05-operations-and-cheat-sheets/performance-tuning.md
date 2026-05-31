# ⚡ Apache Kafka Performance Tuning Guide

Apache Kafka is designed to be highly configurable. You can tune its behavior to prioritize different operational trade-offs depending on your business requirements. 

Every architectural design must choose between three key goals: **Throughput (Batching)**, **Latency (Speed)**, and **Durability (Resiliency)**.

---

## 📊 The Performance Tuning Matrix

| Parameter | High Throughput Profile | Ultra-Low Latency Profile | High Resiliency (Zero-Loss) |
| :--- | :--- | :--- | :--- |
| **Acks (Producer)** | `1` or `all` | `1` or `0` | **`all` (or `-1`)** |
| **Linger.ms (Producer)** | **`50` to `100`** | **`0`** | `0` to `5` |
| **Batch.size (Producer)** | **`64KB` to `256KB`** | `0` | `16KB` |
| **Compression (Producer)**| **`lz4` or `zstd`** | None | `lz4` |
| **Max In-Flight Requests** | `5` | `1` (strict ordering) | **`1` to `5` (with Idempotence)**|
| **Min In-Sync Replicas** | `1` | `1` | **`2` (with replication factor 3)**|
| **Replica Fetch Wait** | `500` ms | `0` | `100` ms |
| **Fetch Min Bytes (Consumer)**| **`1024` to `8192`** | **`1`** | `1` |

---

## 📤 Producer Tuning Details

### 1. Throughput & Batching Optimization
By default, the producer attempts to publish messages as fast as possible. If you want maximum throughput (handling millions of events with minimal CPU/Network load), you must enable **batching**:
*   **`linger.ms`:** Force the producer to wait up to N milliseconds before sending, allowing it to aggregate individual small records into a single large batch (e.g. `linger.ms=100`).
*   **`batch.size`:** Increase the memory allocation per batch to accommodate larger payloads (e.g. `batch.size=131072` / 128KB).
*   **`compression.type`:** Compress batches before sending. This saves massive amounts of network bandwith and disk usage. Use **`lz4`** for optimal performance, or **`zstd`** for maximum compression ratios under high CPU availability.

### 2. Latency Optimization (Real-Time Streams)
For streaming applications requiring sub-millisecond responses (e.g., algorithmic trading, fraud detection):
*   Set `linger.ms=0` and `batch.size=0`. The producer will immediately send every message onto the TCP socket.
*   Disable payload compression (`compression.type=none`) to save JVM serialization CPU cycles.

### 3. Durability Optimization (Financial Data)
For systems where losing a single message is unacceptable:
*   Set `acks=all` to enforce replica matching.
*   Enable idempotence (`enable.idempotence=true`) to block network duplicates.
*   Enforce a retries limit of infinite (`retries=2147483647`) and set `max.in.flight.requests.per.connection=5` (or `1` if ordering is critical).

---

## 📥 Consumer Tuning Details

### 1. Controlling Throughput vs. Latency
Similar to producers, consumers pull data in batches. You can modify how long the consumer waits before returning a poll call:
*   **`fetch.min.bytes`:** The minimum amount of data (in bytes) that the broker must collect in its log before returning the fetch call to the consumer (e.g. `fetch.min.bytes=1048576` / 1MB).
*   **`fetch.max.wait.ms`:** The maximum amount of time the broker will wait to satisfy the `fetch.min.bytes` threshold before returning anyway (e.g. `fetch.max.wait.ms=500`).
*   *For Low Latency:* Set `fetch.min.bytes=1` and `fetch.max.wait.ms=0` to fetch records instantly as they arrive.

### 2. Safeguarding Against Group Kickouts
*   **`max.poll.interval.ms`:** The maximum allowed delay between consumer `poll()` calls. If your business logic processes a large batch slowly and exceeds this time, the Group Coordinator marks the consumer dead and triggers a rebalance.
    > [!TIP]
    > If you process heavy operations (like complex database writes or API calls), increase `max.poll.interval.ms` or decrease the number of records returned in a single batch using `max.poll.records`.

---

## 🧱 Broker-Side Performance Configs

Tune these parameters in your `server.properties` files:
*   **`num.network.threads`:** The number of threads handling request/response socket operations on the network (Default: `3`). Increase for clusters with a high number of active clients.
*   **`num.io.threads`:** The number of threads handling disk read/write logs operations (Default: `8`). Highly recommended to match the CPU core count of the host machine.
*   **`num.recovery.threads.per.data.dir`:** The number of threads used to scan, clean, and recover log segments during startup. Increase this value to drastically speed up broker recovery times after unclean shut downs.
