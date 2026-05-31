# 🐍 Python Kafka Client (Confluent-Kafka)

This client implementation uses the official **`confluent-kafka`** library, which wraps the highly optimized, C-based client **`librdkafka`** for maximum network performance and throughput.

---

## 🏗️ Highlights Included

*   **Idempotence Setup:** The producer configuration establishes `enable.idempotence: True` and `acks: all` to safeguard against in-transit network duplicates and leader failures.
*   **Safe Offsets:** Auto commits are disabled (`enable.auto.commit: False`). The consumer commits read indexes manually (`consumer.commit(asynchronous=False)`) only *after* parsing the record without exception, upholding **At-Least-Once** semantics.
*   **Graceful Termination:** Binds OS termination signals (`SIGINT`, `SIGTERM`) to clean up client connections and exit, notifying the broker coordination group immediately to avoid long partitions rebalance delays.

---

## 🚀 How to Run

### 1. Setup Environment
Ensure you have a Python virtual environment running:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Run the Consumer
In a terminal pane:
```bash
python consumer.py
```

### 3. Run the Producer
In a separate terminal pane:
```bash
python producer.py
```

You will see messages created with success status, partition indexes, and read values printed on the console!
