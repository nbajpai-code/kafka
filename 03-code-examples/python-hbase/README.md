# 🐍 Python HBase (HappyBase) Kafka Sink Integration

This module demonstrates how to consume real-time streaming data from an Apache Kafka topic and synchronously persist it to **Apache HBase** (Wide-Column NoSQL Store) using the **`happybase`** library.

---

## 🗄️ Architectural Focus: Wide-Column Store

In event-driven streaming ingestion architectures, Kafka often acts as the buffer while HBase serves as the high-throughput, low-latency target NoSQL storage system:

*   **HappyBase** connects to the HBase cluster via the HBase **Thrift gateway** server.
*   The raw order JSON event from Kafka is decomposed into individual columns inside a single column family (`order_info`).
*   The `order_id` is used as the HBase **Row Key**, ensuring fast O(1) key-based lookups and range scans.

---

## 🛠️ Local Setup & Infrastructure

### 1. Booting Kafka and HBase with Docker Compose

You can spin up Kafka (KRaft mode) and HBase (with Thrift server and UI enabled) using the following service definition in a `docker-compose.yml`:

```yaml
version: '3.8'
services:
  # HBase with Thrift Server
  hbase:
    image: dajobe/hbase:latest
    container_name: hbase-local
    ports:
      - "9090:9090"      # Thrift Service API (Used by HappyBase)
      - "16010:16010"    # HBase Master Web UI
    environment:
      - HBASE_CONFIG_hbase_master_port=16000

  # Kafka Broker (KRaft Mode)
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka-local
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092'
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_LBR_CONTROLLER_QUORUM_VOTERS: '1@localhost:29093'
      KAFKA_LISTENERS: 'PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,PLAINTEXT_HOST://0.0.0.0:9092'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_LOG_DIRS: '/tmp/kraft-combined-logs'
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'
```

---

## 🚀 Running the Integration

### 1. Install Dependencies

Create a virtual environment and install the required modules:

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/activate

# Install modules
pip install -r requirements.txt
```

> [!NOTE]
> If building `thrift` or `happybase` fails due to local compilation environments on macOS (e.g. M1/M2 chips), you can run both scripts in **Mock Mode** using the automated connection fallbacks.

### 2. Run the Consumer

The consumer connects to Kafka and writes to HBase.

*   **Mock Mode (Default fallback if HBase is not running):**
    ```bash
    MOCK_DB=true python3 consumer.py
    ```
*   **Live Mode (With Docker Compose HBase running):**
    ```bash
    python3 consumer.py
    ```

When started in Live Mode, it automatically creates the HBase table `orders` with the column family `order_info`.

### 3. Run the Simulation Producer

To populate the Kafka topic with sample events:

```bash
python3 producer.py
```
