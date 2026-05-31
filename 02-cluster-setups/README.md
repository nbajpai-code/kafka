# 🐳 Module 2: Cluster Setups (Docker Compose)

The best way to master Kafka is by interacting with a running cluster. This module contains three ready-to-run, fully containerized **Docker Compose** configurations representing common developer environments.

All setups utilize **KRaft (Kafka Raft Metadata Mode)** to align with modern Apache Kafka standards, removing any requirement for legacy ZooKeeper containers.

---

## 🚀 The Clusters

### 1. [🌱 Single-Node KRaft Cluster](./kraft-single-node/)
*   **Purpose:** Ultra-lightweight development cluster. Excellent for quick prototyping and testing client consumer/producer loops.
*   **Configuration:** A single combined broker (`process.roles=broker,controller`).
*   **Port:** Broker exposed on `localhost:9092`.

### 2. [🖥️ Multi-Broker Resilient Cluster with Web UI](./kraft-multi-node-with-ui/)
*   **Purpose:** Simulates a production cluster environment with partition distribution, replica synchronization, and leader failovers.
*   **Configuration:** 3 independent brokers acting in dual-roles, coupled with a beautiful, feature-rich web management portal (**Kafka-UI**).
*   **Ports:** Brokers exposed on `localhost:9092`, `localhost:9093`, and `localhost:9094`. UI dashboard running at **`http://localhost:8080`**.

### 3. [📊 Production Monitoring Stack](./monitoring-stack/)
*   **Purpose:** Learn how to monitor Kafka metrics like partition lag, cluster health, JVM thread counts, and bytes in/out.
*   **Configuration:** Kafka Broker equipped with a **JMX Exporter**, feeding cluster telemetry directly into a **Prometheus** server, visualized through a pre-configured **Grafana** dashboard.
*   **Ports:** Grafana interface running at **`http://localhost:3000`** (Default username/password: `admin` / `admin`).

---

## 🛠️ Global Requirements

Before launching any cluster, ensure you have:
*   [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running.
*   Allocated at least **4GB of RAM** in your Docker Settings (highly recommended for the multi-broker and monitoring stacks).

---

## ⏱️ Quick Management Commands

To spin up any environment:
```bash
# Navigate to the chosen cluster directory
cd 02-cluster-setups/<directory-name>

# Start the cluster in detached mode
docker compose up -d
```

To view container logs:
```bash
docker compose logs -f
```

To stop and remove containers (while preserving data volumes):
```bash
docker compose down
```

To completely reset and wipe all volume data (recommended for fresh starts):
```bash
docker compose down -v
```

---
*Let's check out our first setup: a lightweight single-node developer cluster:* **[Single Node Setup ➡️](./kraft-single-node/)**
