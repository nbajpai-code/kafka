# 💻 Module 3: Ready-to-Run Multi-Language Clients

Building robust client applications requires understanding the language-specific client libraries and establishing best practices (e.g., graceful shutdown, manual commits, deserialization logic).

This module contains **complete, functional, and well-structured code templates** for producing and consuming messages in four major modern software ecosystems.

---

## 🗂️ Language Directories

| Language | Client Library | Features Highlighted |
| :--- | :--- | :--- |
| [**🐍 Python**](./python-confluent/) | `confluent-kafka` | High performance (librdkafka binding), manual offset commit, error catching. |
| [**🛢️ Python HBase**](./python-hbase/) | `confluent-kafka`, `happybase` | Wide-column database sink ingestion, automated schema verification, dry-run/mock fallbacks. |
| [**☕ Java (Spring Boot)**](./java-spring-boot/) | `spring-kafka` | Auto-serialization, structured retry policy, Dead Letter Topic (DLT) routing. |
| [**🐹 Go**](./go-segmentio/) | `segmentio/kafka-go` | Pure Go implementation, concurrent partition reading, standard library context handling. |
| [**🟢 Node.js**](./nodejs-kafkajs/) | `kafkajs` | Zero-dependency JS native client, batch consumer processing, async event loops. |

---

## ⚡ Unified Topology

Each client application is pre-configured to:
1.  Connect to a local broker running at **`localhost:9092`** (you can use the [Single Node Cluster](../02-cluster-setups/kraft-single-node/) to test).
2.  Produce a JSON object resembling a purchase order:
    ```json
    {
      "order_id": "ORD-10492-9382",
      "customer_id": "USR-482",
      "amount": 149.99,
      "timestamp": "2026-05-31T18:00:00Z"
    }
    ```
3.  Read messages from the same topic, deserialize the JSON, process it safely, and handle server/client errors gracefully.

---

## 🏗️ General Best Practices Implemented Across Clients

*   **Graceful Shutdown:** Every client captures termination signals (`SIGINT`, `SIGTERM`) to notify the broker of its departure, close open sockets, and commit pending offsets before exiting.
*   **Manual Commits:** Discards insecure auto-commits (`enable.auto.commit=false`) to ensure true **At-Least-Once** semantics. Offsets are committed only after the client successfully processes the message.
*   **Error Logging:** High-fidelity console logging prints event status and tracks connectivity problems immediately.

---
*Let's check out the Python implementations:* **[Standard Python Client ➡️](./python-confluent/)** | **[HBase / HappyBase Multi-Sync Client ➡️](./python-hbase/)**
