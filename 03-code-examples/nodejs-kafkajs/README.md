# 🟢 Node.js Kafka Client (KafkaJS)

This directory features a modern, clean Node.js integration utilizing **`kafkajs`**, a feature-rich, pure JavaScript client for Apache Kafka.

Because it is written entirely in JS, it contains zero external C binding dependencies, meaning it runs seamlessly across Windows, macOS, and Linux without native compile tools.

---

## 🏗️ Highlights Included

*   **Dual Mode Script:** Use CLI flags (`--mode=producer` or `--mode=consumer`) to trigger either flow from a single file index.
*   **Idempotent Writes:** Enables strict producer deduplication (`idempotent: true`) and cluster-wide writes acknowledgement (`acks: -1`).
*   **Manual Batch Indexing:** Bypasses auto-commits (`autoCommit: false`) to implement secure **At-Least-Once** deliveries, manually committing processed offset coordinates via `consumer.commitOffsets`.

---

## 🚀 How to Run

### 1. Install Dependencies
```bash
npm install
```

### 2. Launch the Consumer
In a dedicated terminal pane:
```bash
node index.js --mode=consumer
```

### 3. Launch the Producer
In a separate terminal pane:
```bash
node index.js --mode=producer
```

Watch the console run the async loops, process JSON purchase models, and manually advance offset coordinates synchronously!
