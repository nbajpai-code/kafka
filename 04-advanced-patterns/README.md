# ⚙️ Module 4: Enterprise Integration Patterns

In enterprise architectures, simple publish-subscribe flows are rarely enough. Resilient production platforms must solve complex challenges such as:
1.  **Handling poison pills** without halting whole pipeline ingestion.
2.  **Enforcing structural schema contracts** dynamically as systems evolve.
3.  **Guaranteeing exact state consistency** across multiple separate data stores.

This module provides comprehensive architecture guides and implementation blueprints for three vital advanced Kafka patterns.

---

## 🗂️ Module Contents

### 1. [🚨 Dead Letter Queue (DLQ) Pattern](./dead-letter-queue/)
*   **The Problem:** Un-parseable "poison pill" messages cause partition consumer loops to crash repeatedly, locking downstream processing.
*   **The Pattern:** Intercept processing exceptions, route the failing raw record to a dedicated `.DLQ` topic, and manually commit the main offset to resume consumption immediately.
*   **Implementation:** Complete flow design and execution guide.

### 2. [📜 Schema Registry & Avro Integration](./schema-registry-avro/)
*   **The Problem:** Producers change JSON structures without warning, breaking downstream consumers silently in production.
*   **The Pattern:** Decouple schema contracts using **Confluent Schema Registry** and **Apache Avro** binary serialization.
*   **Implementation:** Detailed guide showing Avro schema (`.avsc`) design, registration, and backward/forward evolution rules.

### 3. [🎯 Transactional Exactly-Once Semantics (EOS)](./transactional-exactly-once/)
*   **The Problem:** During system failures, duplicate messages written in a read-process-write loop corrupt financial ledger values or count aggregates.
*   **The Pattern:** Synchronize partition writes and offset commits using a **Transaction Coordinator** broker to guarantee atomic, all-or-nothing operations.
*   **Implementation:** Full step-by-step transaction workflow diagram and code mechanics breakdown.

---
*Let's dive into our first pattern, the highly critical Dead Letter Queue system:* **[DLQ Pattern ➡️](./dead-letter-queue/)**
