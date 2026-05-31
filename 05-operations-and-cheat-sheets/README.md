# 🛠️ Module 5: Operations, Configuration Hardening & Tuning

Running Apache Kafka in production requires operational knowledge. As a Kafka administrator or site reliability engineer (SRE), you need to manage topics, audit lag, tune configurations for different system profiles, and secure broker data pathways.

This module is your operational manual, containing practical guides, dynamic performance tuning maps, and security hardening procedures.

---

## 🗂️ Module Contents

### 1. [📋 Admin CLI Command Cheat Sheet](./cli-cheat-sheet.md)
*   Directly copy-pasteable command templates for daily operations.
*   **Topics Admin:** Creating, altering partitions, deleting, dynamically updating configurations.
*   **Consumers Admin:** Listing active groups, checking progress/lag, resetting offsets (to earliest, latest, specific datetime).
*   **Utility scripts:** Custom console producer/consumer diagnostics.

### 2. [⚡ Performance Tuning Guide](./performance-tuning.md)
*   **The Tuning Matrix:** Side-by-side comparison of three standard cluster profiles:
    1.  *High Throughput / Batching Profile*
    2.  *Ultra-Low Latency Profile*
    3.  *High Resiliency / Zero-Loss Profile*
*   Producer-side parameters (`linger.ms`, `batch.size`, `compression.type`).
*   Broker-side parameters (`num.recovery.threads.per.data.dir`, `log.cleaner.threads`).
*   Consumer-side parameters (`fetch.min.bytes`, `max.poll.interval.ms`).

### 3. [🔒 Security Hardening Manual](./security-hardening.md)
*   **SSL/TLS Encryption:** Encrypting data transit pathways between clients and brokers.
*   **SASL Authentication:** Setting up client credentials and broker verification.
    *   *SASL/PLAIN:* Minimal setup.
    *   *SASL/SCRAM:* Enterprise credential hashing.
    *   *SASL/GSSAPI (Kerberos):* Active Directory standard.
*   **Access Control Lists (ACLs):** Enforcing granular topic-level read/write rules for client applications.

---
*Let's start by mastering the CLI administrative tools:* **[CLI Cheat Sheet ➡️](./cli-cheat-sheet.md)**
