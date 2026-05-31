# 📚 Module 1: Kafka Architecture & Core Concepts

Understanding the underlying storage and distribution model of Apache Kafka is crucial for building scalable, fault-tolerant, and high-performance applications. 

This module provides deep-dive guides detailing Kafka's internal mechanics, data layout, coordination engine, and event delivery semantics.

---

## 🗂️ Module Contents

### 1. [🏁 01. Kafka Core Concepts](./01-core-concepts.md)
*   **Topic, Partitions & Log Segments:** Dive into how Kafka stores data sequentially on disk.
*   **Offsets & Offset Tracking:** How consumer state is persisted via the internal `__consumer_offsets` topic.
*   **Consumer Groups & Rebalancing:** Visual explanation of consumer work distribution, group coordinator roles, and partition assigners.
*   **Retention Policies:** Time-based, size-based, and log compaction (`compact` vs `delete`).

### 2. [🛡️ 02. KRaft vs. Legacy ZooKeeper](./02-kraft-vs-zookeeper.md)
*   **The ZooKeeper Era:** Why external consensus became a major scalability bottleneck.
*   **What is KRaft (Kafka Raft)?** How Kafka manages its own metadata log directly within the cluster brokers.
*   **Leader Election:** Comparative breakdown of controller elections and metadata replication times.
*   **Key Operational Differences:** Port settings, configuration simplicity, and metadata partition structures.

### 3. [🎯 03. Delivery Guarantees & Transactional Semantics](./03-delivery-guarantees.md)
*   **Producer Acks:** Demystifying `acks=0`, `acks=1`, and `acks=all` (and their impact on durability).
*   **Delivery Guarantees:** 
    *   *At-Most-Once:* Zero duplication risk, high risk of data loss.
    *   *At-Least-Once:* Zero data loss, risk of duplicate message processing.
    *   *Exactly-Once Semantics (EOS):* True transaction coordination across multiple topics.
*   **Idempotent Producers:** Deduplication logic using Sequence Numbers and Producer IDs (PID).

---

## 🧠 Why Architecture Matters First

Kafka is not a standard message queue like RabbitMQ or ActiveMQ. It is a **distributed commit log**. Because of this:
*   Data is **persisted sequentially to disk** (making it faster than random memory accesses under high load).
*   Consumers **pull** data from Kafka and maintain their own read state (offsets).
*   Topics are horizontally scaled using **partitions**, which determines the maximum parallel consumer count.

Let's begin by mastering the core building blocks: **[Start with 01. Core Concepts ➡️](./01-core-concepts.md)**
