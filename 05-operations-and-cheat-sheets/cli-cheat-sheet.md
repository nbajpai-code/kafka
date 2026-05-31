# 📋 Apache Kafka Administrative CLI Cheat Sheet

All commands are structured for execution against a running Docker container (using our single-node or multi-node container names: e.g., `kafka-single-node` or `kafka-broker-1`). 

If you are running Kafka natively, simply replace `docker exec -it <container-name> <script>` with the direct path to the scripts, e.g., `./bin/<script>.sh`.

---

## 🗂️ Topic Administration (`kafka-topics`)

### 1. Create a Topic
Create a new topic with 6 partitions and a replication factor of 3:
```bash
docker exec -it kafka-broker-1 kafka-topics --create --topic production-orders --partitions 6 --replication-factor 3 --bootstrap-server localhost:9092
```

### 2. List All Active Topics
```bash
docker exec -it kafka-broker-1 kafka-topics --list --bootstrap-server localhost:9092
```

### 3. Describe Topic Details
View partition counts, replication factors, leader broker assignments, and ISR lists:
```bash
docker exec -it kafka-broker-1 kafka-topics --describe --topic production-orders --bootstrap-server localhost:9092
```

### 4. Increase Partition Count (Scale Up)
Increase the partitions of a topic to 12. 
> [!WARNING]
> You can increase partitions, but you can **never** decrease partition counts because Kafka does not support partition merging.
```bash
docker exec -it kafka-broker-1 kafka-topics --alter --topic production-orders --partitions 12 --bootstrap-server localhost:9092
```

### 5. Delete a Topic
```bash
docker exec -it kafka-broker-1 kafka-topics --delete --topic production-orders --bootstrap-server localhost:9092
```

---

## 👥 Consumer Group Management (`kafka-consumer-groups`)

### 1. List All Active Consumer Groups
```bash
docker exec -it kafka-broker-1 kafka-consumer-groups --list --bootstrap-server localhost:9092
```

### 2. Check Consumer Group Lag & Progress
This is the single most important diagnostic command to run when monitoring consumer speed:
```bash
docker exec -it kafka-broker-1 kafka-consumer-groups --describe --group billing-service --bootstrap-server localhost:9092
```
*Look for the `LOG-END-OFFSET` (latest broker offset), `CURRENT-OFFSET` (consumer committed position), and `LAG` columns.*

### 3. Reset Group Offsets to Earliest (Rewind)
Forces the consumer group to re-read all messages from the beginning of time.
> [!IMPORTANT]
> The consumer group **must be fully stopped/inactive** before you can reset its offsets.
```bash
docker exec -it kafka-broker-1 kafka-consumer-groups --reset-offsets --group billing-service --topic telemetry-orders --to-earliest --execute --bootstrap-server localhost:9092
```

### 4. Reset Group Offsets to Latest (Skip Queue)
Forces the consumer group to skip all un-read queued messages and begin reading from the next incoming message:
```bash
docker exec -it kafka-broker-1 kafka-consumer-groups --reset-offsets --group billing-service --topic telemetry-orders --to-latest --execute --bootstrap-server localhost:9092
```

### 5. Shift Offsets Back by N Messages
Shift offsets back by 50 positions to reprocess the last 50 events:
```bash
docker exec -it kafka-broker-1 kafka-consumer-groups --reset-offsets --group billing-service --topic telemetry-orders --shift-by -50 --execute --bootstrap-server localhost:9092
```

---

## 🔬 Real-Time CLI Testing & Verification

### A. High-Fidelity Console Consumer
Read all events from the beginning, printing message keys, values, and partition locations:
```bash
docker exec -it kafka-broker-1 kafka-console-consumer --topic telemetry-orders --from-beginning --property print.key=true --property print.partition=true --property key.separator=" | Key: " --bootstrap-server localhost:9092
```

### B. Dynamic Console Producer (With Keys)
Produce events containing distinct keys. Use tab to separate the key from the value, and press Enter to publish:
```bash
docker exec -it kafka-broker-1 kafka-console-producer --topic telemetry-orders --property parse.key=true --property key.separator="\t" --bootstrap-server localhost:9092
```
*Example input:*
```text
CUST-101	{"order_id":"1","amount":9.99}
CUST-102	{"order_id":"2","amount":19.99}
```

---

## 🪵 Log and Topic Configuration Alters (`kafka-configs`)

### 1. Change Retention Period Dynamically
Alter the log retention period of a topic to 24 hours (`86400000` milliseconds) without restarting any brokers:
```bash
docker exec -it kafka-broker-1 kafka-configs --alter --entity-type topics --entity-name telemetry-orders --add-config retention.ms=86400000 --bootstrap-server localhost:9092
```

### 2. Verify Dynamic Configurations
```bash
docker exec -it kafka-broker-1 kafka-configs --describe --entity-type topics --entity-name telemetry-orders --bootstrap-server localhost:9092
```

### 3. Clear Dynamic Configurations (Reset to Default)
```bash
docker exec -it kafka-broker-1 kafka-configs --alter --entity-type topics --entity-name telemetry-orders --delete-config retention.ms --bootstrap-server localhost:9092
```
