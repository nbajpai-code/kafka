# 🌱 Single-Node KRaft Cluster

This directory contains a minimal, lightweight, single-container Apache Kafka environment operating in **KRaft** mode. It is optimized for rapid local development and testing client code.

---

## 🚀 How to Run

1.  **Spin Up the Container:**
    ```bash
    docker compose up -d
    ```

2.  **Verify Running Status:**
    ```bash
    docker compose ps
    ```
    You should see the `kafka-single-node` container running and exposing port `9092`.

3.  **Inspect Broker Logs:**
    ```bash
    docker compose logs -f
    ```

---

## 🧪 Testing Your Single Node

To verify the broker is working correctly, you can execute standard scripts directly inside the running container.

### A. Create a Topic
```bash
docker exec -it kafka-single-node kafka-topics --create --topic test-topic --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
```

### B. Produce Messages
Start the console producer and type a few messages (press `Enter` after each line, then `Ctrl+C` to exit):
```bash
docker exec -it kafka-single-node kafka-console-producer --topic test-topic --bootstrap-server localhost:9092
```
*Type:*
```text
Hello Kafka!
Single Node is operational.
```

### C. Consume Messages
Open a terminal and read the messages from the beginning:
```bash
docker exec -it kafka-single-node kafka-console-consumer --topic test-topic --from-beginning --bootstrap-server localhost:9092
```

---

## 🧹 Clean Up

To stop the container and wipe its stored partition data:
```bash
docker compose down -v
```
