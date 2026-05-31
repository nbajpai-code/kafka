# ☕ Java Spring Boot Kafka Integration

This is a comprehensive, production-grade Spring Boot integration demonstrating enterprise Kafka integration patterns.

---

## 🏗️ Enterprise Features Demonstrated

1.  **Automatic Topic Provisioning:** The [`KafkaConfig`](./src/main/java/com/example/kafka/config/KafkaConfig.java) automatically creates topics `orders-java` (with 3 partitions) and `orders-java.DLT` (with 1 partition) when the application boots, removing administrative overhead.
2.  **Manual Acknowledgements:** By using `manual_immediate` ack mode coupled with the `Acknowledgment` object, offsets are committed back to the cluster **only** when processing business logic completes safely.
3.  **Resilient Retry & Dead Letter Queues (DLQ):** If consumption throws an exception, the configured `DefaultErrorHandler` retries the message **3 times with a 2-second delay**. If it continues to fail, the exception is intercepted by `DeadLetterPublishingRecoverer` and dynamically routed to `orders-java.DLT` so partition processing is never blocked!

---

## 🚀 How to Run

### 1. Ensure Local Broker is Online
Start the local single-broker environment:
```bash
cd ../../02-cluster-setups/kraft-single-node
docker compose up -d
```

### 2. Compile and Boot the Application
Navigate back to this directory and boot using Maven:
```bash
cd ../../03-code-examples/java-spring-boot
./mvnw spring-boot:run
```
*(If `./mvnw` is not executable, make it executable via `chmod +x mvnw` or use your local maven installation `mvn spring-boot:run`)*

### 🔍 What Will Happen
1.  On startup, `KafkaConfig` registers and creates topics `orders-java` and `orders-java.DLT`.
2.  The `demoProducer` Command Line Runner launches, generating 5 purchase orders.
3.  The consumer reads these orders from `orders-java`. 
4.  Notice that order 5 has an amount over $400.0, causing the consumer to throw a **simulated validation exception**.
5.  Watch the console: Spring will **retry** processing 2 times, then capture it, and route it to the **DLQ (`orders-java.DLT`)**, logging a warning:
    `🚨 [DLQ INGESTION] Toxic order isolated! Order: ORD-...`
