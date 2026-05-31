# 🐹 Go Kafka Client (Segmentio/kafka-go)

This directory features a pure, high-performance Go integration utilizing **`segmentio/kafka-go`**.

Unlike Confluent's Go client, `kafka-go` does not wrap C-based dependencies (librdkafka), making it extremely simple to compile and cross-compile without CGO environments.

---

## 🏗️ Highlights Included

*   **Unified Executable:** The same code operates in either `-mode=producer` or `-mode=consumer` by leveraging standard Go flags.
*   **Context API Driven:** Integrates natively with Go standard library `context.Context` to handle thread-safe cancellations, networking timeouts, and graceful OS signals cleanly.
*   **At-Least-Once Commitment:** Offsets are manually retrieved (`FetchMessage`) and synchronously committed (`CommitMessages`) only after successfully unpacking the JSON record.

---

## 🚀 How to Run

### 1. Download Dependencies
```bash
go mod tidy
```

### 2. Start the Consumer Mode
In a dedicated terminal pane:
```bash
go run main.go -mode=consumer
```

### 3. Start the Producer Mode
In a separate terminal pane:
```bash
go run main.go -mode=producer
```

Watch the terminal outputs process the values concurrently and commit manual offset positions instantly!
