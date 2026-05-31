package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	brokerAddress = "localhost:9092"
	topic         = "orders-go"
	groupID       = "go-billing-group"
)

// Order represents our standard domain payload
type Order struct {
	OrderID    string    `json:"order_id"`
	CustomerID string    `json:"customer_id"`
	Amount     float64   `json:"amount"`
	Timestamp  time.Time `json:"timestamp"`
}

func main() {
	mode := flag.String("mode", "producer", "Run mode: 'producer' or 'consumer'")
	flag.Parse()

	// Capture OS signals for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *mode == "producer" {
		runProducer(ctx)
	} else if *mode == "consumer" {
		runConsumer(ctx)
	} else {
		log.Fatalf("Invalid mode: choose 'producer' or 'consumer'")
	}
}

func runProducer(ctx context.Context) {
	log.Printf("🚀 Starting Go Producer... Publishing to: %s", topic)

	// Initialize high-throughput writer config
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		// Resiliency Configurations
		RequiredAcks: kafka.RequireAll, // acks=all
		Async:        false,            // Synchronous writes for strict ordering
	}
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("Error closing writer: %v", err)
		}
	}()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Generate 10 orders then exit
	sentCount := 0
	for sentCount < 10 {
		select {
		case <-ctx.Done():
			log.Println("Stopping producer gracefully...")
			return
		case <-ticker.C:
			order := Order{
				OrderID:    fmt.Sprintf("ORD-GO-%d", rand.Intn(100000)),
				CustomerID: fmt.Sprintf("CUST-GO-%d", rand.Intn(500)+100),
				Amount:     float64(rand.Intn(20000)) / 100.0,
				Timestamp:  time.Now().UTC(),
			}

			payload, err := json.Marshal(order)
			if err != nil {
				log.Printf("Error marshaling order: %v", err)
				continue
			}

			// Write to Kafka
			err = writer.WriteMessages(ctx, kafka.Message{
				Key:   []byte(order.CustomerID),
				Value: payload,
			})

			if err != nil {
				log.Printf("❌ Failed to write message: %v", err)
			} else {
				log.Printf("✅ Order successfully written: %s | Customer: %s | Amount: $%.2f",
					order.OrderID, order.CustomerID, order.Amount)
			}
			sentCount++
		}
	}
	log.Println("Producer finished sending test batch of 10 messages.")
}

func runConsumer(ctx context.Context) {
	log.Printf("📥 Starting Go Consumer... Group: %s, Subscribed: %s", groupID, topic)

	// Initialize Consumer Reader config
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokerAddress},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
		// Offsets settings
		StartOffset: kafka.FirstOffset,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Error closing reader: %v", err)
		}
	}()

	for {
		// ReadMessage automatically fetches and blocks
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("Shutdown signal processed. Exiting consumer loop...")
				return
			}
			log.Printf("Error fetching message: %v", err)
			continue
		}

		// Process partition record
		var order Order
		if err := json.Unmarshal(msg.Value, &order); err != nil {
			log.Printf("⚠️ Toxic message skipped (JSON error): %v", err)
			// Commit the offset of un-parseable messages to prevent loop stalls
			if err := reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("Failed to commit offset: %v", err)
			}
			continue
		}

		// Simulated business task execution
		log.Printf("📦 [Process Success] Order: %s | Key: %s | Partition: %d | Offset: %d | Amt: $%.2f",
			order.OrderID, string(msg.Key), msg.Partition, msg.Offset, order.Amount)

		// Manual synchronous offset commit after parsing successfully
		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("❌ Failed to commit manual offset: %v", err)
		}
	}
}
