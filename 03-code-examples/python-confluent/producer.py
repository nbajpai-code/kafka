import json
import random
import time
import uuid
from confluent_kafka import Producer

# Config properties
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "orders-python"

def delivery_report(err, msg):
    """Callback triggered upon successful receipt or persistent failure of a write."""
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message delivered to partition {msg.partition()} [Offset: {msg.offset()}]")

def main():
    # Producer config configuration
    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': 'python-producer',
        # Optimizations for reliable delivery
        'acks': 'all',
        'enable.idempotence': True
    }

    # Initialize producer
    producer = Producer(config)

    print(f"🚀 Starting Python Producer. Sending messages to topic '{TOPIC_NAME}'...")

    try:
        for _ in range(10):
            # Generate dummy order
            order = {
                "order_id": str(uuid.uuid4()),
                "customer_id": f"CUST-{random.randint(100, 999)}",
                "amount": round(random.uniform(10.0, 500.0), 2),
                "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
            }

            key = order["customer_id"]
            value = json.dumps(order)

            # Produce message (asynchronous)
            producer.produce(
                topic=TOPIC_NAME,
                key=key,
                value=value,
                callback=delivery_report
            )

            # Serve pending delivery callbacks
            producer.poll(0)
            time.sleep(1.0)

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        # Flush outstanding messages before shutdown
        print("Flushing outstanding messages...")
        producer.flush()
        print("Shutdown complete.")

if __name__ == "__main__":
    main()
