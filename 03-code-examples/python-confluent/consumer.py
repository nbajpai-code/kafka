import json
import signal
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "orders-python"
GROUP_ID = "python-billing-group"

# State variable to track shutdown
running = True

def signal_handler(sig, frame):
    """Handle termination signals (Ctrl+C) to trigger a graceful shutdown."""
    global running
    print("\n🛑 Signal received. Initiating graceful shutdown...")
    running = False

# Register signals
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def main():
    # Consumer configurations
    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,  # Manual commit for At-Least-Once processing
        'client.id': 'python-consumer'
    }

    # Initialize consumer
    consumer = Consumer(config)

    try:
        # Subscribe to topic
        consumer.subscribe([TOPIC_NAME])
        print(f"📥 Python Consumer registered to group '{GROUP_ID}'. Subscribed to '{TOPIC_NAME}'...")

        while running:
            # Poll for messages (timeout 1.0 second)
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Reached end of partition
                    continue
                else:
                    raise KafkaException(msg.error())

            # Parse key and value
            key = msg.key().decode('utf-8') if msg.key() else "No Key"
            value_raw = msg.value().decode('utf-8')

            try:
                # Process the message (Simulated JSON business logic)
                order = json.loads(value_raw)
                print(f"📦 [Process Success] Order: {order['order_id']} | Key (Customer): {key} | Amount: ${order['amount']}")
                
                # Commit offset synchronously only after SUCCESSFUL processing
                consumer.commit(asynchronous=False)
                
            except json.JSONDecodeError:
                print(f"⚠️ Toxic message skipped: Failed to parse JSON. Raw content: {value_raw}")
                # In production, route this to a Dead Letter Queue and commit the offset to prevent stalling!
                consumer.commit(asynchronous=False)

    except Exception as e:
        print(f"❌ Consumer encountered critical error: {e}", file=sys.stderr)
    finally:
        # Close consumer (informs coordinator of departure and commits remaining offsets)
        print("Closing consumer socket connection...")
        consumer.close()
        print("Graceful shutdown complete.")

if __name__ == "__main__":
    main()
