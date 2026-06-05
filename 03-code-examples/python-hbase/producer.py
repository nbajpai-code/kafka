import json
import random
import time
import signal
import sys
from datetime import datetime
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "orders-hbase"

running = True

def signal_handler(sig, frame):
    global running
    print("\n🛑 Shutting down simulation producer...")
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def delivery_report(err, msg):
    """Callback called once for each message sent to indicate delivery success or failure."""
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"🚀 Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def generate_order():
    customers = [f"USR-{random.randint(100, 999)}" for _ in range(10)]
    products = ["laptop", "smartphone", "headphones", "monitor", "keyboard", "mouse"]
    order_id = f"ORD-{random.randint(10000, 99999)}-{random.randint(1000, 9999)}"
    customer_id = random.choice(customers)
    product = random.choice(products)
    amount = round(random.uniform(10.0, 1500.0), 2)
    
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "product": product,
        "amount": amount,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

def main():
    print("⚡ Starting Kafka Simulation Producer (HBase/HappyBase Sync)...")
    
    # Configure producer
    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': 'hbase-sync-producer',
        'acks': 'all',  # Guarantee durability
        'retries': 5,
        'max.in.flight.requests.per.connection': 5
    }
    
    try:
        producer = Producer(config)
    except Exception as e:
        print(f"❌ Failed to create producer: {e}")
        sys.exit(1)
        
    print(f"📡 Publishing messages to topic '{TOPIC_NAME}'. Press Ctrl+C to stop.\n")
    
    while running:
        order = generate_order()
        key = order["customer_id"]
        value = json.dumps(order)
        
        try:
            producer.produce(
                topic=TOPIC_NAME,
                key=key.encode('utf-8'),
                value=value.encode('utf-8'),
                callback=delivery_report
            )
            # Serve callbacks
            producer.poll(0.1)
        except BufferError:
            print("⚠️ Producer queue full, waiting...")
            producer.poll(1.0)
        except Exception as e:
            print(f"❌ Error during produce: {e}")
            
        time.sleep(random.uniform(1.0, 3.0))
        
    print("Flushing pending messages...")
    producer.flush(timeout=5)
    print("Producer shutdown complete.")

if __name__ == "__main__":
    main()
