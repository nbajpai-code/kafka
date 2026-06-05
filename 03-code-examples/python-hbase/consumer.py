import os
import json
import signal
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException
import happybase

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "orders-hbase"
GROUP_ID = "hbase-sync-consumer-group"
HBASE_HOST = os.getenv("HBASE_HOST", "localhost")
HBASE_PORT = int(os.getenv("HBASE_PORT", 9090))
MOCK_DB = os.getenv("MOCK_DB", "false").lower() in ("true", "1", "yes")

running = True
hbase_connection = None
hbase_table = None

def signal_handler(sig, frame):
    global running
    print("\n🛑 Signal received. Initiating graceful shutdown...")
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def init_database():
    global hbase_connection, hbase_table, MOCK_DB
    
    if MOCK_DB:
        print("ℹ️ MOCK_DB environment variable is enabled. Using mock database interface.")
        return
        
    try:
        print(f"🔗 Connecting to HBase Thrift server at {HBASE_HOST}:{HBASE_PORT} (timeout=5s)...")
        # Add a timeout so it doesn't hang forever
        hbase_connection = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT, timeout=5000)
        
        # Verify connection by listing tables
        tables = hbase_connection.tables()
        print(f"✅ Connected to HBase. Existing tables: {tables}")
        
        # Ensure 'orders' table exists with column family 'order_info'
        table_name = b'orders'
        if table_name not in tables:
            print(f"🛠️ Table 'orders' not found. Creating it with column family 'order_info'...")
            hbase_connection.create_table(
                table_name,
                {b'order_info': dict()}
            )
            print("✅ Table 'orders' created successfully.")
            
        hbase_table = hbase_connection.table(table_name)
    except Exception as e:
        print(f"⚠️ Could not connect to HBase Thrift server: {e}")
        print("ℹ️ Falling back to HBase mock/dry-run mode. (Start HBase Thrift server to enable live sync)")
        MOCK_DB = True

def main():
    init_database()
    
    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,  # Manual commits for At-Least-Once processing
        'client.id': 'hbase-sync-consumer'
    }
    
    try:
        consumer = Consumer(config)
    except Exception as e:
        print(f"❌ Failed to create Kafka Consumer: {e}")
        sys.exit(1)
        
    try:
        consumer.subscribe([TOPIC_NAME])
        print(f"📥 Subscribed to Kafka topic '{TOPIC_NAME}'...")
        print("🚀 Ready to process events. Press Ctrl+C to terminate.")
        
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
                    
            key = msg.key().decode('utf-8') if msg.key() else "No Key"
            value_raw = msg.value().decode('utf-8')
            
            try:
                # 1. Parse JSON message
                order = json.loads(value_raw)
                order_id = order["order_id"]
                customer_id = order["customer_id"]
                product = order["product"]
                amount = str(order["amount"])
                timestamp = order["timestamp"]
                
                print(f"\n📦 [Processing] Received Order: {order_id} (Customer: {customer_id}, Product: {product}, Amount: ${amount})")
                
                # 2. Write to HBase (Wide-column Store)
                if MOCK_DB:
                    print(f"ℹ️ [HBase Mock Write] Row key: {order_id} | Data: {{'order_info:customer_id': {customer_id}, 'order_info:product': {product}, 'order_info:amount': {amount}, 'order_info:timestamp': {timestamp}}}")
                else:
                    hbase_table.put(
                        order_id.encode('utf-8'),
                        {
                            b'order_info:customer_id': customer_id.encode('utf-8'),
                            b'order_info:product': product.encode('utf-8'),
                            b'order_info:amount': amount.encode('utf-8'),
                            b'order_info:timestamp': timestamp.encode('utf-8')
                        }
                    )
                    print(f"✅ [HBase Sync] Successfully persisted order {order_id}")
                    
                # 3. Commit offset manually after SUCCESSFUL write to HBase
                consumer.commit(asynchronous=False)
                print(f"💾 [Commit] Offsets committed for order {order_id}")
                
            except json.JSONDecodeError:
                print(f"⚠️ Skipped invalid JSON: {value_raw}")
                consumer.commit(asynchronous=False)  # Commit to progress past invalid message
            except Exception as e:
                print(f"❌ Error processing message: {e}")
                # In production, route to DLQ or retry instead of exiting!
                
    except Exception as e:
        print(f"❌ Consumer critical error: {e}", file=sys.stderr)
    finally:
        print("Closing Kafka consumer and cleaning up database connections...")
        consumer.close()
        if hbase_connection is not None:
            hbase_connection.close()
        print("Graceful shutdown complete.")

if __name__ == "__main__":
    main()
