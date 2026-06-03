#!/usr/bin/env python3
"""
simulation_producer.py
Simulates real-time ingestion streams for AI/ML pipelines:
1. Product reviews sent to the 'customer-reviews' topic.
2. Research document snippets sent to the 'ai-documents' topic.
"""

import sys
import time
import json
import random
import signal
from confluent_kafka import Producer

# Configuration
BOOTSTRAP_SERVERS = "localhost:9092"
REVIEWS_TOPIC = "customer-reviews"
DOCS_TOPIC = "ai-documents"

# Sample Customer Reviews (Text and metadata)
SAMPLE_REVIEWS = [
    {"review_id": "rev-101", "product": "AI Smart Assistant Echo-X", "text": "This smart speaker is absolutely amazing! The sound quality is crystal clear and it responds instantly.", "user_id": "usr-552", "timestamp": None},
    {"review_id": "rev-102", "product": "ZenFlow Wireless Headphones", "text": "Extremely disappointed. The bluetooth connection keeps dropping and the battery lasts barely two hours.", "user_id": "usr-129", "timestamp": None},
    {"review_id": "rev-103", "product": "ApexPro Mechanical Keyboard", "text": "It is decent. Tactile feedback is satisfying, but the spacebar squeaks a lot. Okay for the price.", "user_id": "usr-384", "timestamp": None},
    {"review_id": "rev-104", "product": "NovaCharge Power Bank", "text": "Incredible charging speed and sleek design. Easily charges my phone three times over. Highly recommended!", "user_id": "usr-910", "timestamp": None},
    {"review_id": "rev-105", "product": "ZenFlow Wireless Headphones", "text": "Total waste of money. The earcups are super uncomfortable and clamp way too hard on my ears.", "user_id": "usr-441", "timestamp": None},
    {"review_id": "rev-106", "product": "ApexPro Mechanical Keyboard", "text": "The RGB lighting customization is beautiful, and typing on this feels like typing on a cloud. Best purchase this year!", "user_id": "usr-607", "timestamp": None},
    {"review_id": "rev-107", "product": "UltraFit Fitness Tracker", "text": "Heart rate monitor is highly inaccurate. Registered 140 bpm while I was sitting completely still reading.", "user_id": "usr-883", "timestamp": None},
    {"review_id": "rev-108", "product": "AI Smart Assistant Echo-X", "text": "Average product. Voice recognition struggles with my accent sometimes, but works fine overall.", "user_id": "usr-104", "timestamp": None}
]

# Sample Documents (Chunks for Vector DB / RAG ingest)
SAMPLE_DOCUMENTS = [
    {"doc_id": "doc-001", "source": "ai-weekly-newsletter", "title": "Understanding Retrieval-Augmented Generation", "content": "Retrieval-Augmented Generation (RAG) is a technique that combines LLMs with external knowledge stores. By retrieving relevant documents first, LLMs can generate more factual and up-to-date responses, reducing hallucinations significantly."},
    {"doc_id": "doc-002", "source": "distributed-systems-journal", "title": "Kafka's High-Throughput Design", "content": "Apache Kafka achieves high performance through sequential disk I/O, zero-copy memory transfer using OS page cache, and batching of messages. By grouping partition messages together, network overhead is drastically decreased."},
    {"doc_id": "doc-003", "source": "cloud-native-weekly", "title": "Vector Databases in 2026", "content": "Vector databases like Qdrant, Milvus, and Pinecone store high-dimensional embeddings. They use Approximate Nearest Neighbor (ANN) index structures such as HNSW and IVF-PQ to perform similarity searches over millions of vectors in milliseconds."},
    {"doc_id": "doc-004", "source": "mlops-handbook", "title": "Why Real-Time Features Matter", "content": "Batch feature stores are computed overnight, which fails to capture immediate user context. Real-time feature stores ingest event streams via Kafka, compute aggregations on the fly (e.g., click rate in last 5 minutes), and feed them directly into models."},
    {"doc_id": "doc-005", "source": "ai-weekly-newsletter", "title": "Fine-Tuning vs RAG", "content": "While fine-tuning teaches a model new styles, formats, or specialized domain vocabularies, it is expensive and fails to provide dynamic updates. RAG is better suited for integrating real-time business facts, user permissions, and volatile knowledge."}
]

# Flag for graceful shutdown
running = True

def signal_handler(sig, frame):
    global running
    print("\n[Producer] Termination signal received. Shutting down gracefully...")
    running = False

# Set up signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def delivery_report(err, msg):
    """ Called once for each message delivered or failed. """
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        topic = msg.topic()
        key = msg.key().decode('utf-8') if msg.key() else 'None'
        print(f"✅ Delivered to {topic} [Partition: {msg.partition()}] (Key: {key})")

def main():
    print("🚀 Starting AI/ML Simulation Stream Producer...")
    print(f"Connecting to Kafka brokers at: {BOOTSTRAP_SERVERS}")

    # Configure Producer
    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': 'ai-ml-sim-producer',
        'acks': 'all',                      # Wait for full broker replication
        'enable.idempotence': True,         # Protect against duplicate publishes
        'retries': 5,
        'linger.ms': 50                     # Batch messages up to 50ms for throughput
    }

    try:
        producer = Producer(conf)
    except Exception as e:
        print(f"❌ Failed to create producer: {e}", file=sys.stderr)
        sys.exit(1)

    print("----------------------------------------------------------------")
    print(f"Streaming data...")
    print(f"- Reviews will be published to: {REVIEWS_TOPIC}")
    print(f"- Documents will be published to: {DOCS_TOPIC}")
    print("Press Ctrl+C to terminate.")
    print("----------------------------------------------------------------")

    step = 0
    while running:
        current_time = int(time.time() * 1000)

        # 1. Periodically produce a customer review (every ~3 seconds)
        if step % 3 == 0:
            review = random.choice(SAMPLE_REVIEWS).copy()
            review["timestamp"] = current_time
            review_id = review["review_id"]
            
            # Serialize payload
            payload = json.dumps(review).encode('utf-8')
            
            print(f"\n[Producer] 📦 Generating review review_id={review_id} for product='{review['product']}'")
            try:
                producer.produce(
                    topic=REVIEWS_TOPIC,
                    key=review_id.encode('utf-8'),
                    value=payload,
                    on_delivery=delivery_report
                )
            except BufferError:
                print("[Producer] Queue full, flushing...")
                producer.flush()

        # 2. Periodically produce a document chunk (every ~5 seconds)
        if step % 5 == 0:
            doc = random.choice(SAMPLE_DOCUMENTS).copy()
            doc["timestamp"] = current_time
            doc_id = doc["doc_id"]
            
            # Serialize payload
            payload = json.dumps(doc).encode('utf-8')
            
            print(f"\n[Producer] 📰 Generating document chunk doc_id={doc_id} from source='{doc['source']}'")
            try:
                producer.produce(
                    topic=DOCS_TOPIC,
                    key=doc_id.encode('utf-8'),
                    value=payload,
                    on_delivery=delivery_report
                )
            except BufferError:
                print("[Producer] Queue full, flushing...")
                producer.flush()

        # Flush message queue to trigger callbacks
        producer.poll(0)
        
        # Sleep for 1 second
        time.sleep(1)
        step += 1

    # Final flush
    print("[Producer] Flushing remaining messages...")
    producer.flush(timeout=5.0)
    print("[Producer] Done.")

if __name__ == "__main__":
    main()
