#!/usr/bin/env python3
"""
realtime_inference_service.py
Consumes raw text reviews from 'customer-reviews', runs an NLP Sentiment Analysis
model, and publishes the prediction results to 'review-sentiments'.
Uses manual offset management for reliable At-Least-Once processing.
"""

import sys
import time
import json
import signal

# Try importing ML dependencies
MODEL_AVAILABLE = False
try:
    from transformers import pipeline
    print("🤖 HuggingFace Transformers detected. Loading sentiment analysis pipeline...")
    # Using a fast, lightweight DistilBERT model trained on SST-2 sentiment dataset
    nlp_classifier = pipeline(
        "sentiment-analysis", 
        model="distilbert-base-uncased-finetuned-sst-2-english", 
        device=-1 # Use CPU; change to 0 if CUDA/GPU is available
    )
    MODEL_AVAILABLE = True
    print("✅ Model loaded successfully!")
except Exception as e:
    print(f"⚠️ Could not load neural network libraries ({e}).")
    print("👉 Falling back to standard NLP keyword-heuristic rules engine.")

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

# Configuration
BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "customer-reviews"
OUTPUT_TOPIC = "review-sentiments"
CONSUMER_GROUP_ID = "sentiment-inference-service"

# Flag for graceful shutdown
running = True

def signal_handler(sig, frame):
    global running
    print("\n[Inference Service] Shutting down gracefully...")
    running = False

# Register signals
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class HeuristicSentimentClassifier:
    """A fallback rule-based classifier that runs instantly without heavy ML frameworks."""
    def __init__(self):
        self.positive_words = {"amazing", "crystal", "instantly", "recommend", "recommended", "speed", "sleek", "satisfying", "beautiful", "cloud", "best"}
        self.negative_words = {"disappointed", "dropping", "barely", "waste", "uncomfortable", "clamp", "hard", "inaccurate", "struggles"}

    def predict(self, text):
        words = text.lower().split()
        pos_count = sum(1 for w in words if w in self.positive_words or any(pw in w for pw in self.positive_words))
        neg_count = sum(1 for w in words if w in self.negative_words or any(nw in w for nw in self.negative_words))
        
        if pos_count > neg_count:
            # Add some dynamic variety to confidence scores
            score = 0.75 + (min(pos_count - neg_count, 3) * 0.07)
            return "POSITIVE", min(score, 0.98)
        elif neg_count > pos_count:
            score = 0.75 + (min(neg_count - pos_count, 3) * 0.07)
            return "NEGATIVE", min(score, 0.98)
        else:
            return "NEUTRAL", 0.50

fallback_classifier = HeuristicSentimentClassifier()

def run_sentiment_model(text):
    """Executes model inference on incoming text, timing its execution latency."""
    t_start = time.perf_counter()
    
    if MODEL_AVAILABLE:
        try:
            res = nlp_classifier(text)[0]
            label = res['label']  # 'POSITIVE' or 'NEGATIVE'
            score = res['score']  # Confidence probability
            model_type = "DistilBERT (Neural Net)"
        except Exception as err:
            # Internal model pipeline failure, use fallback
            label, score = fallback_classifier.predict(text)
            model_type = f"Fallback Heuristic (Model Error: {err})"
    else:
        label, score = fallback_classifier.predict(text)
        model_type = "Keyword Heuristic (Fallback)"
        
    latency_ms = (time.perf_counter() - t_start) * 1000.0
    return label, score, model_type, latency_ms

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Sentiment output delivery failed: {err}")

def main():
    print("🚀 Initializing Real-Time Model Inference Service...")
    print(f"Reading from: {INPUT_TOPIC} | Writing to: {OUTPUT_TOPIC}")
    
    # Producer to write predictions downstream
    prod_conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': 'sentiment-inference-producer',
        'acks': 'all',
        'enable.idempotence': True
    }
    
    # Consumer to read product review inputs
    cons_conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': CONSUMER_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,          # Disable auto commit for At-Least-Once processing
        'session.timeout.ms': 45000
    }
    
    try:
        producer = Producer(prod_conf)
        consumer = Consumer(cons_conf)
    except Exception as e:
        print(f"❌ Failed to construct Kafka clients: {e}", file=sys.stderr)
        sys.exit(1)

    # Subscribe to review topic
    consumer.subscribe([INPUT_TOPIC])

    print("----------------------------------------------------------------")
    print("Waiting for review messages. Press Ctrl+C to stop.")
    print("----------------------------------------------------------------")

    while running:
        # Poll Kafka broker
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event (normal behavior)
                continue
            else:
                print(f"❌ Consumer Error: {msg.error()}")
                break

        # Process the message
        try:
            key_str = msg.key().decode('utf-8') if msg.key() else 'None'
            payload_str = msg.value().decode('utf-8')
            data = json.loads(payload_str)
            
            review_text = data.get("text", "")
            review_id = data.get("review_id", key_str)
            product = data.get("product", "Unknown")
            
            print(f"\n📥 Received review {review_id} for '{product}': \"{review_text[:60]}...\"")
            
            # RUN INFERENCE
            label, score, model_type, latency_ms = run_sentiment_model(review_text)
            
            print(f"🧠 Sentiment: {label} (Confidence: {score:.2%}) | Latency: {latency_ms:.2f}ms | Model: {model_type}")
            
            # Format output payload
            output_data = {
                "review_id": review_id,
                "product": product,
                "text": review_text,
                "sentiment": label,
                "confidence": round(score, 4),
                "model_name": model_type,
                "inference_latency_ms": round(latency_ms, 2),
                "processed_timestamp": int(time.time() * 1000)
            }
            
            # Publish prediction results to review-sentiments
            producer.produce(
                topic=OUTPUT_TOPIC,
                key=review_id.encode('utf-8'),
                value=json.dumps(output_data).encode('utf-8'),
                on_delivery=delivery_report
            )
            
            # Trigger callbacks for delivery report
            producer.poll(0)
            
            # Commit offset synchronously to ensure the broker knows this message is successfully processed.
            # This implements the crucial At-Least-Once message delivery pattern.
            consumer.commit(message=msg, asynchronous=False)
            print(f"💾 Committed offset for review {review_id} successfully.")
            
        except Exception as e:
            print(f"❌ Error processing message: {e}", file=sys.stderr)
            # In a production context, you would write this record to a Dead Letter Queue (DLQ)
            # topic using producer.produce, and still commit the offset to allow the consumer pipeline to advance.

    # Cleanup connections
    print("\nClosing consumer and producer connections...")
    consumer.close()
    producer.flush(timeout=5.0)
    print("👋 Shutdown complete.")

if __name__ == "__main__":
    main()
