#!/usr/bin/env python3
"""
streaming_rag_ingester.py
Consumes document snippets from 'ai-documents', chunks the content, generates
dense vector embeddings using SentenceTransformers, and updates a local Vector Store Index.
Demonstrates the data-ingestion pipeline for real-time Retrieval-Augmented Generation (RAG).
"""

import os
import sys
import time
import json
import signal
import hashlib

# Try importing ML dependencies
MODEL_AVAILABLE = False
try:
    from sentence_transformers import SentenceTransformer
    print("🤖 SentenceTransformers detected. Loading embedding model ('all-MiniLM-L6-v2')...")
    # Using a popular, highly efficient, and lightweight model (384 dimensions)
    embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
    MODEL_AVAILABLE = True
    print("✅ Embedding model loaded successfully! Output dimension: 384")
except Exception as e:
    print(f"⚠️ Could not load sentence-transformers library ({e}).")
    print("👉 Falling back to standard hash-based deterministic vector generator (384-dimensions).")

from confluent_kafka import Consumer, KafkaError

# Configuration
BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "ai-documents"
CONSUMER_GROUP_ID = "rag-embedding-ingester"
VECTOR_STORE_FILE = "vector_store_index.json"

# Flag for graceful shutdown
running = True

def signal_handler(sig, frame):
    global running
    print("\n[RAG Ingester] Shutting down gracefully...")
    running = False

# Register signals
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def generate_embedding(text):
    """Generates a 384-dimensional embedding vector for the given text."""
    t_start = time.perf_counter()
    
    if MODEL_AVAILABLE:
        try:
            # Generate real vector embeddings
            vector = embedding_model.encode(text).tolist()
            model_type = "SentenceTransformers (all-MiniLM-L6-v2)"
        except Exception as err:
            vector = generate_fallback_vector(text)
            model_type = f"Fallback Vector (Embedding Error: {err})"
    else:
        vector = generate_fallback_vector(text)
        model_type = "Deterministic Word-Hash (Fallback)"
        
    latency_ms = (time.perf_counter() - t_start) * 1000.0
    return vector, model_type, latency_ms

def generate_fallback_vector(text):
    """Generates a deterministic 384-dimensional unit vector using string hashing."""
    dimensions = 384
    vector = []
    
    # Hash sections of the text to generate floating points
    for i in range(dimensions):
        h = hashlib.sha256(f"{text}-{i}".encode('utf-8')).hexdigest()
        val = int(h[:8], 16) / 4294967295.0  # Normalize to [0.0, 1.0]
        vector.append(round(val * 2.0 - 1.0, 6))  # Shift to [-1.0, 1.0]
        
    # Normalize vector to unit length
    magnitude = sum(x**2 for x in vector)**0.5
    if magnitude > 0:
        vector = [round(x / magnitude, 6) for x in vector]
        
    return vector

def save_to_vector_store(doc_id, metadata, text_chunk, vector):
    """Saves the chunk and vector to a local file representing our Vector Database."""
    store_data = {}
    
    # Load existing database if available
    if os.path.exists(VECTOR_STORE_FILE):
        try:
            with open(VECTOR_STORE_FILE, 'r') as f:
                store_data = json.load(f)
        except Exception:
            pass
            
    # Add or update entry
    store_data[doc_id] = {
        "chunk_id": doc_id,
        "source": metadata.get("source", "unknown"),
        "title": metadata.get("title", "Untitled"),
        "chunk_text": text_chunk,
        "vector_dim": len(vector),
        "vector_preview": vector[:5],  # Save full vector, but preview first 5 in prints
        "vector": vector,
        "last_updated": int(time.time() * 1000)
    }
    
    # Save back to file
    with open(VECTOR_STORE_FILE, 'w') as f:
        json.dump(store_data, f, indent=2)
        
    return len(store_data)

def main():
    print("🚀 Initializing Streaming RAG Ingester Service...")
    print(f"Reading from: {INPUT_TOPIC} | Output Vector Store: {VECTOR_STORE_FILE}")
    
    # Consumer config
    cons_conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': CONSUMER_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,  # Manually commit offsets after saving to database
        'session.timeout.ms': 45000
    }
    
    try:
        consumer = Consumer(cons_conf)
    except Exception as e:
        print(f"❌ Failed to construct Kafka consumer: {e}", file=sys.stderr)
        sys.exit(1)

    # Subscribe
    consumer.subscribe([INPUT_TOPIC])

    print("----------------------------------------------------------------")
    print("Waiting for document messages. Press Ctrl+C to stop.")
    print("----------------------------------------------------------------")

    while running:
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"❌ Consumer Error: {msg.error()}")
                break

        # Process record
        try:
            key_str = msg.key().decode('utf-8') if msg.key() else 'None'
            payload_str = msg.value().decode('utf-8')
            data = json.loads(payload_str)
            
            content = data.get("content", "")
            doc_id = data.get("doc_id", key_str)
            title = data.get("title", "Unknown")
            source = data.get("source", "Unknown")
            
            print(f"\n📥 Received document '{title}' (Source: {source})")
            
            # Simple Chunking Strategy: For this demo, the input represents pre-chunked paragraphs.
            # In a production pipeline, you might use recursive text splitters (e.g. from LangChain/LlamaIndex)
            # here to split large documents into overlap-chunks of e.g. 500 characters.
            chunks = [content] # For simulation, treat content as one chunk
            
            for idx, chunk in enumerate(chunks):
                chunk_id = f"{doc_id}-c{idx+1}"
                
                # GENERATE EMBEDDINGS
                vector, model_name, latency = generate_embedding(chunk)
                
                print(f"🧬 Vector generated. Dim: {len(vector)} | Latency: {latency:.2f}ms | Model: {model_name}")
                print(f"   Vector Preview: {vector[:5]}...")
                
                # WRITE TO VECTOR STORE
                db_size = save_to_vector_store(
                    doc_id=chunk_id,
                    metadata={"source": source, "title": title},
                    text_chunk=chunk,
                    vector=vector
                )
                print(f"💾 Chunk '{chunk_id}' saved to Vector Store! Total indexed chunks: {db_size}")
                
            # Commit offset synchronously to guarantee it was stored safely
            consumer.commit(message=msg, asynchronous=False)
            print(f"💾 Committed offset for document {doc_id} successfully.")
            
        except Exception as e:
            print(f"❌ Error processing document: {e}", file=sys.stderr)

    # Cleanup
    print("\nClosing consumer connection...")
    consumer.close()
    print("👋 Shutdown complete.")

if __name__ == "__main__":
    main()
