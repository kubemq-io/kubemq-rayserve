"""
Embedding Pipeline — Text to Embedding Vector Generation

Demonstrates:
    - Batch text chunking and embedding generation
    - Simulated embedding vector output (no real ML dependencies)
    - Processing multiple text chunks in parallel
    - Collecting vector results for downstream use

Usage:
    python examples/ml_patterns/embedding_pipeline.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

import hashlib
import os
import time
import uuid

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")

EMBEDDING_DIM = 384  # Simulated embedding dimension (e.g., sentence-transformers/all-MiniLM-L6-v2)
BATCH_SIZE = 6


def generate_embedding(text: str, model_name: str = "all-MiniLM-L6-v2") -> dict:
    """Simulated embedding generation for a text chunk."""
    # In production: model = SentenceTransformer(model_name)
    # In production: embedding = model.encode(text).tolist()
    time.sleep(0.08)  # Simulate 80ms inference

    # Generate deterministic pseudo-embedding from text hash
    text_hash = hashlib.sha256(text.encode()).hexdigest()
    embedding = [
        round((int(text_hash[i % len(text_hash)], 16) - 8) / 8.0, 4) for i in range(EMBEDDING_DIM)
    ]

    return {
        "text_preview": text[:50] + ("..." if len(text) > 50 else ""),
        "model": model_name,
        "embedding_dim": EMBEDDING_DIM,
        "embedding": embedding[:10],  # Truncated for display; full vector in production
        "norm": round(sum(v * v for v in embedding) ** 0.5, 4),
    }


def main():
    channel = f"example-ml-embedding-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 1
        failed_task_queue_name = f"{channel}.dlq"
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=3, task_processor_config=_Cfg())
    adapter.register_task_handler(generate_embedding, name="embed")
    adapter.start_consumer()

    try:
        # Simulate a document split into chunks
        chunks = [
            "KubeMQ is a Kubernetes-native message broker for microservices.",
            "Ray Serve enables scalable model serving on Ray clusters.",
            "The adapter bridges KubeMQ queues with Ray Serve deployments.",
            "Embeddings convert text into dense vector representations.",
            "Vector similarity search powers semantic retrieval systems.",
            "Batch processing improves throughput for embedding pipelines.",
        ]

        print(f"Embedding Pipeline: {len(chunks)} text chunks")
        print("  Embedding model: all-MiniLM-L6-v2 (simulated)")
        print(f"  Embedding dimension: {EMBEDDING_DIM}")
        print(f"  Queue: {channel}")

        # Enqueue all chunks as a batch
        print("\nEnqueuing text chunks...")
        task_ids = []
        start = time.perf_counter()
        for i, chunk in enumerate(chunks):
            result = adapter.enqueue_task_sync(
                "embed", args=[chunk], kwargs={"model_name": "all-MiniLM-L6-v2"}
            )
            task_ids.append(result.id)
            print(f"  Chunk {i + 1}: '{chunk[:40]}...' -> task_id={result.id}")

        enqueue_time = time.perf_counter() - start
        print(f"\nEnqueued {len(chunks)} chunks in {enqueue_time:.2f}s")

        # Collect all embedding results
        print("\nCollecting embeddings...")
        embeddings: list[dict] = []
        timeout = 30
        poll_start = time.perf_counter()

        pending = set(range(len(task_ids)))
        while pending and (time.perf_counter() - poll_start) < timeout:
            for idx in list(pending):
                status = adapter.get_task_status_sync(task_ids[idx])
                if status.status in ("SUCCESS", "FAILURE"):
                    embeddings.append(
                        {
                            "chunk_idx": idx,
                            "status": status.status,
                            "result": status.result,
                        }
                    )
                    pending.discard(idx)
            if pending:
                time.sleep(0.5)

        total_time = time.perf_counter() - start
        successes = sum(1 for e in embeddings if e["status"] == "SUCCESS")

        # Display results
        print("\n--- Embedding Results ---")
        print(f"Completed: {successes}/{len(chunks)} SUCCESS")
        print(f"Total time: {total_time:.2f}s")
        print(f"Throughput: {len(chunks) / total_time:.1f} chunks/sec")

        for emb in sorted(embeddings, key=lambda x: x["chunk_idx"]):
            if emb["status"] == "SUCCESS":
                r = emb["result"]
                print(f"\n  Chunk {emb['chunk_idx'] + 1}: {r['text_preview']}")
                print(f"    dim={r['embedding_dim']}, norm={r['norm']}")
                print(f"    vector (first 5): {r['embedding'][:5]}")
    finally:
        adapter.stop_consumer()

    print("\nExample complete.")


if __name__ == "__main__":
    main()
