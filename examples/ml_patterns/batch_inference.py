"""
Batch Inference — Process Multiple Items in Parallel

Demonstrates:
    - Enqueuing a batch of 100 items
    - Parallel processing with consumer_concurrency > 1
    - Collecting all results with polling
    - Throughput measurement

Usage:
    python examples/ml_patterns/batch_inference.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

import os
import time
import uuid

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")

BATCH_SIZE = 10  # Keep small for quick example runs within 30s timeout


def classify_item(item_id: int, text: str) -> dict:
    """Simulated classification for a single item."""
    time.sleep(0.05)  # Simulate 50ms inference
    return {"item_id": item_id, "label": "positive" if item_id % 2 == 0 else "negative"}


def main():
    channel = f"example-batch-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 1
        failed_task_queue_name = f"{channel}.dlq"
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=5, task_processor_config=_Cfg())
    adapter.register_task_handler(classify_item, name="classify_item")
    adapter.start_consumer()

    try:
        # Enqueue batch
        task_ids = []
        start = time.perf_counter()
        for i in range(BATCH_SIZE):
            result = adapter.enqueue_task_sync("classify_item", args=[i, f"sample text {i}"])
            task_ids.append(result.id)
        enqueue_time = time.perf_counter() - start
        print(f"Enqueued {BATCH_SIZE} tasks in {enqueue_time:.2f}s")

        # Collect results
        completed = {}
        poll_start = time.perf_counter()
        timeout = 60
        while len(completed) < BATCH_SIZE and (time.perf_counter() - poll_start) < timeout:
            for tid in task_ids:
                if tid not in completed:
                    status = adapter.get_task_status_sync(tid)
                    if status.status in ("SUCCESS", "FAILURE"):
                        completed[tid] = status
            time.sleep(0.5)

        total_time = time.perf_counter() - start
        successes = sum(1 for s in completed.values() if s.status == "SUCCESS")
        failures = sum(1 for s in completed.values() if s.status == "FAILURE")
        print(
            f"\nResults: {successes} SUCCESS, {failures} FAILURE, "
            f"{BATCH_SIZE - len(completed)} PENDING"
        )
        print(f"Total time: {total_time:.2f}s")
        print(f"Throughput: {BATCH_SIZE / total_time:.1f} tasks/sec")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
