"""
Batch Enqueue — Async Queue-Based Inference

Demonstrates:
    - Enqueuing multiple tasks in a loop
    - Collecting all results by polling each task
    - Batch processing pattern with KubeMQ queues

Usage:
    python examples/async_inference/batch_enqueue.py

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

BATCH_SIZE = 5


def score(text: str) -> dict:
    """Simulated sentiment scoring."""
    time.sleep(0.1)
    return {"text": text, "score": 0.85}


def main():
    channel = f"example-async-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(score, name="score")
    adapter.start_consumer()

    try:
        # Enqueue N tasks in a loop
        inputs = [f"Review {i}" for i in range(BATCH_SIZE)]
        task_ids = []

        print(f"Enqueuing {BATCH_SIZE} tasks...")
        for text in inputs:
            result = adapter.enqueue_task_sync("score", args=[text])
            task_ids.append(result.id)
            print(f"  enqueued task_id={result.id}")

        # Poll for all results
        print("\nPolling for results...")
        results = {}
        for attempt in range(60):
            all_done = True
            for task_id in task_ids:
                if task_id in results:
                    continue
                status = adapter.get_task_status_sync(task_id)
                if status.status in ("SUCCESS", "FAILURE"):
                    results[task_id] = status
                    print(f"  task {task_id}: status={status.status} result={status.result}")
                else:
                    all_done = False
            if all_done:
                break
            time.sleep(0.5)

        print(f"\nCompleted {len(results)}/{BATCH_SIZE} tasks.")
        if len(results) < BATCH_SIZE:
            print("  Some tasks did not complete in time.")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
