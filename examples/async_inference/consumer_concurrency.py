"""
Consumer Concurrency — Async Queue-Based Inference

Demonstrates:
    - Setting consumer_concurrency=5 for parallel message processing
    - Enqueuing 10 tasks with slow handlers (1 second each)
    - Measuring total time to show parallelism (~2s vs ~10s serial)

Usage:
    python examples/async_inference/consumer_concurrency.py

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

NUM_TASKS = 10
CONCURRENCY = 5


def slow_handler(task_num: int) -> dict:
    """Handler that takes 1 second to process."""
    time.sleep(1)
    return {"task_num": task_num, "status": "done"}


def main():
    channel = f"example-async-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    # Set consumer_concurrency=5 for parallel processing
    adapter.initialize(consumer_concurrency=CONCURRENCY, task_processor_config=_Cfg())
    adapter.register_task_handler(slow_handler, name="slow_handler")
    adapter.start_consumer()

    try:
        # Enqueue 10 tasks
        print(f"Enqueuing {NUM_TASKS} tasks (each handler sleeps 1s)...")
        task_ids = []
        for i in range(NUM_TASKS):
            result = adapter.enqueue_task_sync("slow_handler", args=[i])
            task_ids.append(result.id)
            print(f"  enqueued task {i}: task_id={result.id}")

        # Time the completion
        print(f"\nProcessing with consumer_concurrency={CONCURRENCY}...")
        print(f"  Expected serial time: ~{NUM_TASKS}s")
        print(f"  Expected parallel time: ~{NUM_TASKS // CONCURRENCY}s")
        start = time.time()

        # Poll for all results
        completed = set()
        for attempt in range(60):
            for task_id in task_ids:
                if task_id in completed:
                    continue
                status = adapter.get_task_status_sync(task_id)
                if status.status in ("SUCCESS", "FAILURE"):
                    completed.add(task_id)
            if len(completed) == NUM_TASKS:
                break
            time.sleep(0.5)

        elapsed = time.time() - start
        print(f"\nCompleted {len(completed)}/{NUM_TASKS} tasks in {elapsed:.1f}s")
        print(f"  Speedup: ~{NUM_TASKS / elapsed:.1f}x (ideal: {CONCURRENCY}x)")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
