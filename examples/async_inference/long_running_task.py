"""
Long Running Task — Async Queue-Based Inference

Demonstrates:
    - A handler that takes significant time (5 seconds)
    - Polling shows PENDING status before transitioning to SUCCESS
    - Patience in poll loops for slow handlers

Usage:
    python examples/async_inference/long_running_task.py

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


def heavy_computation(data: str) -> dict:
    """Simulated long-running computation."""
    time.sleep(5)  # Simulate expensive processing
    return {"input": data, "result": "processed", "duration_seconds": 5}


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
    adapter.register_task_handler(heavy_computation, name="heavy_computation")
    adapter.start_consumer()

    try:
        print("Enqueuing long-running task (handler sleeps 5s)...")
        start = time.time()
        result = adapter.enqueue_task_sync("heavy_computation", args=["large dataset"])
        print(f"  task_id={result.id}")

        print("\nPolling for result (expect PENDING for ~5 seconds)...")
        for i in range(30):
            status = adapter.get_task_status_sync(result.id)
            elapsed = time.time() - start
            print(f"  poll {i + 1} ({elapsed:.1f}s): status={status.status}")
            if status.status in ("SUCCESS", "FAILURE"):
                print(f"  result={status.result}")
                print(f"  Total time: {elapsed:.1f}s")
                break
            time.sleep(1.0)
        else:
            print("  Timed out waiting for result.")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
