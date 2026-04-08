"""
Basic Enqueue — Async Queue-Based Inference

Demonstrates:
    - Enqueuing a single task to a KubeMQ queue
    - Polling for the result using get_task_status_sync
    - Observing the PENDING -> SUCCESS status transition

Usage:
    python examples/async_inference/basic_enqueue.py

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


def classify(text: str) -> dict:
    """Simulated text classification."""
    time.sleep(0.1)  # Simulate model inference
    return {"label": "positive", "confidence": 0.92}


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
    adapter.register_task_handler(classify, name="classify")
    adapter.start_consumer()

    try:
        print("Enqueuing task...")
        result = adapter.enqueue_task_sync("classify", args=["This is great!"])
        print(f"  task_id={result.id} status={result.status}")

        print("Polling for result...")
        for i in range(30):
            status = adapter.get_task_status_sync(result.id)
            print(f"  poll {i + 1}: status={status.status}")
            if status.status in ("SUCCESS", "FAILURE"):
                print(f"  result={status.result}")
                break
            time.sleep(0.5)
        else:
            print("  Timed out waiting for result.")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
