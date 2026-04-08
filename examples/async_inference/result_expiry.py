"""
Result Expiry — Async Queue-Based Inference

Demonstrates:
    - Configuring result_expiry_seconds=5 for short-lived results
    - Polling result immediately after completion (SUCCESS)
    - Waiting past the TTL and showing the result is gone

Usage:
    python examples/async_inference/result_expiry.py

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


def quick_task(value: str) -> dict:
    """A fast handler that returns immediately."""
    return {"processed": value}


def main():
    channel = f"example-async-{uuid.uuid4().hex[:8]}"

    # Configure with short result expiry (5 seconds)
    config = KubeMQAdapterConfig(
        address=BROKER,
        result_expiry_seconds=5,
    )
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(quick_task, name="quick_task")
    adapter.start_consumer()

    try:
        print("Enqueuing task (result_expiry_seconds=5)...")
        result = adapter.enqueue_task_sync("quick_task", args=["test-data"])
        task_id = result.id
        print(f"  task_id={task_id}")

        # Poll until complete
        print("\nWaiting for task to complete...")
        for i in range(30):
            status = adapter.get_task_status_sync(task_id)
            if status.status in ("SUCCESS", "FAILURE"):
                print(f"  Task completed: status={status.status} result={status.result}")
                break
            time.sleep(0.5)
        else:
            print("  Timed out waiting for completion.")

        # Fetch result again immediately — should still be available
        print("\nFetching result immediately after completion...")
        status = adapter.get_task_status_sync(task_id)
        print(f"  status={status.status} result={status.result}")

        # Wait past the TTL
        print("\nWaiting 6 seconds for result to expire...")
        time.sleep(6)

        # Fetch result again — should be gone (PENDING or not found)
        print("Fetching result after TTL expiry...")
        status = adapter.get_task_status_sync(task_id)
        print(f"  status={status.status} result={status.result}")
        print("  (Result should be expired/gone after TTL)")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
