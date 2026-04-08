"""
Task with Keyword Args — Async Queue-Based Inference

Demonstrates:
    - Passing keyword arguments via the kwargs parameter
    - Handler receives kwargs as named parameters

Usage:
    python examples/async_inference/task_with_kwargs.py

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


def greet(name: str, greeting: str = "Hello") -> dict:
    """Generate a greeting message."""
    return {"message": f"{greeting}, {name}!"}


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
    adapter.register_task_handler(greet, name="greet")
    adapter.start_consumer()

    try:
        # Enqueue with kwargs only
        print("Enqueuing 'greet' with kwargs={'name': 'Alice', 'greeting': 'Hi'}...")
        result1 = adapter.enqueue_task_sync("greet", kwargs={"name": "Alice", "greeting": "Hi"})
        print(f"  task_id={result1.id}")

        # Enqueue with partial kwargs (uses default greeting)
        print("Enqueuing 'greet' with kwargs={'name': 'Bob'}...")
        result2 = adapter.enqueue_task_sync("greet", kwargs={"name": "Bob"})
        print(f"  task_id={result2.id}")

        # Poll for results
        print("\nPolling for results...")
        pending = {result1.id: "greet(Alice)", result2.id: "greet(Bob)"}
        for attempt in range(30):
            for task_id in list(pending.keys()):
                status = adapter.get_task_status_sync(task_id)
                if status.status in ("SUCCESS", "FAILURE"):
                    print(f"  {pending[task_id]}: status={status.status} result={status.result}")
                    del pending[task_id]
            if not pending:
                break
            time.sleep(0.5)

        if pending:
            print(f"  Timed out waiting for: {list(pending.values())}")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
