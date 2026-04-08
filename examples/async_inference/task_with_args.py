"""
Task with Positional Args — Async Queue-Based Inference

Demonstrates:
    - Passing positional arguments via the args parameter
    - Handler receives args as positional parameters

Usage:
    python examples/async_inference/task_with_args.py

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


def add(a: int, b: int) -> dict:
    """Add two numbers."""
    return {"sum": a + b}


def concat(first: str, second: str) -> dict:
    """Concatenate two strings."""
    return {"result": f"{first} {second}"}


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
    adapter.register_task_handler(add, name="add")
    adapter.register_task_handler(concat, name="concat")
    adapter.start_consumer()

    try:
        # Enqueue with integer args
        print("Enqueuing 'add' with args=[3, 7]...")
        result1 = adapter.enqueue_task_sync("add", args=[3, 7])
        print(f"  task_id={result1.id}")

        # Enqueue with string args
        print("Enqueuing 'concat' with args=['hello', 'world']...")
        result2 = adapter.enqueue_task_sync("concat", args=["hello", "world"])
        print(f"  task_id={result2.id}")

        # Poll for both results
        print("\nPolling for results...")
        pending = {result1.id: "add", result2.id: "concat"}
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
