"""
Task with Mixed Args and Kwargs — Async Queue-Based Inference

Demonstrates:
    - Passing both positional args and keyword kwargs together
    - Handler receives both positional and named parameters

Usage:
    python examples/async_inference/task_with_mixed_args.py

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


def search(query: str, max_results: int = 10, language: str = "en") -> dict:
    """Simulated search with positional and keyword arguments."""
    return {
        "query": query,
        "max_results": max_results,
        "language": language,
        "results": [f"Result {i} for '{query}'" for i in range(min(3, max_results))],
    }


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
    adapter.register_task_handler(search, name="search")
    adapter.start_consumer()

    try:
        # Enqueue with both args and kwargs
        print(
            "Enqueuing 'search' with args=['machine learning'], "
            "kwargs={'max_results': 5, 'language': 'en'}..."
        )
        result = adapter.enqueue_task_sync(
            "search",
            args=["machine learning"],
            kwargs={"max_results": 5, "language": "en"},
        )
        print(f"  task_id={result.id}")

        # Poll for result
        print("\nPolling for result...")
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
