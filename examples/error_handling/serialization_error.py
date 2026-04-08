"""
Serialization Error — Non-JSON-Serializable Arguments

Demonstrates:
    - What happens when you pass non-JSON-serializable arguments
    - TypeError raised by enqueue_task_sync
    - How to fix: convert objects before enqueueing

Usage:
    python examples/error_handling/serialization_error.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

import os
import uuid

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")


def process(data: dict) -> dict:
    return {"processed": True}


def main():
    channel = f"example-serial-err-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(process, name="process")
    adapter.start_consumer()

    try:
        # This will fail: set() is not JSON-serializable
        print("Attempting to enqueue with non-serializable args...")
        try:
            adapter.enqueue_task_sync("process", kwargs={"data": {1, 2, 3}})
        except TypeError as exc:
            print(f"  Expected TypeError: {exc}")
            print("  Fix: convert set to list before enqueueing")

        # Fixed version
        print("\nFixed version with list conversion:")
        result = adapter.enqueue_task_sync("process", kwargs={"data": [1, 2, 3]})
        print(f"  Success: task_id={result.id} status={result.status}")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
