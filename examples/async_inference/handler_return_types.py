"""
Handler Return Types — Async Queue-Based Inference

Demonstrates:
    - Handlers returning different types: dict, list, str, int, None
    - All return types are serialized and stored as task results

Usage:
    python examples/async_inference/handler_return_types.py

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


def return_dict() -> dict:
    """Handler that returns a dict."""
    return {"key": "value", "nested": {"a": 1}}


def return_list() -> list:
    """Handler that returns a list."""
    return [1, 2, 3, "four", 5.0]


def return_str() -> str:
    """Handler that returns a string."""
    return "hello from handler"


def return_int() -> int:
    """Handler that returns an integer."""
    return 42


def return_none() -> None:
    """Handler that returns None."""
    return None


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
    adapter.register_task_handler(return_dict, name="return_dict")
    adapter.register_task_handler(return_list, name="return_list")
    adapter.register_task_handler(return_str, name="return_str")
    adapter.register_task_handler(return_int, name="return_int")
    adapter.register_task_handler(return_none, name="return_none")
    adapter.start_consumer()

    try:
        handlers = ["return_dict", "return_list", "return_str", "return_int", "return_none"]
        task_ids = {}

        print("Enqueuing tasks for each return type...")
        for name in handlers:
            result = adapter.enqueue_task_sync(name)
            task_ids[result.id] = name
            print(f"  {name}: task_id={result.id}")

        # Poll for all results
        print("\nPolling for results...")
        completed = {}
        for attempt in range(30):
            for task_id in list(task_ids.keys()):
                if task_id in completed:
                    continue
                status = adapter.get_task_status_sync(task_id)
                if status.status in ("SUCCESS", "FAILURE"):
                    completed[task_id] = status
                    handler_name = task_ids[task_id]
                    print(f"  {handler_name}: status={status.status}")
                    print(f"    result={status.result} (type: {type(status.result).__name__})")
            if len(completed) == len(handlers):
                break
            time.sleep(0.5)

        print(f"\nCompleted {len(completed)}/{len(handlers)} tasks.")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
