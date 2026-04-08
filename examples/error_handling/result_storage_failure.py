"""
Result Storage Failure — Graceful Degradation for Large Results

Demonstrates:
    - A handler producing a very large result that may exceed max_send_size
    - The adapter logs a warning but does not crash
    - Task status shows SUCCESS but the result may be None (in-memory fallback)
    - Pattern for handling result storage limitations

Usage:
    python examples/error_handling/result_storage_failure.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve

Notes:
    The default max_send_size is 4MB (4_194_304 bytes). When a result
    exceeds this limit, the adapter degrades gracefully:
      - The task is marked SUCCESS (the handler completed without error)
      - The result may be None or truncated in the result backend
      - A warning is logged indicating the result could not be stored
    This prevents a successful computation from being marked as failed
    just because the result was too large to transmit.
"""

from __future__ import annotations

import os
import time
import uuid

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")


def small_result_handler(data: str) -> dict:
    """Returns a small result that fits within message size limits."""
    return {"data": data, "size": "small"}


def large_result_handler(data: str) -> dict:
    """Returns a very large result that may exceed max_send_size."""
    # Generate a ~5MB string to exceed the default 4MB limit
    large_payload = "x" * (5 * 1024 * 1024)
    return {"data": data, "payload": large_payload}


def main():
    channel = f"example-error-{uuid.uuid4().hex[:8]}"

    # Configure with a small max_send_size to demonstrate the behavior
    config = KubeMQAdapterConfig(
        address=BROKER,
        max_send_size=1_048_576,  # 1MB limit for demonstration
    )
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(small_result_handler, name="small_result_handler")
    adapter.register_task_handler(large_result_handler, name="large_result_handler")
    adapter.start_consumer()

    try:
        # --- Case 1: Small result (fits within limit) ---
        print("Case 1: Handler with small result (fits within 1MB limit)...")
        result1 = adapter.enqueue_task_sync("small_result_handler", args=["test"])
        print(f"  task_id={result1.id}")

        for _ in range(30):
            status1 = adapter.get_task_status_sync(result1.id)
            if status1.status in ("SUCCESS", "FAILURE"):
                print(f"  status={status1.status} result={status1.result}")
                break
            time.sleep(0.5)

        # --- Case 2: Large result (exceeds limit) ---
        print("\nCase 2: Handler with large result (~5MB, exceeds 1MB limit)...")
        result2 = adapter.enqueue_task_sync("large_result_handler", args=["test"])
        print(f"  task_id={result2.id}")

        for _ in range(30):
            status2 = adapter.get_task_status_sync(result2.id)
            if status2.status in ("SUCCESS", "FAILURE"):
                print(f"  status={status2.status}")
                if status2.result is None:
                    print("  result=None (result too large for storage — graceful degradation)")
                else:
                    if isinstance(status2.result, dict):
                        keys = list(status2.result.keys())
                        print(f"  result keys={keys}")
                    else:
                        print(f"  result type={type(status2.result)}")
                break
            time.sleep(0.5)
        else:
            print("  Timed out waiting for result.")

        print("\nKey takeaway: The adapter does not crash when results exceed max_send_size.")
        print("The handler's work is preserved (SUCCESS) even if the result cannot be stored.")
        print("For large results, consider writing to external storage and returning a reference.")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
