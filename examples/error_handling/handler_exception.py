"""
Handler Exception — Handler Raises, Task Nacked for Retry

Demonstrates:
    - A handler that raises RuntimeError
    - The adapter nacks the message, making it available for retry
    - Task eventually succeeds after transient failure clears
    - Pattern for handlers with transient errors

Usage:
    python examples/error_handling/handler_exception.py

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

# Track invocation count to simulate transient failure
call_count: dict[str, int] = {}


def flaky_handler(data: str) -> dict:
    """Handler that fails on first call but succeeds on retry."""
    count = call_count.get(data, 0) + 1
    call_count[data] = count
    print(f"  [handler] Attempt {count} for: {data}")

    if count < 2:
        raise RuntimeError(f"Transient error on attempt {count} for: {data}")

    return {"data": data, "succeeded_on_attempt": count}


def main():
    channel = f"example-error-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 3
        failed_task_queue_name = f"{channel}.dlq"
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(flaky_handler, name="flaky_handler")
    adapter.start_consumer()

    try:
        print("Enqueuing task with a flaky handler (fails first, succeeds on retry)...")
        result = adapter.enqueue_task_sync("flaky_handler", args=["transient-job"])
        print(f"  task_id={result.id}")

        print("\nPolling for result (expect retry after initial failure)...")
        for i in range(30):
            status = adapter.get_task_status_sync(result.id)
            print(f"  poll {i + 1}: status={status.status}")
            if status.status == "SUCCESS":
                print(f"  result={status.result}")
                print("  Handler succeeded on retry after initial RuntimeError.")
                break
            if status.status == "FAILURE":
                print(f"  error={status.result}")
                print("  Task failed permanently (exhausted retries).")
                break
            time.sleep(1.0)
        else:
            print("  Timed out waiting for result.")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
