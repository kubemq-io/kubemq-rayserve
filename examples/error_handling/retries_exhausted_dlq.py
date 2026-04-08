"""
Retries Exhausted DLQ — Max Retries Exceeded, Task Routed to Dead Letter Queue

Demonstrates:
    - Configuring max_retries=2
    - Handler that always raises RuntimeError
    - Task exhausts all retries and is routed to the DLQ
    - Verifying the task ends in FAILURE status

Usage:
    python examples/error_handling/retries_exhausted_dlq.py

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

attempt_count = 0


def always_fails(data: str) -> dict:
    """Handler that always raises to demonstrate DLQ routing."""
    global attempt_count
    attempt_count += 1
    print(f"  [handler] Attempt {attempt_count} — raising RuntimeError")
    raise RuntimeError(f"Permanent failure processing: {data}")


def main():
    global attempt_count
    channel = f"example-error-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 2
        failed_task_queue_name = f"{channel}.dlq"
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(always_fails, name="always_fails")
    adapter.start_consumer()

    try:
        print(f"Config: max_retries=2, DLQ={channel}.dlq")
        print("Enqueuing task with a handler that always fails...")
        result = adapter.enqueue_task_sync("always_fails", args=["doomed-work"])
        print(f"  task_id={result.id}")

        print("\nWaiting for retries to exhaust (up to 15 seconds)...")
        for i in range(30):
            status = adapter.get_task_status_sync(result.id)
            if status.status in ("SUCCESS", "FAILURE"):
                print(f"\n  Final status: {status.status}")
                if status.result:
                    print(f"  Result/error: {status.result}")
                break
            time.sleep(0.5)
        else:
            print("  Timed out waiting for final status.")

        print(f"\nHandler was invoked {attempt_count} time(s).")
        print(f"After exhausting {_Cfg.max_retries} retries, the task is routed to: {channel}.dlq")
        print("In production, monitor the DLQ for failed tasks that need attention.")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
