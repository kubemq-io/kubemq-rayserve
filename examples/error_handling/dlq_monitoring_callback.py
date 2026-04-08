"""
DLQ Monitoring Callback — Alert When Tasks Exhaust Retries

Demonstrates:
    - on_dlq callback configuration in KubeMQAdapterConfig
    - Callback fires when a task exhausts all retries
    - Pattern for alerting/logging DLQ events

Usage:
    python examples/error_handling/dlq_monitoring_callback.py

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

dlq_events: list[dict] = []


def on_dlq_callback(task_id: str, error: str) -> None:
    """Called when a task exhausts retries and moves to DLQ."""
    print(f"  [DLQ ALERT] task_id={task_id} error={error}")
    dlq_events.append({"task_id": task_id, "error": error})


def always_fails(data: str) -> dict:
    """Handler that always raises to demonstrate DLQ routing."""
    raise RuntimeError(f"Simulated failure for: {data}")


def main():
    channel = f"example-dlq-cb-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(
        address=BROKER,
        on_dlq=on_dlq_callback,
    )
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
        print("Enqueuing task that will always fail...")
        result = adapter.enqueue_task_sync("always_fails", args=["test-data"])
        print(f"  task_id={result.id}")

        print("Waiting for retries to exhaust...")
        time.sleep(10)

        print(f"\nDLQ events received: {len(dlq_events)}")
        for evt in dlq_events:
            print(f"  {evt}")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
