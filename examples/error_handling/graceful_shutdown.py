"""
Graceful Shutdown — stop_consumer() With In-Flight Processing

Demonstrates:
    - Starting a consumer with a slow handler
    - Calling stop_consumer(timeout=10.0) while a task is in-flight
    - In-flight messages are nacked and can be re-delivered
    - Clean shutdown pattern

Usage:
    python examples/error_handling/graceful_shutdown.py

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


def slow_handler(data: str) -> dict:
    """Handler that takes 5 seconds — simulates in-flight processing."""
    print(f"  [handler] Started processing: {data}")
    time.sleep(5)
    print(f"  [handler] Finished processing: {data}")
    return {"result": data, "duration": 5}


def main():
    channel = f"example-shutdown-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(slow_handler, name="slow_handler")
    adapter.start_consumer()

    try:
        # Enqueue a slow task
        print("Enqueuing a slow task (handler takes 5 seconds)...")
        result = adapter.enqueue_task_sync("slow_handler", args=["important-work"])
        print(f"  task_id={result.id}")

        # Give the consumer a moment to pick up the message
        time.sleep(1)

        # Initiate graceful shutdown while the task is likely still in-flight
        print("\nInitiating graceful shutdown with timeout=10.0...")
        print("  (The in-flight task should complete or be nacked)")
    finally:
        adapter.stop_consumer(timeout=10.0)
        print("  Consumer stopped.")

    # Check final status
    print("\nChecking task status after shutdown:")
    # Re-create adapter briefly to query result
    config2 = KubeMQAdapterConfig(address=BROKER)
    adapter2 = KubeMQTaskProcessorAdapter(config2)

    class _Cfg2:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter2.initialize(consumer_concurrency=1, task_processor_config=_Cfg2())
    try:
        status = adapter2.get_task_status_sync(result.id)
        print(f"  status={status.status} result={status.result}")
        if status.status == "SUCCESS":
            print("  Task completed before shutdown finished.")
        else:
            print("  Task was in-flight during shutdown (may be nacked for re-delivery).")
    except Exception as exc:
        print(f"  Could not query status: {exc}")
    finally:
        try:
            adapter2.stop_consumer()
        except Exception:
            pass

    print("Example complete.")


if __name__ == "__main__":
    main()
