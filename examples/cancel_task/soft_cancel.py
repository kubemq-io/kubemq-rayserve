"""
Soft Cancel — Cancel a Task and Overwrite Result With CANCELLED

Demonstrates:
    - Enqueuing a long-running task
    - Cancelling the task with cancel_task()
    - The result is overwritten with CANCELLED status
    - cancel_task() returns True on success

Usage:
    python examples/cancel_task/soft_cancel.py

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
    """Slow handler to give time for cancellation."""
    time.sleep(10)
    return {"data": data, "completed": True}


def main():
    channel = f"example-cancel-{uuid.uuid4().hex[:8]}"

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
        # Enqueue a long-running task
        print("Enqueuing a slow task (handler takes 10 seconds)...")
        result = adapter.enqueue_task_sync("slow_handler", args=["work-item"])
        task_id = result.id
        print(f"  task_id={task_id} status={result.status}")

        # Cancel the task before it completes
        print("\nCancelling the task...")
        cancelled = adapter.cancel_task(task_id)
        print(f"  cancel_task() returned: {cancelled}")

        # Check the status after cancellation
        print("\nChecking task status after cancellation:")
        status = adapter.get_task_status_sync(task_id)
        print(f"  status={status.status}")
        if status.result:
            print(f"  result={status.result}")

        if status.status == "CANCELLED":
            print("\n  Task was successfully cancelled. Result overwritten with CANCELLED.")
        else:
            print(f"\n  Task status is {status.status} (may have completed before cancel arrived).")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
