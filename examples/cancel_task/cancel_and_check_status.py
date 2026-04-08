"""
Cancel and Check Status — Observe CANCELLED Status Transition

Demonstrates:
    - Enqueuing a long-running task
    - Polling to confirm PENDING status
    - Cancelling the task
    - Polling again to confirm CANCELLED status
    - Full status lifecycle: PENDING -> CANCELLED

Usage:
    python examples/cancel_task/cancel_and_check_status.py

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
    """Slow handler to give time for status transitions."""
    time.sleep(15)
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
        # Step 1: Enqueue a long-running task
        print("Step 1: Enqueue a slow task (handler takes 15 seconds)...")
        result = adapter.enqueue_task_sync("slow_handler", args=["work-item"])
        task_id = result.id
        print(f"  task_id={task_id}")

        # Step 2: Poll to see initial status
        print("\nStep 2: Check initial status...")
        status = adapter.get_task_status_sync(task_id)
        print(f"  status={status.status}")

        # Step 3: Cancel the task
        print("\nStep 3: Cancel the task...")
        cancelled = adapter.cancel_task(task_id)
        print(f"  cancel_task() returned: {cancelled}")

        # Step 4: Poll to confirm CANCELLED status
        print("\nStep 4: Poll for CANCELLED status...")
        for i in range(10):
            status = adapter.get_task_status_sync(task_id)
            print(f"  poll {i + 1}: status={status.status}")
            if status.status == "CANCELLED":
                print("  Task confirmed as CANCELLED.")
                break
            if status.status in ("SUCCESS", "FAILURE"):
                print(f"  Task reached {status.status} before cancel took effect.")
                break
            time.sleep(0.5)
        else:
            print("  Status did not transition to CANCELLED within polling window.")

        # Summary
        print("\nStatus transition summary:")
        print(f"  Initial: {result.status}")
        print(f"  After cancel: {status.status}")
        if status.status == "CANCELLED":
            print("  The cancel_task() call successfully overwrote the task result.")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
