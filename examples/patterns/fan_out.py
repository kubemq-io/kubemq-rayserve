"""
Fan-Out Pattern — One Task Triggers N Downstream Tasks

Demonstrates:
    - A coordinator adapter that receives a task and fans out to N worker channels
    - Each downstream channel processes its task independently
    - No result gathering (fire-and-forget fan-out)

Usage:
    python examples/patterns/fan_out.py

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

NUM_DOWNSTREAM = 3


def process_notification(channel: str, message: str) -> dict:
    """Simulated downstream processing (e.g., email, SMS, push)."""
    time.sleep(0.1)  # Simulate work
    return {"channel": channel, "message": message, "delivered": True}


def main():
    run_id = uuid.uuid4().hex[:8]

    # --- 1. Create downstream worker adapters ---
    downstream: list[tuple[str, KubeMQTaskProcessorAdapter]] = []
    for i in range(NUM_DOWNSTREAM):
        channel = f"example-fanout-{run_id}-downstream-{i}"
        config = KubeMQAdapterConfig(address=BROKER)
        adapter = KubeMQTaskProcessorAdapter(config)

        class _Cfg:
            queue_name = channel
            max_retries = 1
            failed_task_queue_name = f"{channel}.dlq"
            unprocessable_task_queue_name = ""

        adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
        adapter.register_task_handler(process_notification, name="process_notification")
        adapter.start_consumer()
        downstream.append((channel, adapter))
        print(f"  Downstream {i} started on channel={channel}")

    try:
        # --- 2. Fan-out: coordinator enqueues to all downstream channels ---
        notification_types = ["email", "sms", "push"]
        original_message = "Order #12345 has been shipped"

        print(f"\nFanning out message to {NUM_DOWNSTREAM} downstream channels...")
        task_ids: list[tuple[str, int]] = []
        for i, (channel, adapter) in enumerate(downstream):
            result = adapter.enqueue_task_sync(
                "process_notification",
                kwargs={"channel": notification_types[i], "message": original_message},
            )
            task_ids.append((result.id, i))
            print(f"  Fanned out to downstream {i} ({notification_types[i]}): task_id={result.id}")

        # --- 3. Optionally verify delivery (not required for pure fan-out) ---
        print("\nVerifying delivery (optional for fan-out)...")
        time.sleep(2)  # Allow processing time
        for task_id, idx in task_ids:
            _, adapter = downstream[idx]
            status = adapter.get_task_status_sync(task_id)
            print(f"  Downstream {idx} ({notification_types[idx]}): status={status.status}")

        print("\nFan-Out complete:")
        print(f"  Downstream channels: {NUM_DOWNSTREAM}")
        print(f"  Tasks dispatched: {len(task_ids)}")

    finally:
        # --- 4. Cleanup: stop all downstream adapters ---
        for channel, adapter in downstream:
            adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
