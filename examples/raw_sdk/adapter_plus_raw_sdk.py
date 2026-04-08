"""
Adapter Plus Raw SDK — Mixing Adapter and Direct SDK Calls

Demonstrates:
    - Using the adapter for standard task enqueue/processing
    - Using QueuesClient directly for queue inspection and monitoring
    - Combining both approaches in a single application
    - When to use each: adapter for business logic, raw SDK for operations

Usage:
    python examples/raw_sdk/adapter_plus_raw_sdk.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

import os
import time
import uuid

from kubemq import ClientConfig, QueuesClient

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")


def classify(text: str) -> dict:
    """Simulated text classification handler."""
    time.sleep(0.1)
    return {"label": "positive", "confidence": 0.95, "text": text}


def main():
    channel = f"example-raw-mixed-{uuid.uuid4().hex[:8]}"

    # --- Set up the adapter for task processing ---
    adapter_config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(adapter_config)

    class _Cfg:
        queue_name = channel
        max_retries = 3
        failed_task_queue_name = f"{channel}.dlq"
        unprocessable_task_queue_name = f"{channel}.unprocessable"

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(classify, name="classify")
    adapter.start_consumer()

    # --- Set up raw SDK client for monitoring ---
    sdk_config = ClientConfig(
        address=BROKER,
        client_id=f"raw-monitor-{uuid.uuid4().hex[:8]}",
    )
    sdk_client = QueuesClient(config=sdk_config)

    try:
        # Enqueue tasks through the adapter
        print("--- Enqueuing tasks via adapter ---")
        task_ids = []
        for i in range(3):
            result = adapter.enqueue_task_sync("classify", kwargs={"text": f"Sample text {i}"})
            task_ids.append(result.id)
            print(f"  Enqueued task: {result.id}")

        # Use raw SDK to inspect queue channels
        print("\n--- Inspecting channels via raw SDK ---")
        channels = sdk_client.list_queues_channels(channel_search=f"{channel}*")
        print(f"Channels matching '{channel}*': {len(channels)}")
        for ch in channels:
            print(f"  {ch.name}: waiting={ch.incoming.waiting}, messages={ch.incoming.messages}")

        # Wait for tasks to complete
        print("\n--- Waiting for tasks to complete ---")
        time.sleep(2)

        # Check task results through the adapter
        for task_id in task_ids:
            status = adapter.get_task_status_sync(task_id)
            print(f"  Task {task_id}: status={status.status}")

        # Use raw SDK to peek at result channels
        print("\n--- Peeking at result channels via raw SDK ---")
        result_channels = sdk_client.list_queues_channels(channel_search=f"{channel}*")
        for ch in result_channels:
            if ch.incoming.waiting > 0:
                result = sdk_client.peek_queue_messages(
                    channel=ch.name,
                    max_messages=5,
                    wait_timeout_in_seconds=2,
                )
                if not result.is_error:
                    print(f"  {ch.name}: {len(result.messages)} messages waiting")

    finally:
        adapter.stop_consumer()
        sdk_client.close()

    print("\nExample complete.")


if __name__ == "__main__":
    main()
