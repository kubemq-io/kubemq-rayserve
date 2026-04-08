"""
Custom Result Channel Prefix

Demonstrates:
    - Setting result_channel_prefix to a custom value
    - How result channels are named based on the prefix
    - Verifying the prefix is applied to result storage

Usage:
    python examples/config/result_channel_prefix.py

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


def echo(message: str) -> str:
    return f"echo: {message}"


def main():
    channel = f"example-config-{uuid.uuid4().hex[:8]}"
    custom_prefix = "my-custom-results-"

    # Configure with custom result channel prefix
    config = KubeMQAdapterConfig(
        address=BROKER,
        result_channel_prefix=custom_prefix,
    )
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(echo, name="echo")
    adapter.start_consumer()

    try:
        print(f"Result channel prefix: {custom_prefix!r}")
        print(f"Task queue channel:    {channel}")
        print(f"Result channels will be named: {custom_prefix}<task_id>\n")

        # Enqueue a task and get its result
        result = adapter.enqueue_task_sync("echo", args=["hello"])
        task_id = result.id
        print(f"Enqueued task: {task_id}")
        print(f"Expected result channel: {custom_prefix}{task_id}")

        # Poll for result
        for _ in range(30):
            status = adapter.get_task_status_sync(task_id)
            if status.status in ("SUCCESS", "FAILURE"):
                print(f"\nTask status: {status.status}")
                print(f"Task result: {status.result}")
                break
            time.sleep(0.5)

        # Compare with default prefix
        print("\nDefault prefix:  'rayserve-result-'")
        print(f"Custom prefix:   {custom_prefix!r}")
        print("Use custom prefixes to namespace results by environment or service.")
    finally:
        adapter.stop_consumer()

    print("\nExample complete.")


if __name__ == "__main__":
    main()
