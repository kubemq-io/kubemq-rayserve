"""
Custom Client ID — Named Client Identification

Demonstrates:
    - Setting a custom client_id for broker identification
    - Useful for monitoring and debugging multiple workers
    - Client ID appears in KubeMQ broker logs and dashboard

Usage:
    python examples/connection/custom_client_id.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

import os
import uuid

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")


def dummy_handler(x: int) -> int:
    return x * 2


def main():
    channel = f"example-connection-{uuid.uuid4().hex[:8]}"

    # Set a named client_id for this worker
    config = KubeMQAdapterConfig(
        address=BROKER,
        client_id="my-worker-01",
    )
    print(f"Config client_id: {config.client_id}")

    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())

    try:
        healthy = adapter.health_check()
        print(f"Health check: {healthy}")
        print(f"Worker '{config.client_id}' connected successfully.")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
