"""
Custom Address — Connect to a Non-Default Broker

Demonstrates:
    - Overriding the default broker address
    - Connecting to a remote or custom-port KubeMQ broker
    - Using KUBEMQ_ADDRESS environment variable as fallback

Usage:
    python examples/connection/custom_address.py

Requirements:
    - KubeMQ broker running on the configured address
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

    # Override with a custom address (falls back to BROKER env var for this example)
    custom_address = os.environ.get("KUBEMQ_CUSTOM_ADDRESS", BROKER)
    config = KubeMQAdapterConfig(address=custom_address)
    print(f"Connecting to custom address: {config.address}")

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

        if healthy:
            print("Successfully connected to custom broker address.")
        else:
            print("Connection failed. Verify the broker is running at the specified address.")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
