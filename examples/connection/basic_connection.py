"""
Basic Connection — Default localhost:50000

Demonstrates:
    - Default KubeMQAdapterConfig connection to localhost:50000
    - Adapter initialization and health check
    - Verifying broker connectivity with ping

Usage:
    python examples/connection/basic_connection.py

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

    # Default config connects to localhost:50000
    config = KubeMQAdapterConfig(address=BROKER)
    print(f"Config address: {config.address}")
    print(f"Config client_id: {config.client_id!r} (auto-generated if empty)")

    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())

    try:
        # Verify connectivity
        healthy = adapter.health_check()
        print(f"Health check: {healthy}")

        detailed = adapter.health_check_sync()
        print(f"Detailed health: {detailed}")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
