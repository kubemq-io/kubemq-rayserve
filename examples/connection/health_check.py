"""
Health Check — Verify Broker Connectivity

Demonstrates:
    - health_check() convenience method returning True/False
    - health_check_sync() returning detailed health status list
    - Using health checks for readiness probes and monitoring

Usage:
    python examples/connection/health_check.py

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

    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())

    try:
        # Simple boolean health check
        healthy = adapter.health_check()
        print(f"health_check() -> {healthy}")

        # Detailed health check returning list of status dicts
        detailed = adapter.health_check_sync()
        print(f"health_check_sync() -> {detailed}")

        # Use in a readiness probe pattern
        if healthy:
            print("Broker is reachable — ready to process tasks.")
        else:
            print("Broker is NOT reachable — check connection settings.")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
