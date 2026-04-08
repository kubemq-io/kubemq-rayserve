"""
Sync Hello World — Synchronous Inference via KubeMQ Queries

Demonstrates:
    - Synchronous request-response inference using query_task_sync()
    - No polling loop needed — result returned immediately
    - Minimal sync inference setup

Usage:
    python examples/quickstart/sync_hello_world.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

import os
import uuid

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")


def greet(name: str) -> dict:
    return {"greeting": f"Hello, {name}!"}


def main():
    channel = f"example-quickstart-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER, sync_inference_timeout=10)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(greet, name="greet")
    adapter.start_consumer()

    try:
        print("Sending sync inference query...")
        result = adapter.query_task_sync("greet", args=["World"], timeout=10)
        print(f"  status={result.status}")
        print(f"  result={result.result}")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
