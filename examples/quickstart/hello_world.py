"""
Hello World — KubeMQ Ray Serve Adapter

Demonstrates:
    - Minimal adapter setup
    - Enqueuing a single task
    - Polling for the result
    - Stopping the consumer

Usage:
    python examples/quickstart/hello_world.py

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


def greet(name: str) -> dict:
    return {"greeting": f"Hello, {name}!"}


def main():
    channel = f"example-quickstart-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER)
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
        result = adapter.enqueue_task_sync("greet", args=["World"])
        print(f"Enqueued: task_id={result.id} status={result.status}")

        for _ in range(30):
            status = adapter.get_task_status_sync(result.id)
            if status.status in ("SUCCESS", "FAILURE"):
                print(f"Result: status={status.status} result={status.result}")
                break
            time.sleep(0.5)
        else:
            print("Timed out waiting for result.")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
