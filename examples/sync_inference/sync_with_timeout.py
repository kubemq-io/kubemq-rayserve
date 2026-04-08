"""
Sync With Timeout — Custom Timeout for Synchronous Inference

Demonstrates:
    - Setting a custom timeout for query_task_sync()
    - Short timeout for fast handlers
    - Long timeout for slow handlers
    - Timeout parameter overrides default sync_inference_timeout

Usage:
    python examples/sync_inference/sync_with_timeout.py

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


def fast_handler(text: str) -> dict:
    """Handler that completes quickly."""
    return {"echo": text, "speed": "fast"}


def slow_handler(text: str) -> dict:
    """Handler that takes a few seconds."""
    time.sleep(2)
    return {"echo": text, "speed": "slow"}


def main():
    channel = f"example-sync-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER, sync_inference_timeout=30)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(fast_handler, name="fast_handler")
    adapter.register_task_handler(slow_handler, name="slow_handler")
    adapter.start_consumer()

    try:
        # Short timeout for fast handler
        print("Sync query with 5s timeout (fast handler)...")
        t0 = time.time()
        result = adapter.query_task_sync(
            task_name="fast_handler",
            args=["hello"],
            timeout=5,
        )
        elapsed = time.time() - t0
        print(f"  status={result.status}")
        print(f"  result={result.result}")
        print(f"  elapsed={elapsed:.2f}s")

        # Longer timeout for slow handler
        print("\nSync query with 10s timeout (slow handler)...")
        t0 = time.time()
        result = adapter.query_task_sync(
            task_name="slow_handler",
            args=["world"],
            timeout=10,
        )
        elapsed = time.time() - t0
        print(f"  status={result.status}")
        print(f"  result={result.result}")
        print(f"  elapsed={elapsed:.2f}s")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
