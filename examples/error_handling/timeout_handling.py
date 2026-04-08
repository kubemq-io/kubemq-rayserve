"""
Timeout Handling — Sync Query Timeout With Slow Handler

Demonstrates:
    - Using query_task_sync() with a short timeout
    - Handler that takes longer than the timeout
    - Catching the timeout error
    - Choosing an appropriate timeout for your use case

Usage:
    python examples/error_handling/timeout_handling.py

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


def slow_handler(data: str) -> dict:
    """Handler that takes 5 seconds — longer than the query timeout."""
    time.sleep(5)
    return {"data": data, "processed": True}


def fast_handler(data: str) -> dict:
    """Handler that completes quickly — within the query timeout."""
    return {"data": data, "processed": True}


def main():
    channel = f"example-error-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(slow_handler, name="slow_handler")
    adapter.register_task_handler(fast_handler, name="fast_handler")
    adapter.start_consumer()

    try:
        # --- Timeout case: slow handler with short timeout ---
        print("Case 1: Sync query with timeout=1s, but handler takes 5s...")
        try:
            result = adapter.query_task_sync(
                "slow_handler",
                args=["slow-data"],
                timeout=1,
            )
            print(f"  Unexpected success: {result.result}")
        except Exception as exc:
            print(f"  Expected timeout error: {type(exc).__name__}: {exc}")
            print("  The handler takes 5s but timeout is only 1s.")

        # --- Success case: fast handler with adequate timeout ---
        print("\nCase 2: Sync query with timeout=10s, fast handler...")
        try:
            result = adapter.query_task_sync(
                "fast_handler",
                args=["fast-data"],
                timeout=10,
            )
            print(f"  Success: status={result.status} result={result.result}")
        except Exception as exc:
            print(f"  Unexpected error: {exc}")

        print("\nBest practice: Set timeout based on expected handler duration + margin.")
        print("  For fast handlers: timeout=5-10s")
        print("  For slow handlers: timeout=30-60s or use async enqueue instead")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
