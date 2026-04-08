"""
Sync Inference Timeout Configuration

Demonstrates:
    - sync_inference_timeout as default timeout for all sync queries
    - How the config-level default applies when no per-call timeout is given
    - Overriding the default with a per-call timeout

Usage:
    python examples/config/sync_inference_timeout.py

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


def fast_predict(x: int) -> dict:
    """Fast prediction — completes quickly."""
    time.sleep(0.1)
    return {"prediction": x * 2, "model": "fast-v1"}


def slow_predict(x: int) -> dict:
    """Slow prediction — takes a while."""
    time.sleep(2.0)
    return {"prediction": x * 3, "model": "slow-v1"}


def main():
    channel = f"example-config-{uuid.uuid4().hex[:8]}"

    # Configure with a custom default sync inference timeout
    custom_timeout = 10  # seconds (default is 30)

    config = KubeMQAdapterConfig(
        address=BROKER,
        sync_inference_timeout=custom_timeout,
    )
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(fast_predict, name="fast_predict")
    adapter.register_task_handler(slow_predict, name="slow_predict")
    adapter.start_consumer()

    try:
        print(f"sync_inference_timeout: {custom_timeout}s (default is 30s)")
        print("This timeout applies to all query_task_sync() calls unless overridden.\n")

        # Test 1: Fast query — uses config default timeout
        print("Test 1: Fast query with config default timeout...")
        start = time.time()
        try:
            result = adapter.query_task_sync("fast_predict", args=[5])
            elapsed = time.time() - start
            print(f"  Result: {result.result}")
            print(f"  Elapsed: {elapsed:.3f}s (within {custom_timeout}s timeout)")
        except Exception as e:
            print(f"  Error: {e}")

        # Test 2: Slow query — uses config default timeout (should succeed)
        print("\nTest 2: Slow query with config default timeout...")
        start = time.time()
        try:
            result = adapter.query_task_sync("slow_predict", args=[5])
            elapsed = time.time() - start
            print(f"  Result: {result.result}")
            print(f"  Elapsed: {elapsed:.3f}s (within {custom_timeout}s timeout)")
        except Exception as e:
            print(f"  Error: {e}")

        # Test 3: Per-call timeout override
        print("\nTest 3: Slow query with per-call timeout=1s (should timeout)...")
        start = time.time()
        try:
            result = adapter.query_task_sync("slow_predict", args=[5], timeout=1)
            elapsed = time.time() - start
            print(f"  Result: {result.result}")
            print(f"  Elapsed: {elapsed:.3f}s")
        except Exception as e:
            elapsed = time.time() - start
            print(f"  Timeout after {elapsed:.3f}s: {e}")
            print("  Per-call timeout=1s overrides config default of 10s")

        print("\nConfiguration summary:")
        print(f"  Config default (sync_inference_timeout): {custom_timeout}s")
        print("  Per-call override: query_task_sync(..., timeout=N)")
        print("  Per-call timeout takes precedence over config default")
    finally:
        adapter.stop_consumer()

    print("\nExample complete.")


if __name__ == "__main__":
    main()
