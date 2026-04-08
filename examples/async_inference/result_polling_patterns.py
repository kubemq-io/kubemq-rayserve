"""
Result Polling Patterns — Async Queue-Based Inference

Demonstrates:
    - Three polling strategies for retrieving task results
    - Tight loop: poll as fast as possible
    - Exponential backoff: increasing delay between polls
    - Timeout: poll with a hard deadline

Usage:
    python examples/async_inference/result_polling_patterns.py

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


def process(data: str) -> dict:
    """Simulated processing with short delay."""
    time.sleep(0.5)
    return {"data": data, "processed": True}


def poll_tight_loop(adapter: KubeMQTaskProcessorAdapter, task_id: str) -> None:
    """Strategy 1: Tight loop — poll every 0.1s, max 50 attempts."""
    print("\n--- Strategy 1: Tight Loop (0.1s interval, 50 max) ---")
    start = time.time()
    for i in range(50):
        status = adapter.get_task_status_sync(task_id)
        if status.status in ("SUCCESS", "FAILURE"):
            elapsed = time.time() - start
            print(f"  Completed in {i + 1} polls ({elapsed:.2f}s): result={status.result}")
            return
        time.sleep(0.1)
    print("  Timed out.")


def poll_exponential_backoff(adapter: KubeMQTaskProcessorAdapter, task_id: str) -> None:
    """Strategy 2: Exponential backoff — start at 0.1s, double each time, cap at 2s."""
    print("\n--- Strategy 2: Exponential Backoff (0.1s start, 2s cap) ---")
    start = time.time()
    delay = 0.1
    max_delay = 2.0
    attempt = 0

    while True:
        attempt += 1
        status = adapter.get_task_status_sync(task_id)
        if status.status in ("SUCCESS", "FAILURE"):
            elapsed = time.time() - start
            print(f"  Completed in {attempt} polls ({elapsed:.2f}s): result={status.result}")
            return
        if time.time() - start > 30:
            print("  Timed out after 30s.")
            return
        print(f"  poll {attempt}: PENDING, sleeping {delay:.1f}s")
        time.sleep(delay)
        delay = min(delay * 2, max_delay)


def poll_with_timeout(
    adapter: KubeMQTaskProcessorAdapter, task_id: str, timeout: float = 10.0
) -> None:
    """Strategy 3: Timeout — poll with a hard deadline."""
    print(f"\n--- Strategy 3: Timeout ({timeout}s deadline, 0.5s interval) ---")
    start = time.time()
    attempt = 0

    while time.time() - start < timeout:
        attempt += 1
        status = adapter.get_task_status_sync(task_id)
        if status.status in ("SUCCESS", "FAILURE"):
            elapsed = time.time() - start
            print(f"  Completed in {attempt} polls ({elapsed:.2f}s): result={status.result}")
            return
        time.sleep(0.5)

    elapsed = time.time() - start
    print(f"  Timed out after {elapsed:.2f}s ({attempt} polls).")


def main():
    channel = f"example-async-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(process, name="process")
    adapter.start_consumer()

    try:
        # Strategy 1: Tight loop
        print("Enqueuing task for tight-loop polling...")
        r1 = adapter.enqueue_task_sync("process", args=["data-1"])
        poll_tight_loop(adapter, r1.id)

        # Strategy 2: Exponential backoff
        print("\nEnqueuing task for exponential-backoff polling...")
        r2 = adapter.enqueue_task_sync("process", args=["data-2"])
        poll_exponential_backoff(adapter, r2.id)

        # Strategy 3: Timeout
        print("\nEnqueuing task for timeout polling...")
        r3 = adapter.enqueue_task_sync("process", args=["data-3"])
        poll_with_timeout(adapter, r3.id, timeout=10.0)
    finally:
        adapter.stop_consumer()

    print("\nExample complete.")


if __name__ == "__main__":
    main()
