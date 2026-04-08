"""
Consumer Poll Timeout Tuning

Demonstrates:
    - consumer_poll_timeout_seconds configuration
    - Effect of shorter vs longer poll timeouts
    - Trade-off between responsiveness and broker load

Usage:
    python examples/config/consumer_poll_timeout.py

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


def quick_task(x: int) -> int:
    return x + 1


def run_with_poll_timeout(poll_timeout: int) -> float:
    """Run a task with a given poll timeout and measure time to result."""
    channel = f"example-config-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(
        address=BROKER,
        consumer_poll_timeout_seconds=poll_timeout,
    )
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(quick_task, name="quick_task")
    adapter.start_consumer()

    try:
        start = time.time()
        result = adapter.enqueue_task_sync("quick_task", args=[42])

        for _ in range(60):
            status = adapter.get_task_status_sync(result.id)
            if status.status in ("SUCCESS", "FAILURE"):
                break
            time.sleep(0.1)

        elapsed = time.time() - start
        return elapsed
    finally:
        adapter.stop_consumer()


def main():
    print("Consumer Poll Timeout Comparison")
    print("=" * 50)
    print()
    print("Poll timeout controls how long the consumer waits for")
    print("new messages before polling again.")
    print()

    # Test with different poll timeouts
    timeouts = [1, 5]

    print(f"{'Poll Timeout':>15} | {'Time to Result':>15}")
    print("-" * 35)

    for timeout in timeouts:
        print(f"Testing consumer_poll_timeout_seconds={timeout}...")
        elapsed = run_with_poll_timeout(timeout)
        print(f"{timeout:>14}s | {elapsed:>14.3f}s")

    print()
    print("Key observations:")
    print("  - Shorter timeout (1s): More responsive, slightly higher broker load")
    print("  - Longer timeout (5s):  Less responsive, lower broker load")
    print("  - Default is 1s — suitable for most real-time inference workloads")
    print("  - Use 5-10s for batch/background processing to reduce broker calls")

    print("\nExample complete.")


if __name__ == "__main__":
    main()
