"""
Processing Duration Statistics

Demonstrates:
    - task_processing_duration_seconds histogram metric
    - Min, max, avg, and count duration statistics
    - How different workload durations affect the stats

Usage:
    python examples/metrics/processing_duration_stats.py

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


def fast_handler(x: int) -> str:
    """Fast task — ~50ms."""
    time.sleep(0.05)
    return f"fast-{x}"


def medium_handler(x: int) -> str:
    """Medium task — ~200ms."""
    time.sleep(0.2)
    return f"medium-{x}"


def slow_handler(x: int) -> str:
    """Slow task — ~500ms."""
    time.sleep(0.5)
    return f"slow-{x}"


def main():
    channel = f"example-metrics-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(fast_handler, name="fast_handler")
    adapter.register_task_handler(medium_handler, name="medium_handler")
    adapter.register_task_handler(slow_handler, name="slow_handler")
    adapter.start_consumer()

    try:
        # Enqueue varied workloads
        task_ids = []
        workloads = [
            ("fast_handler", 3),
            ("medium_handler", 3),
            ("slow_handler", 2),
        ]

        print("Enqueuing varied workloads...")
        for handler_name, count in workloads:
            for i in range(count):
                result = adapter.enqueue_task_sync(handler_name, args=[i])
                task_ids.append(result.id)
                print(f"  {handler_name}({i}) -> {result.id}")

        # Wait for all tasks to complete
        print("\nWaiting for all tasks to complete...")
        for task_id in task_ids:
            for _ in range(60):
                status = adapter.get_task_status_sync(task_id)
                if status.status in ("SUCCESS", "FAILURE"):
                    break
                time.sleep(0.5)

        # Get duration statistics
        metrics = adapter.get_metrics()
        duration = metrics["task_processing_duration_seconds"]

        print("\n--- Processing Duration Statistics ---")
        print(f"  Total tasks processed: {duration['count']}")
        print(f"  Minimum duration:      {duration['min']:.4f}s")
        print(f"  Maximum duration:      {duration['max']:.4f}s")
        print(f"  Average duration:      {duration['avg']:.4f}s")

        print("\nExpected ranges:")
        print("  - Fast tasks:   ~0.05s each (3 tasks)")
        print("  - Medium tasks: ~0.20s each (3 tasks)")
        print("  - Slow tasks:   ~0.50s each (2 tasks)")
        print(f"  - Expected avg: ~{(3 * 0.05 + 3 * 0.2 + 2 * 0.5) / 8:.3f}s")

        print("\nFull metrics snapshot:")
        print(f"  tasks_enqueued_total:  {metrics['tasks_enqueued_total']}")
        print(f"  tasks_completed_total: {metrics['tasks_completed_total']}")
    finally:
        adapter.stop_consumer()

    print("\nExample complete.")


if __name__ == "__main__":
    main()
