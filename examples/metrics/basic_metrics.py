"""
Basic Metrics

Demonstrates:
    - get_metrics() after processing tasks
    - All 8 metric keys returned by the adapter
    - Counter, gauge, and histogram metric types

Usage:
    python examples/metrics/basic_metrics.py

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


def compute(x: int) -> int:
    """Simulated computation with variable delay."""
    time.sleep(0.05 * (x % 3 + 1))
    return x * x


def main():
    channel = f"example-metrics-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = f"{channel}.dlq"
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(compute, name="compute")
    adapter.start_consumer()

    try:
        # Enqueue and process several tasks
        task_ids = []
        for i in range(5):
            result = adapter.enqueue_task_sync("compute", args=[i])
            task_ids.append(result.id)
            print(f"Enqueued task {i + 1}/5: {result.id}")

        # Wait for all tasks to complete
        print("\nWaiting for tasks to complete...")
        for task_id in task_ids:
            for _ in range(30):
                status = adapter.get_task_status_sync(task_id)
                if status.status in ("SUCCESS", "FAILURE"):
                    break
                time.sleep(0.5)

        # Retrieve and display all metrics
        metrics = adapter.get_metrics()

        print("\n--- All 8 Metrics ---")
        print(f"  queue_depth:                       {metrics['queue_depth']}")
        print(f"  in_flight:                         {metrics['in_flight']}")
        print(f"  dlq_depth:                         {metrics['dlq_depth']}")
        print(f"  tasks_enqueued_total:              {metrics['tasks_enqueued_total']}")
        print(f"  tasks_completed_total:             {metrics['tasks_completed_total']}")
        print(f"  task_processing_duration_seconds:  {metrics['task_processing_duration_seconds']}")
        print(f"  result_storage_retries_total:      {metrics['result_storage_retries_total']}")
        print(
            f"  consumer_poll_latency_seconds:     {metrics['consumer_poll_latency_seconds']:.4f}"
        )

        print("\nMetric types:")
        print("  Gauges:     queue_depth, in_flight, dlq_depth, consumer_poll_latency_seconds")
        print(
            "  Counters:   tasks_enqueued_total, tasks_completed_total, "
            "result_storage_retries_total"
        )
        print("  Histogram:  task_processing_duration_seconds (min/max/avg/count)")
    finally:
        adapter.stop_consumer()

    print("\nExample complete.")


if __name__ == "__main__":
    main()
