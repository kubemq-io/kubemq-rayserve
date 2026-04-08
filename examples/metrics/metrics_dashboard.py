"""
Metrics Dashboard

Demonstrates:
    - Periodic metrics polling in a loop
    - Formatted console output resembling a monitoring dashboard
    - Observing metrics change as tasks are enqueued and processed

Usage:
    python examples/metrics/metrics_dashboard.py

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


def process_item(x: int) -> dict:
    """Simulated processing with variable duration."""
    time.sleep(0.1 * (x % 4 + 1))
    return {"input": x, "result": x * 3}


def main():
    channel = f"example-metrics-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = f"{channel}.dlq"
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=2, task_processor_config=_Cfg())
    adapter.register_task_handler(process_item, name="process_item")
    adapter.start_consumer()

    try:
        # Enqueue a batch of tasks
        num_tasks = 10
        for i in range(num_tasks):
            adapter.enqueue_task_sync("process_item", args=[i])
        print(f"Enqueued {num_tasks} tasks to channel: {channel}\n")

        # Poll metrics periodically and display dashboard
        for tick in range(8):
            metrics = adapter.get_metrics()
            duration = metrics["task_processing_duration_seconds"]

            print(f"=== Metrics Dashboard (t={tick}s) ===")
            print(f"  Queue Depth:      {metrics['queue_depth']:>6}")
            print(f"  In-Flight:        {metrics['in_flight']:>6}")
            print(f"  DLQ Depth:        {metrics['dlq_depth']:>6}")
            print(f"  Enqueued Total:   {metrics['tasks_enqueued_total']:>6}")
            print(f"  Completed Total:  {metrics['tasks_completed_total']}")
            print(
                f"  Duration Stats:   min={duration['min']:.3f}s  max={duration['max']:.3f}s  "
                f"avg={duration['avg']:.3f}s  count={duration['count']}"
            )
            print(f"  Storage Retries:  {metrics['result_storage_retries_total']:>6}")
            print(f"  Poll Latency:     {metrics['consumer_poll_latency_seconds']:.4f}s")
            print()

            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\nDashboard stopped.")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
