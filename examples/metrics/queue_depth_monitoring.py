"""
Queue Depth Monitoring

Demonstrates:
    - Monitoring queue_depth metric over time
    - Observing depth increase as tasks are enqueued
    - Observing depth decrease as consumer processes tasks
    - Simple time-series tracking of a gauge metric

Usage:
    python examples/metrics/queue_depth_monitoring.py

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


def slow_task(x: int) -> int:
    """Simulated slow processing."""
    time.sleep(0.5)
    return x + 1


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
    adapter.register_task_handler(slow_task, name="slow_task")

    depth_history: list[int] = []

    try:
        # Phase 1: Enqueue tasks WITHOUT consumer — depth should increase
        print("Phase 1: Enqueuing tasks (consumer stopped)...")
        for i in range(8):
            adapter.enqueue_task_sync("slow_task", args=[i])
            metrics = adapter.get_metrics()
            depth = metrics["queue_depth"]
            depth_history.append(depth)
            print(f"  Enqueued {i + 1}/8 — queue_depth: {depth}")

        # Phase 2: Start consumer — depth should decrease
        print("\nPhase 2: Starting consumer (depth should decrease)...")
        adapter.start_consumer()

        for tick in range(12):
            time.sleep(0.5)
            metrics = adapter.get_metrics()
            depth = metrics["queue_depth"]
            depth_history.append(depth)
            print(f"  t={tick * 0.5:.1f}s — queue_depth: {depth}")
            if depth == 0:
                print("  Queue drained!")
                break

        # Summary
        print(f"\nQueue depth over time: {depth_history}")
        print(f"  Peak depth:  {max(depth_history)}")
        print(f"  Final depth: {depth_history[-1]}")
    finally:
        adapter.stop_consumer()

    print("\nExample complete.")


if __name__ == "__main__":
    main()
