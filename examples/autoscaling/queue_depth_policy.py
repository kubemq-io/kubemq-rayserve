"""
Queue Depth Autoscaling Policy

Demonstrates:
    - kubemq_queue_depth_policy() basic usage
    - Mock Ray Serve autoscaling context
    - How queue depth maps to desired replica count
    - Formula: desired_replicas = max(1, ceil(queue_depth / tasks_per_replica))

Usage:
    python examples/autoscaling/queue_depth_policy.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

import os
import uuid

from kubemq_rayserve import (
    KubeMQAdapterConfig,
    KubeMQTaskProcessorAdapter,
    kubemq_queue_depth_policy,
)

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")


def dummy_handler(x: int) -> int:
    return x * 2


def main():
    channel = f"example-autoscale-{uuid.uuid4().hex[:8]}"

    # Set up adapter and enqueue some tasks to create queue depth
    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())

    # Enqueue tasks WITHOUT starting consumer -> builds queue depth
    for i in range(12):
        adapter.enqueue_task_sync("dummy_handler", args=[i])
    print(f"Enqueued 12 tasks to channel: {channel}")

    # Mock Ray Serve autoscaling context
    class _MockContext:
        current_num_replicas = 1

    ctxs = {"deployment-1": _MockContext()}

    # Call the autoscaling policy
    decisions, state = kubemq_queue_depth_policy(
        ctxs=ctxs,
        kubemq_address=BROKER,
        queue_name=channel,
        tasks_per_replica=5,
    )

    print(f"\nQueue depth: {state.get('queue_depth', 'unknown')}")
    print(f"Desired replicas: {state.get('desired_replicas', 'unknown')}")
    print(f"Decisions: {decisions}")
    print("Formula: max(1, ceil(12 / 5)) = 3 replicas")

    # Cleanup
    try:
        adapter.stop_consumer()
    except Exception:
        pass

    print("\nExample complete.")


if __name__ == "__main__":
    main()
