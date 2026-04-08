"""
Custom Tasks Per Replica

Demonstrates:
    - Varying tasks_per_replica parameter to tune scaling sensitivity
    - How different values change desired replica count for the same queue depth
    - Formula: desired_replicas = max(1, ceil(queue_depth / tasks_per_replica))

Usage:
    python examples/autoscaling/custom_tasks_per_replica.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

import math
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

    # Set up adapter and enqueue tasks to create queue depth
    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())

    num_tasks = 20
    for i in range(num_tasks):
        adapter.enqueue_task_sync("dummy_handler", args=[i])
    print(f"Enqueued {num_tasks} tasks to channel: {channel}")

    # Mock Ray Serve autoscaling context
    class _MockContext:
        current_num_replicas = 1

    ctxs = {"deployment-1": _MockContext()}

    # Test with different tasks_per_replica values
    test_values = [1, 3, 5, 10, 20, 50]

    print(f"\nScaling math for queue_depth={num_tasks}:")
    print(f"{'tasks_per_replica':>20} | {'desired_replicas':>18} | formula")
    print("-" * 70)

    for tpr in test_values:
        decisions, state = kubemq_queue_depth_policy(
            ctxs=ctxs,
            kubemq_address=BROKER,
            queue_name=channel,
            tasks_per_replica=tpr,
        )
        depth = state.get("queue_depth", num_tasks)
        desired = state.get("desired_replicas", "?")
        expected = max(1, math.ceil(num_tasks / tpr))
        print(f"{tpr:>20} | {desired:>18} | max(1, ceil({depth}/{tpr})) = {expected}")

    print("\nKey takeaways:")
    print("  - Lower tasks_per_replica = more aggressive scaling (more replicas)")
    print("  - Higher tasks_per_replica = conservative scaling (fewer replicas)")
    print("  - Value of 1 = one replica per pending task (maximum parallelism)")

    # Cleanup
    try:
        adapter.stop_consumer()
    except Exception:
        pass

    print("\nExample complete.")


if __name__ == "__main__":
    main()
