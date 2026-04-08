"""
Monitor Scaling Decisions

Demonstrates:
    - Periodic polling of kubemq_queue_depth_policy()
    - Logging scaling decisions over time as queue depth changes
    - Simulating a consumer draining tasks while monitoring

Usage:
    python examples/autoscaling/monitor_scaling_decisions.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

import os
import time
import uuid

from kubemq_rayserve import (
    KubeMQAdapterConfig,
    KubeMQTaskProcessorAdapter,
    kubemq_queue_depth_policy,
)

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")


def slow_handler(x: int) -> int:
    """Simulated processing with delay."""
    time.sleep(0.3)
    return x * 2


def main():
    channel = f"example-autoscale-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(slow_handler, name="slow_handler")

    # Enqueue tasks to build initial queue depth
    num_tasks = 15
    for i in range(num_tasks):
        adapter.enqueue_task_sync("slow_handler", args=[i])
    print(f"Enqueued {num_tasks} tasks to channel: {channel}")

    # Mock Ray Serve autoscaling context
    class _MockContext:
        current_num_replicas = 1

    ctxs = {"deployment-1": _MockContext()}

    # Start consumer so tasks drain over time
    adapter.start_consumer()
    print("Consumer started — tasks will drain gradually.\n")

    # Monitor scaling decisions periodically
    print(f"{'Time':>6} | {'Queue Depth':>12} | {'Desired Replicas':>18} | Decision")
    print("-" * 65)

    try:
        for tick in range(10):
            decisions, state = kubemq_queue_depth_policy(
                ctxs=ctxs,
                kubemq_address=BROKER,
                queue_name=channel,
                tasks_per_replica=5,
            )

            depth = state.get("queue_depth", 0)
            desired = state.get("desired_replicas", 1)
            current = _MockContext.current_num_replicas

            if desired > current:
                action = f"SCALE UP ({current} -> {desired})"
            elif desired < current:
                action = f"SCALE DOWN ({current} -> {desired})"
            else:
                action = "NO CHANGE"

            print(f"{tick:>5}s | {depth:>12} | {desired:>18} | {action}")

            # Simulate Ray Serve applying the decision
            _MockContext.current_num_replicas = desired

            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\nMonitoring interrupted.")
    finally:
        adapter.stop_consumer()

    print("\nExample complete.")


if __name__ == "__main__":
    main()
