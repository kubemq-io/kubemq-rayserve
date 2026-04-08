"""
Autoscaling with Authentication

Demonstrates:
    - kubemq_queue_depth_policy() with auth_token parameter
    - Authenticated broker connection for autoscaling
    - Fallback behavior when auth fails

Usage:
    python examples/autoscaling/autoscaling_with_auth.py

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
AUTH_TOKEN = os.environ.get("KUBEMQ_AUTH_TOKEN", "")


def dummy_handler(x: int) -> int:
    return x * 2


def main():
    channel = f"example-autoscale-{uuid.uuid4().hex[:8]}"

    # Set up adapter (with optional auth) and enqueue tasks
    config = KubeMQAdapterConfig(address=BROKER, auth_token=AUTH_TOKEN)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())

    for i in range(8):
        adapter.enqueue_task_sync("dummy_handler", args=[i])
    print(f"Enqueued 8 tasks to channel: {channel}")

    # Mock Ray Serve autoscaling context
    class _MockContext:
        current_num_replicas = 2

    ctxs = {"deployment-1": _MockContext()}

    # --- Call with auth_token (matches broker config) ---
    print("\n--- Policy with auth_token ---")
    decisions, state = kubemq_queue_depth_policy(
        ctxs=ctxs,
        kubemq_address=BROKER,
        queue_name=channel,
        tasks_per_replica=5,
        auth_token=AUTH_TOKEN,
    )
    print(f"Queue depth: {state.get('queue_depth', 'unknown')}")
    print(f"Desired replicas: {state.get('desired_replicas', 'unknown')}")
    print(f"Decisions: {decisions}")

    # --- Call with invalid auth_token (demonstrates fallback) ---
    print("\n--- Policy with invalid auth_token (fallback) ---")
    decisions_bad, state_bad = kubemq_queue_depth_policy(
        ctxs=ctxs,
        kubemq_address=BROKER,
        queue_name=channel,
        tasks_per_replica=5,
        auth_token="invalid-token-for-demo",
    )
    print(f"State: {state_bad}")
    print(f"Decisions: {decisions_bad}")
    print("Note: On auth failure, policy returns current replica counts unchanged.")

    # Cleanup
    try:
        adapter.stop_consumer()
    except Exception:
        pass

    print("\nExample complete.")


if __name__ == "__main__":
    main()
