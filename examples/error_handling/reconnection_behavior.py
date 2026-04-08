"""
Reconnection Behavior — SDK Auto-Reconnect During Consume

Demonstrates:
    - Starting a consumer and processing tasks normally
    - SDK automatically reconnects if the broker connection is briefly lost
    - Consumer resumes processing after broker restart

Usage:
    python examples/error_handling/reconnection_behavior.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve

Notes:
    The KubeMQ Python SDK handles reconnection internally (PY-83, PY-116).
    To test reconnection behavior manually:
      1. Run this example
      2. Stop the KubeMQ broker (docker stop kubemq or kill the process)
      3. Observe the consumer logs — the SDK will retry connection
      4. Restart the broker
      5. Enqueue another task — the consumer will resume processing

    During the disconnect window you may see gRPC errors in the logs.
    Once the broker is back, the SDK reconnects transparently.
"""

from __future__ import annotations

import os
import time
import uuid

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")


def process(data: str) -> dict:
    return {"echo": data, "processed_at": time.time()}


def main():
    channel = f"example-reconnect-{uuid.uuid4().hex[:8]}"

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
        # --- Step 1: Normal processing ---
        print("Step 1: Enqueue and process a task normally...")
        result = adapter.enqueue_task_sync("process", args=["before-disconnect"])
        print(f"  Enqueued: task_id={result.id}")

        for _ in range(30):
            status = adapter.get_task_status_sync(result.id)
            if status.status in ("SUCCESS", "FAILURE"):
                print(f"  Result: status={status.status} result={status.result}")
                break
            time.sleep(0.5)

        # --- Step 2: Reconnection window ---
        print("\nStep 2: Reconnection test window")
        print("  To test auto-reconnect:")
        print("    1. Stop the KubeMQ broker NOW")
        print("    2. Wait a few seconds")
        print("    3. Restart the broker")
        print("  (Waiting 5 seconds for you to act, or just let it continue...)")
        time.sleep(5)

        # --- Step 3: Post-reconnect processing ---
        print("\nStep 3: Enqueue another task (after potential reconnect)...")
        try:
            result2 = adapter.enqueue_task_sync("process", args=["after-reconnect"])
            print(f"  Enqueued: task_id={result2.id}")

            for _ in range(30):
                status2 = adapter.get_task_status_sync(result2.id)
                if status2.status in ("SUCCESS", "FAILURE"):
                    print(f"  Result: status={status2.status} result={status2.result}")
                    break
                time.sleep(0.5)
            print("  Consumer successfully processed task after reconnect window.")
        except Exception as exc:
            print(f"  Error (broker may still be down): {type(exc).__name__}: {exc}")
            print("  Restart the broker and retry — SDK will reconnect automatically.")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
