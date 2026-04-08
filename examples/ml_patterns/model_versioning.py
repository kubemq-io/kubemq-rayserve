"""
Model Versioning — Hot-Swap Model Versions Without Downtime

Demonstrates:
    - Registering handler v1 and verifying results
    - Overwriting with handler v2 via register_task_handler (same name)
    - Verifying v2 results without stopping the consumer
    - Zero-downtime model version swap

Usage:
    python examples/ml_patterns/model_versioning.py

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


def predict_v1(text: str) -> dict:
    """Model v1 — basic sentiment classifier."""
    # In production: model = load_model("sentiment_v1.bin")
    time.sleep(0.1)  # Simulate inference
    score = len(text) % 10 / 10.0
    return {
        "version": 1,
        "label": "positive" if score > 0.4 else "negative",
        "confidence": round(0.75 + score * 0.1, 3),
    }


def predict_v2(text: str) -> dict:
    """Model v2 — improved sentiment classifier with neutral class."""
    # In production: model = load_model("sentiment_v2.bin")
    time.sleep(0.08)  # Simulate faster v2 inference
    score = len(text) % 10 / 10.0
    if score > 0.6:
        label = "positive"
    elif score > 0.3:
        label = "neutral"
    else:
        label = "negative"
    return {
        "version": 2,
        "label": label,
        "confidence": round(0.80 + score * 0.15, 3),
    }


def poll_result(
    adapter: KubeMQTaskProcessorAdapter, task_id: str, timeout: int = 15
) -> dict | None:
    """Poll until task completes or timeout."""
    start = time.perf_counter()
    while (time.perf_counter() - start) < timeout:
        status = adapter.get_task_status_sync(task_id)
        if status.status in ("SUCCESS", "FAILURE"):
            return {"status": status.status, "result": status.result}
        time.sleep(0.5)
    return None


def main():
    channel = f"example-ml-versioning-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 1
        failed_task_queue_name = f"{channel}.dlq"
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())

    # --- Phase 1: Register v1 and verify ---
    print("Phase 1: Registering model v1...")
    adapter.register_task_handler(predict_v1, name="predict")
    adapter.start_consumer()

    try:
        # Enqueue a task for v1
        print("  Enqueuing task for v1...")
        result_v1 = adapter.enqueue_task_sync("predict", args=["This is a great product"])
        print(f"  task_id={result_v1.id}")

        outcome_v1 = poll_result(adapter, result_v1.id)
        if outcome_v1:
            print(f"  v1 result: {outcome_v1['result']}")
            assert outcome_v1["result"]["version"] == 1, "Expected version 1"
            print("  Confirmed: handler returned version=1")
        else:
            print("  v1 task timed out!")

        # --- Phase 2: Hot-swap to v2 without stopping consumer ---
        print("\nPhase 2: Hot-swapping to model v2 (consumer still running)...")
        adapter.register_task_handler(predict_v2, name="predict")
        print("  Handler overwritten with v2")

        # Small delay to ensure handler swap takes effect
        time.sleep(0.5)

        # Enqueue a task for v2
        print("  Enqueuing task for v2...")
        result_v2 = adapter.enqueue_task_sync("predict", args=["This product could be better"])
        print(f"  task_id={result_v2.id}")

        outcome_v2 = poll_result(adapter, result_v2.id)
        if outcome_v2:
            print(f"  v2 result: {outcome_v2['result']}")
            assert outcome_v2["result"]["version"] == 2, "Expected version 2"
            print("  Confirmed: handler returned version=2")
        else:
            print("  v2 task timed out!")

        # --- Summary ---
        print("\n--- Version Swap Summary ---")
        print("  v1 registered -> task processed with v1")
        print("  v2 registered (same name 'predict') -> task processed with v2")
        print("  Consumer was never stopped during the swap")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
