"""
A/B Model Testing — Route Tasks to Model-A vs Model-B Queues

Demonstrates:
    - Two separate adapters with different queues (model-a, model-b)
    - Random routing of inference requests between models
    - Comparing results from both models
    - Simulated A/B testing for model evaluation

Usage:
    python examples/ml_patterns/ab_model_testing.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

import os
import random
import time
import uuid

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")

NUM_REQUESTS = 10


def model_a_predict(text: str) -> dict:
    """Simulated Model A — higher accuracy, slower inference."""
    # In production: model = load_model("model_a_v2.3")
    time.sleep(0.15)  # Simulate 150ms inference
    score = hash(text) % 100 / 100.0
    return {
        "model": "model-a",
        "label": "positive" if score > 0.45 else "negative",
        "confidence": round(0.85 + random.uniform(0, 0.14), 3),
    }


def model_b_predict(text: str) -> dict:
    """Simulated Model B — lower accuracy, faster inference."""
    # In production: model = load_model("model_b_v1.0")
    time.sleep(0.05)  # Simulate 50ms inference
    score = hash(text) % 100 / 100.0
    return {
        "model": "model-b",
        "label": "positive" if score > 0.50 else "negative",
        "confidence": round(0.70 + random.uniform(0, 0.20), 3),
    }


def main():
    uid = uuid.uuid4().hex[:8]
    channel_a = f"example-ml-ab-model-a-{uid}"
    channel_b = f"example-ml-ab-model-b-{uid}"

    config_a = KubeMQAdapterConfig(address=BROKER)
    adapter_a = KubeMQTaskProcessorAdapter(config_a)

    class _CfgA:
        queue_name = channel_a
        max_retries = 1
        failed_task_queue_name = f"{channel_a}.dlq"
        unprocessable_task_queue_name = ""

    adapter_a.initialize(consumer_concurrency=2, task_processor_config=_CfgA())
    adapter_a.register_task_handler(model_a_predict, name="predict")
    adapter_a.start_consumer()

    config_b = KubeMQAdapterConfig(address=BROKER)
    adapter_b = KubeMQTaskProcessorAdapter(config_b)

    class _CfgB:
        queue_name = channel_b
        max_retries = 1
        failed_task_queue_name = f"{channel_b}.dlq"
        unprocessable_task_queue_name = ""

    adapter_b.initialize(consumer_concurrency=2, task_processor_config=_CfgB())
    adapter_b.register_task_handler(model_b_predict, name="predict")
    adapter_b.start_consumer()

    try:
        print(f"A/B Testing with {NUM_REQUESTS} requests")
        print(f"  Model A queue: {channel_a}")
        print(f"  Model B queue: {channel_b}")

        # Route requests randomly to model A or B
        tasks: list[dict] = []
        for i in range(NUM_REQUESTS):
            text = f"Sample review text number {i}"
            use_model_a = random.random() < 0.5
            adapter = adapter_a if use_model_a else adapter_b
            model_name = "model-a" if use_model_a else "model-b"

            result = adapter.enqueue_task_sync("predict", args=[text])
            tasks.append(
                {
                    "task_id": result.id,
                    "model": model_name,
                    "adapter": adapter,
                    "text": text,
                }
            )
            print(f"  Request {i + 1}: routed to {model_name}")

        # Collect all results
        print("\nCollecting results...")
        model_a_results: list[dict] = []
        model_b_results: list[dict] = []
        timeout = 30
        start = time.perf_counter()

        pending = list(tasks)
        while pending and (time.perf_counter() - start) < timeout:
            still_pending = []
            for task in pending:
                status = task["adapter"].get_task_status_sync(task["task_id"])
                if status.status in ("SUCCESS", "FAILURE"):
                    entry = {
                        "model": task["model"],
                        "status": status.status,
                        "result": status.result,
                    }
                    if task["model"] == "model-a":
                        model_a_results.append(entry)
                    else:
                        model_b_results.append(entry)
                else:
                    still_pending.append(task)
            pending = still_pending
            if pending:
                time.sleep(0.5)

        # Compare results
        print("\n--- A/B Test Results ---")
        print(f"Model A: {len(model_a_results)} responses")
        if model_a_results:
            a_successes = sum(1 for r in model_a_results if r["status"] == "SUCCESS")
            a_confidences = [
                r["result"]["confidence"]
                for r in model_a_results
                if r["status"] == "SUCCESS" and r["result"]
            ]
            avg_a_conf = sum(a_confidences) / len(a_confidences) if a_confidences else 0
            print(f"  Successes: {a_successes}, Avg confidence: {avg_a_conf:.3f}")

        print(f"Model B: {len(model_b_results)} responses")
        if model_b_results:
            b_successes = sum(1 for r in model_b_results if r["status"] == "SUCCESS")
            b_confidences = [
                r["result"]["confidence"]
                for r in model_b_results
                if r["status"] == "SUCCESS" and r["result"]
            ]
            avg_b_conf = sum(b_confidences) / len(b_confidences) if b_confidences else 0
            print(f"  Successes: {b_successes}, Avg confidence: {avg_b_conf:.3f}")

        if pending:
            print(f"\n  {len(pending)} tasks still pending (timed out)")
    finally:
        adapter_a.stop_consumer()
        adapter_b.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
