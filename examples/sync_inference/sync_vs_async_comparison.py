"""
Sync vs Async Comparison — Latency Comparison

Demonstrates:
    - Same task executed via sync query_task_sync()
    - Same task executed via async enqueue_task_sync() + polling
    - Side-by-side latency comparison
    - When to prefer sync vs async

Usage:
    python examples/sync_inference/sync_vs_async_comparison.py

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


def summarize(text: str) -> dict:
    """Simulated text summarization."""
    time.sleep(0.2)  # Simulate model inference
    words = text.split()
    summary = " ".join(words[:3]) + "..." if len(words) > 3 else text
    return {"summary": summary, "original_length": len(words)}


def main():
    channel = f"example-sync-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER, sync_inference_timeout=10)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(summarize, name="summarize")
    adapter.start_consumer()

    input_text = "The quick brown fox jumps over the lazy dog"

    try:
        # --- Sync approach: query_task_sync ---
        print("=== Sync Inference (query_task_sync) ===")
        t0 = time.time()
        sync_result = adapter.query_task_sync(
            task_name="summarize",
            args=[input_text],
            timeout=10,
        )
        sync_elapsed = time.time() - t0
        print(f"  status={sync_result.status}")
        print(f"  result={sync_result.result}")
        print(f"  elapsed={sync_elapsed:.3f}s")

        # --- Async approach: enqueue + poll ---
        print("\n=== Async Inference (enqueue + poll) ===")
        t0 = time.time()
        enqueued = adapter.enqueue_task_sync("summarize", args=[input_text])
        print(f"  task_id={enqueued.id}")

        for i in range(30):
            status = adapter.get_task_status_sync(enqueued.id)
            if status.status in ("SUCCESS", "FAILURE"):
                break
            time.sleep(0.5)
        async_elapsed = time.time() - t0
        print(f"  status={status.status}")
        print(f"  result={status.result}")
        print(f"  elapsed={async_elapsed:.3f}s")

        # --- Comparison ---
        print("\n=== Latency Comparison ===")
        print(f"  Sync:  {sync_elapsed:.3f}s (blocking, immediate result)")
        print(f"  Async: {async_elapsed:.3f}s (enqueue + poll loop)")
        print("  Note: Sync is typically faster for single requests because")
        print("        it avoids the polling interval overhead.")
        print("        Async is better for fire-and-forget or batch workloads.")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
