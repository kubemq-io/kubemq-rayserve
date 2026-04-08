"""
Pipeline Chain Pattern — Stage 1 -> Stage 2 -> Stage 3

Demonstrates:
    - Multi-stage pipeline where each stage's output feeds the next stage's input
    - Three adapters on separate channels, one per pipeline stage
    - Coordinator enqueues to stage 1, polls result, feeds to stage 2, etc.

Usage:
    python examples/patterns/pipeline_chain.py

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


def stage1_extract(raw_text: str) -> dict:
    """Stage 1: Extract words from raw text."""
    time.sleep(0.1)
    words = raw_text.strip().split()
    return {"words": words, "count": len(words)}


def stage2_transform(words: list, count: int) -> dict:
    """Stage 2: Transform words (uppercase, deduplicate)."""
    time.sleep(0.1)
    unique_upper = sorted(set(w.upper() for w in words))
    return {
        "unique_words": unique_upper,
        "original_count": count,
        "unique_count": len(unique_upper),
    }


def stage3_load(unique_words: list, original_count: int, unique_count: int) -> dict:
    """Stage 3: Load/summarize the pipeline results."""
    time.sleep(0.1)
    return {
        "summary": f"Processed {original_count} words -> {unique_count} unique",
        "unique_words": unique_words,
        "reduction_pct": round((1 - unique_count / max(original_count, 1)) * 100, 1),
    }


def main():
    run_id = uuid.uuid4().hex[:8]

    # --- 1. Create one adapter per pipeline stage ---
    stages = [
        ("stage1-extract", stage1_extract),
        ("stage2-transform", stage2_transform),
        ("stage3-load", stage3_load),
    ]

    adapters: list[tuple[str, KubeMQTaskProcessorAdapter]] = []
    for stage_name, handler_fn in stages:
        channel = f"example-pipeline-{run_id}-{stage_name}"
        config = KubeMQAdapterConfig(address=BROKER)
        adapter = KubeMQTaskProcessorAdapter(config)

        class _Cfg:
            queue_name = channel
            max_retries = 1
            failed_task_queue_name = f"{channel}.dlq"
            unprocessable_task_queue_name = ""

        adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
        adapter.register_task_handler(handler_fn, name=stage_name)
        adapter.start_consumer()
        adapters.append((stage_name, adapter))
        print(f"  Stage '{stage_name}' started on channel={channel}")

    try:
        # --- 2. Feed pipeline: stage 1 -> stage 2 -> stage 3 ---
        raw_input = "the quick brown fox jumps over the lazy brown fox"
        print(f"\nPipeline input: '{raw_input}'")

        # Stage 1: Extract
        print("\n--- Stage 1: Extract ---")
        stage1_name, stage1_adapter = adapters[0]
        result1 = stage1_adapter.enqueue_task_sync(stage1_name, kwargs={"raw_text": raw_input})
        print(f"  Enqueued: task_id={result1.id}")

        stage1_output = None
        for _ in range(30):
            status = stage1_adapter.get_task_status_sync(result1.id)
            if status.status in ("SUCCESS", "FAILURE"):
                stage1_output = status.result
                print(f"  Result: {status.status} -> {stage1_output}")
                break
            time.sleep(0.5)

        if not stage1_output or status.status != "SUCCESS":
            print("  Stage 1 failed, aborting pipeline.")
            return

        # Stage 2: Transform (input = stage 1 output)
        print("\n--- Stage 2: Transform ---")
        stage2_name, stage2_adapter = adapters[1]
        result2 = stage2_adapter.enqueue_task_sync(stage2_name, kwargs=stage1_output)
        print(f"  Enqueued: task_id={result2.id}")

        stage2_output = None
        for _ in range(30):
            status = stage2_adapter.get_task_status_sync(result2.id)
            if status.status in ("SUCCESS", "FAILURE"):
                stage2_output = status.result
                print(f"  Result: {status.status} -> {stage2_output}")
                break
            time.sleep(0.5)

        if not stage2_output or status.status != "SUCCESS":
            print("  Stage 2 failed, aborting pipeline.")
            return

        # Stage 3: Load (input = stage 2 output)
        print("\n--- Stage 3: Load ---")
        stage3_name, stage3_adapter = adapters[2]
        result3 = stage3_adapter.enqueue_task_sync(stage3_name, kwargs=stage2_output)
        print(f"  Enqueued: task_id={result3.id}")

        for _ in range(30):
            status = stage3_adapter.get_task_status_sync(result3.id)
            if status.status in ("SUCCESS", "FAILURE"):
                print(f"  Result: {status.status} -> {status.result}")
                break
            time.sleep(0.5)

        print("\nPipeline complete (3 stages).")

    finally:
        # --- 3. Cleanup: stop all stage adapters ---
        for stage_name, adapter in adapters:
            adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
