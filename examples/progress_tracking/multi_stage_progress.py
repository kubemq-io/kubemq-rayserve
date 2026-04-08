"""
Multi-Stage Progress — Two-Stage Pipeline Progress Reporting

Demonstrates:
    - Two-stage pipeline where each stage reports progress independently
    - Stage 1: preprocessing (0-50%)
    - Stage 2: inference (50-100%)
    - Explicit task_id kwarg pattern (v1.0.0 workaround)
    - Detailed stage-aware progress messages

Usage:
    python examples/progress_tracking/multi_stage_progress.py

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

# Module-level adapter reference for handler closure
adapter: KubeMQTaskProcessorAdapter | None = None


def preprocess(data: str, task_id: str = "") -> dict:
    """Stage 1: Preprocessing with progress 0-50%."""
    assert adapter is not None

    adapter.report_progress(task_id, 0, "Stage 1: Starting preprocessing")
    time.sleep(0.2)

    adapter.report_progress(task_id, 10, "Stage 1: Validating input")
    time.sleep(0.2)

    adapter.report_progress(task_id, 25, "Stage 1: Normalizing data")
    time.sleep(0.2)

    adapter.report_progress(task_id, 40, "Stage 1: Tokenizing")
    time.sleep(0.2)

    adapter.report_progress(task_id, 50, "Stage 1: Preprocessing complete")
    return {"preprocessed": data.lower().strip(), "tokens": len(data.split())}


def inference(preprocessed: str, token_count: int = 0, task_id: str = "") -> dict:
    """Stage 2: Inference with progress 50-100%."""
    assert adapter is not None

    adapter.report_progress(task_id, 55, "Stage 2: Loading model")
    time.sleep(0.2)

    adapter.report_progress(task_id, 65, "Stage 2: Running inference")
    time.sleep(0.3)

    adapter.report_progress(task_id, 80, "Stage 2: Post-processing results")
    time.sleep(0.2)

    adapter.report_progress(task_id, 95, "Stage 2: Formatting output")
    time.sleep(0.1)

    adapter.report_progress(task_id, 100, "Stage 2: Inference complete")
    return {"prediction": "positive", "confidence": 0.94, "token_count": token_count}


def main():
    global adapter

    channel = f"example-progress-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(preprocess, name="preprocess")
    adapter.register_task_handler(inference, name="inference")
    adapter.start_consumer()

    try:
        input_data = "The quick brown fox jumps over the lazy dog"

        # --- Stage 1: Preprocess ---
        stage1_task_id = f"task-{uuid.uuid4().hex[:8]}"
        print(f"Stage 1: Enqueuing preprocess task (task_id={stage1_task_id})...")
        result1 = adapter.enqueue_task_sync(
            "preprocess",
            kwargs={"data": input_data, "task_id": stage1_task_id},
        )
        print(f"  enqueued task_id={result1.id}")

        # Poll for stage 1 completion
        stage1_output = None
        for i in range(30):
            status = adapter.get_task_status_sync(result1.id)
            if status.status in ("SUCCESS", "FAILURE"):
                stage1_output = status.result
                print(f"  Stage 1 result: {stage1_output}")
                break
            time.sleep(0.5)
        else:
            print("  Stage 1 timed out.")

        if stage1_output is None:
            print("  Stage 1 failed, skipping Stage 2.")
        else:
            # --- Stage 2: Inference ---
            stage2_task_id = f"task-{uuid.uuid4().hex[:8]}"
            print(f"\nStage 2: Enqueuing inference task (task_id={stage2_task_id})...")
            result2 = adapter.enqueue_task_sync(
                "inference",
                kwargs={
                    "preprocessed": stage1_output.get("preprocessed", ""),
                    "token_count": stage1_output.get("tokens", 0),
                    "task_id": stage2_task_id,
                },
            )
            print(f"  enqueued task_id={result2.id}")

            # Poll for stage 2 completion
            for i in range(30):
                status = adapter.get_task_status_sync(result2.id)
                if status.status in ("SUCCESS", "FAILURE"):
                    print(f"  Stage 2 result: {status.result}")
                    break
                time.sleep(0.5)
            else:
                print("  Stage 2 timed out.")

        print("\nPipeline complete.")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
