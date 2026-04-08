"""
Basic Progress — Report Progress During Task Execution

Demonstrates:
    - report_progress() called from within a handler
    - Explicit task_id kwarg pattern (v1.0.0 workaround)
    - Progress percentage and detail string
    - Enqueue with known task_id passed as a kwarg

Usage:
    python examples/progress_tracking/basic_progress.py

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


def process_data(data: str, task_id: str = "") -> dict:
    """Handler that reports progress via explicit task_id kwarg."""
    assert adapter is not None
    adapter.report_progress(task_id, 0, "Starting")
    time.sleep(0.3)

    adapter.report_progress(task_id, 25, "Loaded data")
    time.sleep(0.3)

    adapter.report_progress(task_id, 50, "Processing")
    time.sleep(0.3)

    adapter.report_progress(task_id, 75, "Finalizing")
    time.sleep(0.3)

    adapter.report_progress(task_id, 100, "Complete")
    return {"processed": data, "status": "done"}


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
    adapter.register_task_handler(process_data, name="process_data")
    adapter.start_consumer()

    try:
        # Pre-generate a known task_id and pass as kwarg
        known_task_id = f"task-{uuid.uuid4().hex[:8]}"
        print(f"Enqueuing task with known task_id={known_task_id}...")
        result = adapter.enqueue_task_sync(
            "process_data",
            kwargs={"data": "sample input", "task_id": known_task_id},
        )
        print(f"  enqueued task_id={result.id}")

        # Poll for completion
        print("Polling for result...")
        for i in range(30):
            status = adapter.get_task_status_sync(result.id)
            print(f"  poll {i + 1}: status={status.status}")
            if status.status in ("SUCCESS", "FAILURE"):
                print(f"  result={status.result}")
                break
            time.sleep(0.5)
        else:
            print("  Timed out waiting for result.")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
