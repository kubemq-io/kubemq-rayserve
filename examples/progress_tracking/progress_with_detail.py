"""
Progress With Detail — Rich Detail Strings in Progress Reports

Demonstrates:
    - report_progress() with rich detail messages
    - Descriptive progress like "Processing image 3/10"
    - Explicit task_id kwarg pattern (v1.0.0 workaround)
    - Progress tracking through a simulated batch operation

Usage:
    python examples/progress_tracking/progress_with_detail.py

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


def process_images(image_count: int = 5, task_id: str = "") -> dict:
    """Handler that processes images with rich progress detail strings."""
    assert adapter is not None

    adapter.report_progress(task_id, 0, f"Starting batch of {image_count} images")
    time.sleep(0.2)

    processed = 0
    for i in range(1, image_count + 1):
        pct = int((i / image_count) * 80) + 10  # 10% to 90%
        adapter.report_progress(task_id, pct, f"Processing image {i}/{image_count}")
        time.sleep(0.2)
        processed += 1

    adapter.report_progress(task_id, 95, f"Generating summary for {processed} images")
    time.sleep(0.2)

    adapter.report_progress(task_id, 100, "Batch complete")
    return {"processed_count": processed, "status": "success"}


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
    adapter.register_task_handler(process_images, name="process_images")
    adapter.start_consumer()

    try:
        # Pre-generate a known task_id and pass as kwarg
        known_task_id = f"task-{uuid.uuid4().hex[:8]}"
        print(f"Enqueuing image batch task with known task_id={known_task_id}...")
        result = adapter.enqueue_task_sync(
            "process_images",
            kwargs={"image_count": 5, "task_id": known_task_id},
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
