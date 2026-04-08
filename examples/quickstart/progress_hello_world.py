"""
Progress Hello World — Task Progress Tracking

Demonstrates:
    - Reporting progress from within a task handler
    - Subscribing to progress events via PubSubClient
    - Real-time progress updates using KubeMQ Events

Usage:
    python examples/quickstart/progress_hello_world.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

import json
import os
import time
import uuid

from kubemq import CancellationToken, ClientConfig, EventsSubscription, PubSubClient

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")

# Adapter instance accessible from handler via closure
adapter: KubeMQTaskProcessorAdapter | None = None


def process_data(data: str, task_id: str = "") -> dict:
    """Handler that reports progress at each stage."""
    if adapter is not None and task_id:
        adapter.report_progress(task_id, 25, "Starting processing")
    time.sleep(0.3)

    if adapter is not None and task_id:
        adapter.report_progress(task_id, 50, "Halfway done")
    time.sleep(0.3)

    if adapter is not None and task_id:
        adapter.report_progress(task_id, 75, "Almost done")
    time.sleep(0.3)

    if adapter is not None and task_id:
        adapter.report_progress(task_id, 100, "Complete")
    return {"processed": data, "status": "done"}


def main():
    global adapter

    channel = f"example-quickstart-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(process_data, name="process_data")

    # Subscribe to progress events
    progress_events: list[dict] = []
    cancel_token = CancellationToken()

    def on_event(event) -> None:
        try:
            data = json.loads(event.body)
            progress_events.append(data)
            print(f"  [Progress] pct={data.get('pct', 0)}% detail={data.get('detail', '')}")
        except Exception:
            pass

    def on_error(error: str) -> None:
        pass

    pubsub = PubSubClient(
        config=ClientConfig(
            address=BROKER,
            client_id=f"progress-sub-{uuid.uuid4().hex[:8]}",
        )
    )
    subscription = EventsSubscription(
        channel=f"{channel}.progress",
        on_receive_event_callback=on_event,
        on_error_callback=on_error,
    )
    pubsub.subscribe_to_events(subscription, cancel_token)

    adapter.start_consumer()

    try:
        known_task_id = f"task-{uuid.uuid4().hex[:8]}"
        print(f"Enqueuing task with known task_id={known_task_id}...")
        result = adapter.enqueue_task_sync(
            "process_data",
            kwargs={"data": "hello", "task_id": known_task_id},
        )
        print(f"  enqueued: task_id={result.id}")

        # Wait for processing to complete
        for _ in range(30):
            status = adapter.get_task_status_sync(result.id)
            if status.status in ("SUCCESS", "FAILURE"):
                print(f"\nResult: status={status.status} result={status.result}")
                break
            time.sleep(0.5)
        else:
            print("Timed out waiting for result.")

        print(f"\nTotal progress events received: {len(progress_events)}")
    finally:
        cancel_token.cancel()
        try:
            pubsub.close()
        except Exception:
            pass
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
