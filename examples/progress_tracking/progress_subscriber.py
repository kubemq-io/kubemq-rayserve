"""
Progress Subscriber — External Subscriber Monitoring Progress Events

Demonstrates:
    - PubSubClient subscribing to {queue}.progress channel
    - Real-time progress event observation from an external client
    - Explicit task_id kwarg pattern (v1.0.0 workaround)
    - Filtering progress events by known task_id

Usage:
    python examples/progress_tracking/progress_subscriber.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

import os
import time
import uuid

from kubemq import (
    CancellationToken,
    ClientConfig,
    EventReceived,
    EventsSubscription,
    PubSubClient,
)

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")

# Module-level adapter reference for handler closure
adapter: KubeMQTaskProcessorAdapter | None = None


def analyze(data: str, task_id: str = "") -> dict:
    """Handler that reports progress via explicit task_id kwarg."""
    assert adapter is not None
    adapter.report_progress(task_id, 0, "Starting analysis")
    time.sleep(0.3)

    adapter.report_progress(task_id, 33, "Phase 1 complete")
    time.sleep(0.3)

    adapter.report_progress(task_id, 66, "Phase 2 complete")
    time.sleep(0.3)

    adapter.report_progress(task_id, 100, "Analysis finished")
    return {"analyzed": data, "phases": 2}


def main():
    global adapter

    channel = f"example-progress-{uuid.uuid4().hex[:8]}"
    progress_channel = f"{channel}.progress"

    # --- Set up progress subscriber ---
    received_events: list[str] = []

    def on_event(event: EventReceived) -> None:
        body = event.body.decode("utf-8") if event.body else ""
        received_events.append(body)
        print(f"  [subscriber] progress event: {body}")

    def on_error(err: str) -> None:
        print(f"  [subscriber] error: {err}")

    cancel_token = CancellationToken()
    pubsub_config = ClientConfig(address=BROKER, client_id=f"progress-sub-{uuid.uuid4().hex[:8]}")
    pubsub_client = PubSubClient(config=pubsub_config)

    subscription = EventsSubscription(
        channel=progress_channel,
        group="",
        on_receive_event_callback=on_event,
        on_error_callback=on_error,
    )
    pubsub_client.subscribe_to_events(subscription, cancel_token)
    print(f"Subscribed to progress channel: {progress_channel}")

    # Brief pause to let subscription establish
    time.sleep(0.5)

    # --- Set up adapter ---
    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(analyze, name="analyze")
    adapter.start_consumer()

    try:
        # Pre-generate a known task_id and pass as kwarg
        known_task_id = f"task-{uuid.uuid4().hex[:8]}"
        print(f"Enqueuing task with known task_id={known_task_id}...")
        result = adapter.enqueue_task_sync(
            "analyze",
            kwargs={"data": "test input", "task_id": known_task_id},
        )
        print(f"  enqueued task_id={result.id}")

        # Poll for completion
        print("Polling for result...")
        for i in range(30):
            status = adapter.get_task_status_sync(result.id)
            if status.status in ("SUCCESS", "FAILURE"):
                print(f"  final status={status.status}")
                print(f"  result={status.result}")
                break
            time.sleep(0.5)
        else:
            print("  Timed out waiting for result.")

        # Brief pause to receive any remaining events
        time.sleep(0.5)
        print(f"\nTotal progress events received: {len(received_events)}")
    finally:
        cancel_token.cancel()
        pubsub_client.close()
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
