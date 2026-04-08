"""
Custom Result Query — Peek Result Channels for Debugging

Demonstrates:
    - Using QueuesClient to peek at result channels directly
    - Debugging task results without the adapter's get_task_status_sync
    - Inspecting raw result message format stored by the adapter
    - Useful for operational monitoring and debugging failed tasks

Usage:
    python examples/raw_sdk/custom_result_query.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

import json
import os
import time
import uuid

from kubemq import ClientConfig, QueuesClient

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")


def summarize(text: str) -> dict:
    """Simulated text summarization handler."""
    time.sleep(0.1)
    return {"summary": f"Summary of: {text[:30]}...", "length": len(text)}


def failing_handler(text: str) -> dict:
    """Handler that always fails for DLQ demonstration."""
    raise RuntimeError("Simulated processing failure")


def main():
    channel = f"example-raw-result-{uuid.uuid4().hex[:8]}"

    # --- Set up the adapter to produce results ---
    adapter_config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(adapter_config)

    class _Cfg:
        queue_name = channel
        max_retries = 1
        failed_task_queue_name = f"{channel}.dlq"
        unprocessable_task_queue_name = f"{channel}.unprocessable"

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(summarize, name="summarize")
    adapter.register_task_handler(failing_handler, name="failing")
    adapter.start_consumer()

    # --- Set up raw SDK client for direct inspection ---
    sdk_config = ClientConfig(
        address=BROKER,
        client_id=f"raw-debug-{uuid.uuid4().hex[:8]}",
    )
    sdk_client = QueuesClient(config=sdk_config)

    try:
        # Enqueue a successful task
        print("--- Enqueuing tasks ---")
        result_ok = adapter.enqueue_task_sync(
            "summarize",
            kwargs={"text": "This is a long document that needs summarizing."},
        )
        task_id_ok = result_ok.id
        print(f"  Enqueued success task: {task_id_ok}")

        # Wait for processing
        time.sleep(2)

        # Use raw SDK to list all channels related to this queue
        print("\n--- Listing all related channels ---")
        channels = sdk_client.list_queues_channels(channel_search=f"{channel}*")
        for ch in channels:
            print(f"  {ch.name}: waiting={ch.incoming.waiting}, messages={ch.incoming.messages}")

        # Peek at result channels to see raw result format
        print("\n--- Peeking at result channels ---")
        for ch in channels:
            if ch.incoming.waiting > 0 and ch.name != channel:
                result = sdk_client.peek_queue_messages(
                    channel=ch.name,
                    max_messages=10,
                    wait_timeout_in_seconds=2,
                )
                if not result.is_error and result.messages:
                    print(f"\n  Channel: {ch.name}")
                    print(f"  Messages waiting: {len(result.messages)}")
                    for m in result.messages:
                        try:
                            body = json.loads(m.body.decode("utf-8"))
                            print(f"    id={m.id}")
                            print(f"    body={json.dumps(body, indent=6)}")
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            print(f"    id={m.id}")
                            print(f"    body (raw)={m.body[:100]}")

        # Also verify via adapter's normal method
        print("\n--- Verifying via adapter ---")
        status = adapter.get_task_status_sync(task_id_ok)
        print(f"  Task {task_id_ok}: {status}")

        # Peek at DLQ channel if it exists
        print("\n--- Checking DLQ channel ---")
        dlq_result = sdk_client.peek_queue_messages(
            channel=f"{channel}.dlq",
            max_messages=10,
            wait_timeout_in_seconds=2,
        )
        if not dlq_result.is_error:
            print(f"  DLQ messages: {len(dlq_result.messages)}")
            for m in dlq_result.messages:
                try:
                    body = json.loads(m.body.decode("utf-8"))
                    print(f"    DLQ entry: {body}")
                except (json.JSONDecodeError, UnicodeDecodeError):
                    print(f"    DLQ entry (raw): {m.body[:100]}")

    finally:
        adapter.stop_consumer()
        sdk_client.close()

    print("\nExample complete.")


if __name__ == "__main__":
    main()
