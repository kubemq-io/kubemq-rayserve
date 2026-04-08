"""
Malformed Message — Raw Invalid JSON Sent to Queue

Demonstrates:
    - Sending a raw invalid JSON message directly via KubeMQ SDK
    - The adapter routes unprocessable messages to the unprocessable queue
    - Normal tasks continue processing unaffected
    - Why the unprocessable_task_queue_name config matters

Usage:
    python examples/error_handling/malformed_message.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve kubemq
"""

from __future__ import annotations

import os
import time
import uuid

from kubemq import ClientConfig, QueueMessage, QueuesClient

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")


def handler(data: str) -> dict:
    return {"echo": data}


def main():
    channel = f"example-error-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = f"{channel}.unprocessable"

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(handler, name="handler")
    adapter.start_consumer()

    try:
        # --- Step 1: Send malformed (non-JSON) message via raw SDK ---
        print("Step 1: Sending malformed message via raw KubeMQ SDK...")
        raw_config = ClientConfig(
            address=BROKER,
            client_id=f"malformed-sender-{uuid.uuid4().hex[:8]}",
        )
        raw_client = QueuesClient(config=raw_config)

        try:
            malformed_body = b"this is not valid JSON {{{{"
            msg = QueueMessage(
                channel=channel,
                body=malformed_body,
            )
            raw_client.send_queue_message(msg)
            print(f"  Sent malformed message to channel: {channel}")
            print(f"  Body: {malformed_body!r}")
        finally:
            raw_client.close()

        # Give the consumer time to process (and reject) the malformed message
        time.sleep(3)

        # --- Step 2: Send a valid task through the adapter ---
        print("\nStep 2: Sending a valid task through the adapter...")
        result = adapter.enqueue_task_sync("handler", args=["valid-data"])
        print(f"  task_id={result.id}")

        for _ in range(30):
            status = adapter.get_task_status_sync(result.id)
            if status.status in ("SUCCESS", "FAILURE"):
                print(f"  status={status.status} result={status.result}")
                break
            time.sleep(0.5)
        else:
            print("  Timed out waiting for result.")

        print(f"\nThe malformed message was routed to: {channel}.unprocessable")
        print("The valid task was processed normally — malformed messages do not block the queue.")
        print("\nBest practice: Always configure unprocessable_task_queue_name to catch")
        print("messages that cannot be deserialized by the adapter.")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
