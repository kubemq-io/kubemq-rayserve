"""
Direct Queue Send — Raw KubeMQ SDK Usage

Demonstrates:
    - Using QueuesClient directly (bypassing the adapter)
    - Constructing and sending QueueMessage
    - ClientConfig for connection setup
    - When to use raw SDK: custom message formats, direct queue manipulation

Usage:
    python examples/raw_sdk/direct_queue_send.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

import json
import os
import uuid

from kubemq import ClientConfig, QueueMessage, QueuesClient

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")


def main():
    channel = f"example-raw-send-{uuid.uuid4().hex[:8]}"

    config = ClientConfig(
        address=BROKER,
        client_id=f"raw-sender-{uuid.uuid4().hex[:8]}",
    )
    client = QueuesClient(config=config)

    try:
        # Send a simple message
        body = json.dumps({"task": "classify", "text": "Hello world"}).encode("utf-8")
        msg = QueueMessage(channel=channel, body=body)
        client.send_queue_message(msg)
        print(f"Sent message to channel: {channel}")

        # Send with DLQ routing
        msg_with_dlq = QueueMessage(
            channel=channel,
            body=json.dumps({"task": "process", "data": [1, 2, 3]}).encode("utf-8"),
            max_receive_count=3,
            max_receive_queue=f"{channel}.dlq",
        )
        client.send_queue_message(msg_with_dlq)
        print("Sent message with DLQ routing (max_receive_count=3)")

        # Verify by peeking
        result = client.peek_queue_messages(
            channel=channel, max_messages=10, wait_timeout_in_seconds=2
        )
        if not result.is_error:
            print(f"\nPeeked {len(result.messages)} messages:")
            for m in result.messages:
                print(f"  body={m.body.decode('utf-8')}")
    finally:
        client.close()

    print("Example complete.")


if __name__ == "__main__":
    main()
