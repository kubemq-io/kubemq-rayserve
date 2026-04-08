"""
Direct Queue Peek — Raw KubeMQ SDK Usage

Demonstrates:
    - Using QueuesClient.peek_queue_messages() for non-destructive reads
    - Messages remain in the queue after peeking
    - Inspecting message body, metadata, and receive count
    - Useful for monitoring and debugging without consuming messages

Usage:
    python examples/raw_sdk/direct_queue_peek.py

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
    channel = f"example-raw-peek-{uuid.uuid4().hex[:8]}"

    config = ClientConfig(
        address=BROKER,
        client_id=f"raw-peeker-{uuid.uuid4().hex[:8]}",
    )
    client = QueuesClient(config=config)

    try:
        # Populate the queue
        for i in range(3):
            body = json.dumps({"task": "process", "item": i}).encode("utf-8")
            msg = QueueMessage(channel=channel, body=body)
            client.send_queue_message(msg)
        print(f"Sent 3 messages to channel: {channel}")

        # Peek at messages — non-destructive
        print("\n--- First peek (messages remain in queue) ---")
        result = client.peek_queue_messages(
            channel=channel,
            max_messages=10,
            wait_timeout_in_seconds=3,
        )

        if result.is_error:
            print(f"Peek error: {result.error}")
        else:
            print(f"Peeked {len(result.messages)} messages:")
            for m in result.messages:
                payload = json.loads(m.body.decode("utf-8"))
                print(f"  id={m.id}, body={payload}, sequence={m.sequence}")

        # Peek again — same messages are still there
        print("\n--- Second peek (messages still in queue) ---")
        result2 = client.peek_queue_messages(
            channel=channel,
            max_messages=10,
            wait_timeout_in_seconds=3,
        )

        if not result2.is_error:
            print(f"Peeked {len(result2.messages)} messages (unchanged):")
            for m in result2.messages:
                payload = json.loads(m.body.decode("utf-8"))
                print(f"  id={m.id}, body={payload}")

        # Now consume the messages to prove peek was non-destructive
        print("\n--- Consuming messages (destructive) ---")
        response = client.receive_queue_messages(
            channel=channel,
            max_messages=10,
            wait_timeout_in_seconds=3,
            auto_ack=True,
        )

        if not response.is_error:
            print(f"Consumed {len(response.messages)} messages")

        # Peek again — queue should be empty
        print("\n--- Final peek (queue empty after consume) ---")
        result3 = client.peek_queue_messages(
            channel=channel,
            max_messages=10,
            wait_timeout_in_seconds=2,
        )

        if not result3.is_error:
            print(f"Peeked {len(result3.messages)} messages (should be 0)")

    finally:
        client.close()

    print("\nExample complete.")


if __name__ == "__main__":
    main()
