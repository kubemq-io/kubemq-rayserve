"""
Direct Queue Receive — Raw KubeMQ SDK Usage

Demonstrates:
    - Using QueuesClient.receive_queue_messages() directly
    - Message acknowledgment with ack() and nack()
    - Processing received messages with body decoding
    - Transaction-based message consumption

Usage:
    python examples/raw_sdk/direct_queue_receive.py

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
    channel = f"example-raw-receive-{uuid.uuid4().hex[:8]}"

    config = ClientConfig(
        address=BROKER,
        client_id=f"raw-receiver-{uuid.uuid4().hex[:8]}",
    )
    client = QueuesClient(config=config)

    try:
        # Pre-populate the queue with messages
        for i in range(5):
            body = json.dumps({"index": i, "text": f"Message {i}"}).encode("utf-8")
            msg = QueueMessage(channel=channel, body=body)
            client.send_queue_message(msg)
        print(f"Sent 5 messages to channel: {channel}")

        # Receive messages with manual acknowledgment
        print("\n--- Receiving with manual ack ---")
        response = client.receive_queue_messages(
            channel=channel,
            max_messages=3,
            wait_timeout_in_seconds=5,
            auto_ack=False,
        )

        if response.is_error:
            print(f"Error: {response.error}")
        else:
            print(f"Received {len(response.messages)} messages:")
            for msg in response.messages:
                payload = json.loads(msg.body.decode("utf-8"))
                print(f"  Message: {payload}")

                # Ack even-indexed messages, nack odd-indexed
                if payload["index"] % 2 == 0:
                    msg.ack()
                    print(f"    -> Acknowledged (index={payload['index']})")
                else:
                    msg.nack()
                    print(f"    -> Nacked (index={payload['index']})")

        # Receive remaining messages with auto-ack
        print("\n--- Receiving with auto_ack=True ---")
        response = client.receive_queue_messages(
            channel=channel,
            max_messages=10,
            wait_timeout_in_seconds=3,
            auto_ack=True,
        )

        if response.is_error:
            print(f"Error: {response.error}")
        else:
            print(f"Received {len(response.messages)} messages (auto-acked):")
            for msg in response.messages:
                payload = json.loads(msg.body.decode("utf-8"))
                print(f"  Message: {payload}")

    finally:
        client.close()

    print("\nExample complete.")


if __name__ == "__main__":
    main()
