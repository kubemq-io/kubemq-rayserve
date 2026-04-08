"""
Direct Channel List — Raw KubeMQ SDK Usage

Demonstrates:
    - Using QueuesClient.list_queues_channels() for queue monitoring
    - Inspecting channel name, activity status, and message statistics
    - Using channel search filters with wildcards
    - Useful for operational dashboards and health monitoring

Usage:
    python examples/raw_sdk/direct_channel_list.py

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
    prefix = f"example-raw-list-{uuid.uuid4().hex[:8]}"

    config = ClientConfig(
        address=BROKER,
        client_id=f"raw-lister-{uuid.uuid4().hex[:8]}",
    )
    client = QueuesClient(config=config)

    try:
        # Create some channels by sending messages
        channels_to_create = [
            f"{prefix}.tasks",
            f"{prefix}.results",
            f"{prefix}.dlq",
        ]

        for ch in channels_to_create:
            body = json.dumps({"init": True}).encode("utf-8")
            msg = QueueMessage(channel=ch, body=body)
            client.send_queue_message(msg)
        print(f"Created {len(channels_to_create)} channels with prefix: {prefix}")

        # List all queue channels
        print("\n--- All queue channels ---")
        all_channels = client.list_queues_channels()
        print(f"Total queue channels on broker: {len(all_channels)}")
        for ch in all_channels:
            print(
                f"  name={ch.name}, "
                f"is_active={ch.is_active}, "
                f"incoming={ch.incoming}, "
                f"outgoing={ch.outgoing}"
            )

        # List channels with a search filter
        print(f"\n--- Channels matching '{prefix}*' ---")
        filtered = client.list_queues_channels(channel_search=f"{prefix}*")
        print(f"Matching channels: {len(filtered)}")
        for ch in filtered:
            print(
                f"  name={ch.name}, "
                f"is_active={ch.is_active}, "
                f"incoming: messages={ch.incoming.messages}, waiting={ch.incoming.waiting}"
            )

        # Send more messages and check updated stats
        print("\n--- Sending additional messages ---")
        for i in range(5):
            body = json.dumps({"index": i}).encode("utf-8")
            msg = QueueMessage(channel=f"{prefix}.tasks", body=body)
            client.send_queue_message(msg)
        print(f"Sent 5 more messages to {prefix}.tasks")

        # Re-list to see updated stats
        print(f"\n--- Updated stats for '{prefix}*' ---")
        updated = client.list_queues_channels(channel_search=f"{prefix}*")
        for ch in updated:
            print(
                f"  name={ch.name}, "
                f"incoming: messages={ch.incoming.messages}, "
                f"waiting={ch.incoming.waiting}"
            )

    finally:
        client.close()

    print("\nExample complete.")


if __name__ == "__main__":
    main()
