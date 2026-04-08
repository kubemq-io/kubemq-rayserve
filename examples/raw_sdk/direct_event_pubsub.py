"""
Direct Event PubSub — Raw KubeMQ SDK Usage

Demonstrates:
    - Using PubSubClient for fire-and-forget event publishing
    - Subscribing to events with EventsSubscription
    - CancellationToken for subscription lifecycle management
    - Callback-based event handling

Usage:
    python examples/raw_sdk/direct_event_pubsub.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid

from kubemq import (
    CancellationToken,
    ClientConfig,
    EventMessage,
    EventsSubscription,
    PubSubClient,
)

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")


def main():
    channel = f"example-raw-events-{uuid.uuid4().hex[:8]}"
    received_events: list[dict] = []
    event_received = threading.Event()

    def on_receive_event(event):
        """Callback invoked when an event is received."""
        payload = json.loads(event.body.decode("utf-8"))
        received_events.append(payload)
        print(f"  Received event: {payload}")
        event_received.set()

    def on_error(err):
        """Callback invoked on subscription error."""
        print(f"  Subscription error: {err}")

    config = ClientConfig(
        address=BROKER,
        client_id=f"raw-pubsub-{uuid.uuid4().hex[:8]}",
    )
    client = PubSubClient(config=config)
    cancel = CancellationToken()

    try:
        # Set up the subscriber first
        subscription = EventsSubscription(
            channel=channel,
            on_receive_event_callback=on_receive_event,
            on_error_callback=on_error,
        )
        client.subscribe_to_events(subscription, cancel)
        print(f"Subscribed to events on channel: {channel}")

        # Allow subscription to establish
        time.sleep(1)

        # Publish events
        print("\n--- Publishing events ---")
        for i in range(3):
            body = json.dumps({"type": "notification", "index": i}).encode("utf-8")
            event = EventMessage(channel=channel, body=body)
            client.send_event(event)
            print(f"Published event {i}")

        # Wait for events to be received
        print("\n--- Waiting for events ---")
        for _ in range(10):
            if len(received_events) >= 3:
                break
            time.sleep(0.5)

        print(f"\nTotal events received: {len(received_events)}")

        # Publish with metadata and tags
        print("\n--- Publishing event with metadata and tags ---")
        event_with_meta = EventMessage(
            channel=channel,
            body=json.dumps({"type": "alert", "severity": "high"}).encode("utf-8"),
            metadata="alert-metadata",
            tags={"source": "raw-sdk-example", "priority": "high"},
        )
        client.send_event(event_with_meta)
        time.sleep(1)

        print(f"Total events received: {len(received_events)}")

    finally:
        cancel.cancel()
        client.close()

    print("\nExample complete.")


if __name__ == "__main__":
    main()
