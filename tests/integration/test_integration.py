"""Integration tests requiring a live KubeMQ broker (localhost:50000).

Each test uses unique channel names (UUID-based) for isolation and creates
its own client instances to avoid shared state. All tests run against the
live broker and verify real send/receive cycles.
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid

import pytest

from kubemq import (
    CancellationToken,
    ClientConfig,
    CQClient,
    EventMessage,
    EventsSubscription,
    PubSubClient,
    QueriesSubscription,
    QueryMessage,
    QueryResponse,
    QueueMessage,
    QueuesClient,
    ServerInfo,
)

pytestmark = pytest.mark.integration

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")


def _make_config(client_id: str | None = None) -> ClientConfig:
    """Build a ClientConfig for tests."""
    return ClientConfig(
        address=BROKER,
        client_id=client_id or f"test-{uuid.uuid4().hex[:8]}",
    )


@pytest.mark.timeout(15)
def test_queue_send_receive_cycle():
    """Real queue send/receive cycle with unique channel."""
    channel = f"test-integ-queue-{uuid.uuid4().hex[:12]}"
    payload = json.dumps({"key": "value", "ts": time.time()}).encode("utf-8")

    client = QueuesClient(config=_make_config())
    try:
        # Send a message
        msg = QueueMessage(channel=channel, body=payload)
        send_result = client.send_queue_message(msg)
        assert not send_result.is_error, f"Send failed: {send_result.error}"

        # Receive the message
        response = client.receive_queue_messages(
            channel=channel,
            max_messages=1,
            wait_timeout_in_seconds=5,
            auto_ack=True,
        )
        assert not response.is_error, f"Receive failed: {response.error}"
        assert len(response.messages) == 1, f"Expected 1 message, got {len(response.messages)}"
        assert response.messages[0].body == payload
    finally:
        client.close()


@pytest.mark.timeout(15)
def test_result_peek_round_trip():
    """Real result store + peek retrieval round-trip using ResultBackend."""
    from kubemq_rayserve.result_backend import ResultBackend

    task_id = str(uuid.uuid4())
    result_prefix = f"test-integ-result-{uuid.uuid4().hex[:8]}-"

    client = QueuesClient(config=_make_config())
    try:
        backend = ResultBackend(
            client=client,
            result_channel_prefix=result_prefix,
            result_expiry_seconds=60,
        )

        # Store a SUCCESS result
        backend.store_result(task_id, "SUCCESS", result={"prediction": 0.95})

        # Retrieve via peek
        meta = backend.get_result(task_id)
        assert meta["status"] == "SUCCESS", f"Expected SUCCESS, got {meta['status']}"
        assert meta["result"] == {"prediction": 0.95}
        assert meta["task_id"] == task_id
    finally:
        client.close()


@pytest.mark.timeout(15)
def test_dlq_routing():
    """Real DLQ routing via re_queue when processing fails.

    Simulates the DLQ pattern the adapter uses: receive a message,
    determine it should go to the dead-letter queue, and re_queue it
    to the DLQ channel. Verifies the message arrives intact.
    """
    channel = f"test-integ-dlq-src-{uuid.uuid4().hex[:12]}"
    dlq_channel = f"test-integ-dlq-dest-{uuid.uuid4().hex[:12]}"
    payload = b"dlq-test-payload"

    client = QueuesClient(config=_make_config())
    try:
        # Send message to the source queue
        msg = QueueMessage(channel=channel, body=payload)
        send_result = client.send_queue_message(msg)
        assert not send_result.is_error, f"Send failed: {send_result.error}"

        # Receive the message and re_queue to the DLQ channel
        response = client.receive_queue_messages(
            channel=channel,
            max_messages=1,
            wait_timeout_in_seconds=5,
            auto_ack=False,
        )
        assert not response.is_error, f"Receive failed: {response.error}"
        assert len(response.messages) == 1
        response.messages[0].re_queue(dlq_channel)

        # Verify the message landed in the DLQ channel
        dlq_response = client.receive_queue_messages(
            channel=dlq_channel,
            max_messages=1,
            wait_timeout_in_seconds=5,
            auto_ack=True,
        )
        assert not dlq_response.is_error, f"DLQ receive failed: {dlq_response.error}"
        assert len(dlq_response.messages) == 1, (
            f"Expected 1 DLQ message, got {len(dlq_response.messages)}"
        )
        assert dlq_response.messages[0].body == payload
    finally:
        client.close()


@pytest.mark.timeout(15)
def test_event_progress_tracking():
    """Real Event publish/subscribe for progress updates."""


    channel = f"test-integ-events-{uuid.uuid4().hex[:12]}"
    payload = json.dumps({"task_id": "t1", "pct": 50.0, "detail": "halfway"}).encode("utf-8")

    received_events: list[bytes] = []
    event_received = threading.Event()

    def on_event(event):
        received_events.append(event.body)
        event_received.set()

    def on_error(err):
        pass  # Ignore subscription errors in test

    client = PubSubClient(config=_make_config())
    cancel = CancellationToken()
    try:
        # Subscribe first
        sub = EventsSubscription(
            channel=channel,
            on_receive_event_callback=on_event,
            on_error_callback=on_error,
        )
        client.subscribe_to_events(sub, cancel)

        # Allow subscription to establish
        time.sleep(2)

        # Publish event
        event = EventMessage(channel=channel, body=payload)
        client.send_event(event)

        # Wait for the event to arrive
        assert event_received.wait(timeout=5), "Event not received within 5s"
        assert len(received_events) >= 1
        received_data = json.loads(received_events[0])
        assert received_data["task_id"] == "t1"
        assert received_data["pct"] == 50.0
    finally:
        cancel.cancel()
        client.close()


@pytest.mark.timeout(15)
def test_query_sync_inference():
    """Real Query send/response for sync inference round-trip.

    Sets up a query subscriber that echoes back a response, then
    sends a query and verifies the response.
    """


    channel = f"test-integ-query-{uuid.uuid4().hex[:12]}"
    echo_payload = {"status": "SUCCESS", "result": {"output": 42}}

    def on_query(query_received):
        """Respond to the query with an echo payload."""
        response = QueryResponse(
            query_received=query_received,
            body=json.dumps(echo_payload).encode("utf-8"),
            is_executed=True,
        )
        responder_client.send_response_message(response)

    def on_error(err):
        pass

    responder_client = CQClient(config=_make_config("responder"))
    sender_client = CQClient(config=_make_config("sender"))
    cancel = CancellationToken()
    try:
        # Subscribe to queries
        sub = QueriesSubscription(
            channel=channel,
            on_receive_query_callback=on_query,
            on_error_callback=on_error,
        )
        responder_client.subscribe_to_queries(sub, cancel)

        # Allow subscription to establish
        time.sleep(2)

        # Send query
        query = QueryMessage(
            channel=channel,
            body=json.dumps({"task_name": "predict", "args": [1, 2]}).encode("utf-8"),
            timeout_in_seconds=10,
        )
        response = sender_client.send_query(query)

        assert response.is_executed, f"Query not executed: {response.error}"
        result_data = json.loads(response.body)
        assert result_data["status"] == "SUCCESS"
        assert result_data["result"]["output"] == 42
    finally:
        cancel.cancel()
        responder_client.close()
        sender_client.close()


@pytest.mark.timeout(15)
def test_health_check_live():
    """Health check against live broker returns ServerInfo."""
    client = QueuesClient(config=_make_config())
    try:
        info = client.ping()
        assert isinstance(info, ServerInfo), f"Expected ServerInfo, got {type(info)}"
        assert info.host, "ServerInfo.host should not be empty"
        assert info.version, "ServerInfo.version should not be empty"
        assert info.server_up_time_seconds >= 0
    finally:
        client.close()


@pytest.mark.timeout(15)
def test_autoscaling_policy_real_depth():
    """Autoscaling policy with real queue depth measurement.

    Sends messages to a queue and verifies peek_queue_messages
    reports the correct number of waiting messages (queue depth > 0).
    """
    channel = f"test-integ-depth-{uuid.uuid4().hex[:12]}"
    num_messages = 3

    client = QueuesClient(config=_make_config())
    try:
        # Send multiple messages
        for i in range(num_messages):
            msg = QueueMessage(
                channel=channel,
                body=json.dumps({"index": i}).encode("utf-8"),
            )
            send_result = client.send_queue_message(msg)
            assert not send_result.is_error, f"Send {i} failed: {send_result.error}"

        # Verify queue depth via peek (non-destructive)
        peek_result = client.peek_queue_messages(
            channel=channel,
            max_messages=10,
            wait_timeout_in_seconds=5,
        )
        assert not peek_result.is_error, f"Peek failed: {peek_result.error}"
        assert len(peek_result.messages) >= num_messages, (
            f"Expected >= {num_messages} waiting, got {len(peek_result.messages)}"
        )

        # Also verify list_queues_channels works after broker indexes the channel.
        # The broker may take several seconds to surface newly created channels
        # in the listing API, so we poll with retries.
        found = False
        for _ in range(8):
            time.sleep(1)
            channels = client.list_queues_channels(channel_search="")
            for ch in channels:
                if ch.name == channel:
                    assert ch.incoming.messages >= num_messages, (
                        f"Expected >= {num_messages} total messages, "
                        f"got {ch.incoming.messages}"
                    )
                    found = True
                    break
            if found:
                break
        assert found, f"Channel {channel} not found in list after 8s"

        # Clean up: consume the messages to avoid broker clutter
        response = client.receive_queue_messages(
            channel=channel,
            max_messages=num_messages,
            wait_timeout_in_seconds=5,
            auto_ack=True,
        )
        assert not response.is_error
    finally:
        client.close()
