"""
Direct Query Request — Raw KubeMQ SDK Usage

Demonstrates:
    - Using CQClient for synchronous request-response queries
    - Subscribing to queries with QueriesSubscription
    - Responding to queries with QueryResponse
    - CancellationToken for subscription lifecycle management

Usage:
    python examples/raw_sdk/direct_query_request.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

import json
import os
import time
import uuid

from kubemq import (
    CancellationToken,
    ClientConfig,
    CQClient,
    QueriesSubscription,
    QueryMessage,
    QueryResponse,
)

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")


def main():
    channel = f"example-raw-queries-{uuid.uuid4().hex[:8]}"

    # --- Responder setup ---
    responder_config = ClientConfig(
        address=BROKER,
        client_id=f"raw-responder-{uuid.uuid4().hex[:8]}",
    )
    responder = CQClient(config=responder_config)
    cancel = CancellationToken()

    def on_receive_query(query):
        """Handle incoming query and return a response."""
        request_body = json.loads(query.body.decode("utf-8"))
        print(f"  Responder received query: {request_body}")

        # Simulate processing
        result = {
            "status": "ok",
            "input": request_body,
            "result": f"Processed: {request_body.get('text', '')}",
        }

        response = QueryResponse(
            query_received=query,
            body=json.dumps(result).encode("utf-8"),
            is_executed=True,
            metadata="query-response-metadata",
        )
        responder.send_response_message(response)

    def on_error(err):
        """Handle subscription error."""
        print(f"  Subscription error: {err}")

    try:
        # Subscribe to queries (acts as the responder)
        subscription = QueriesSubscription(
            channel=channel,
            on_receive_query_callback=on_receive_query,
            on_error_callback=on_error,
        )
        responder.subscribe_to_queries(subscription, cancel)
        print(f"Responder subscribed to queries on channel: {channel}")

        # Allow subscription to establish
        time.sleep(1)

        # --- Requester setup ---
        requester_config = ClientConfig(
            address=BROKER,
            client_id=f"raw-requester-{uuid.uuid4().hex[:8]}",
        )
        requester = CQClient(config=requester_config)

        try:
            # Send a query and wait for the response
            print("\n--- Sending query ---")
            query = QueryMessage(
                channel=channel,
                body=json.dumps({"text": "Hello from raw SDK"}).encode("utf-8"),
                timeout_in_seconds=10,
            )
            response = requester.send_query(query)

            if response.is_executed:
                result = json.loads(response.body.decode("utf-8"))
                print("Query response received:")
                print(f"  is_executed: {response.is_executed}")
                print(f"  body: {result}")
                print(f"  metadata: {response.metadata}")
            else:
                print(f"Query not executed: {response.error}")

            # Send another query with tags
            print("\n--- Sending query with tags ---")
            query_with_tags = QueryMessage(
                channel=channel,
                body=json.dumps({"text": "Tagged query"}).encode("utf-8"),
                timeout_in_seconds=10,
                tags={"request-type": "tagged", "version": "1.0"},
            )
            response2 = requester.send_query(query_with_tags)

            if response2.is_executed:
                result2 = json.loads(response2.body.decode("utf-8"))
                print(f"Tagged query response: {result2}")

        finally:
            requester.close()

    finally:
        cancel.cancel()
        responder.close()

    print("\nExample complete.")


if __name__ == "__main__":
    main()
