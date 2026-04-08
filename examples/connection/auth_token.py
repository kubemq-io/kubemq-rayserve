"""
Auth Token — JWT Authentication

Demonstrates:
    - Configuring a JWT authentication token for broker access
    - Handling authentication errors when token is invalid
    - auth_token is masked in config repr (Pydantic repr=False)

Usage:
    python examples/connection/auth_token.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
    - Valid JWT token (example uses a placeholder that will fail auth)
"""

from __future__ import annotations

import os
import uuid

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")


def dummy_handler(x: int) -> int:
    return x * 2


def main():
    channel = f"example-connection-{uuid.uuid4().hex[:8]}"

    # Configure with an auth token (use a real JWT in production)
    auth_token = os.environ.get(
        "KUBEMQ_AUTH_TOKEN",
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.placeholder",
    )

    config = KubeMQAdapterConfig(
        address=BROKER,
        auth_token=auth_token,
    )

    # Note: auth_token is hidden in repr for security
    print(f"Config: {config}")
    print(f"Auth token set: {bool(config.auth_token)}")

    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    try:
        adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
        healthy = adapter.health_check()
        print(f"Health check with auth: {healthy}")
    except Exception as exc:
        print(f"Connection error (expected if token is invalid): {exc}")
        print("In production, use a valid JWT token from your KubeMQ license.")
    finally:
        try:
            adapter.stop_consumer()
        except Exception:
            pass

    print("Example complete.")


if __name__ == "__main__":
    main()
