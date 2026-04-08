"""
Environment Variable Configuration — All Config from Env Vars

Demonstrates:
    - Loading all KubeMQAdapterConfig fields from environment variables
    - Sensible defaults when env vars are not set
    - Production-friendly configuration pattern for containers

Usage:
    python examples/connection/env_var_config.py

    With custom env vars:
    KUBEMQ_ADDRESS=my-broker:50000 KUBEMQ_CLIENT_ID=worker-1 \
        python examples/connection/env_var_config.py

Requirements:
    - KubeMQ broker running on localhost:50000 (or KUBEMQ_ADDRESS)
    - pip install kubemq-rayserve
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

    # Build config entirely from environment variables with defaults
    config = KubeMQAdapterConfig(
        address=os.environ.get("KUBEMQ_ADDRESS", "localhost:50000"),
        client_id=os.environ.get("KUBEMQ_CLIENT_ID", ""),
        auth_token=os.environ.get("KUBEMQ_AUTH_TOKEN", ""),
        tls=os.environ.get("KUBEMQ_TLS", "false").lower() == "true",
        tls_ca_file=os.environ.get("KUBEMQ_TLS_CA_FILE", ""),
        tls_cert_file=os.environ.get("KUBEMQ_TLS_CERT_FILE", ""),
        tls_key_file=os.environ.get("KUBEMQ_TLS_KEY_FILE", ""),
    )

    print("Configuration from environment variables:")
    print(f"  address={config.address}")
    print(f"  client_id={config.client_id!r}")
    print(f"  auth_token={'[set]' if config.auth_token else '[not set]'}")
    print(f"  tls={config.tls}")
    print(f"  tls_ca_file={config.tls_ca_file!r}")
    print(f"  tls_cert_file={config.tls_cert_file!r}")
    print(f"  tls_key_file={config.tls_key_file!r}")

    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())

    try:
        healthy = adapter.health_check()
        print(f"\nHealth check: {healthy}")

        if healthy:
            print("Successfully connected using env var configuration.")
        else:
            print("Connection failed. Check KUBEMQ_ADDRESS and broker status.")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
