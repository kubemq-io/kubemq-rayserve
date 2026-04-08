"""
TLS Setup — Encrypted Connection

Demonstrates:
    - Enabling TLS for encrypted broker communication
    - Configuring CA certificate path
    - Handling connection errors when certs are not available

Usage:
    python examples/connection/tls_setup.py

Requirements:
    - KubeMQ broker running with TLS enabled
    - pip install kubemq-rayserve
    - CA certificate file (ca.pem)

Note:
    This example constructs a valid TLS config but will fail to connect
    without real certificate files. It demonstrates the configuration
    pattern for production use.
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

    # TLS configuration with CA certificate
    config = KubeMQAdapterConfig(
        address=BROKER,
        tls=True,
        tls_ca_file="ca.pem",
    )

    print("TLS Configuration:")
    print(f"  tls={config.tls}")
    print(f"  tls_ca_file={config.tls_ca_file!r}")

    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    try:
        adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
        healthy = adapter.health_check()
        print(f"Health check: {healthy}")
    except Exception as exc:
        print(f"Expected error (no TLS certs available): {exc}")
        print("In production, provide real certificate paths:")
        print("  - ca.pem: CA certificate that signed the broker's certificate")
    finally:
        try:
            adapter.stop_consumer()
        except Exception:
            pass

    print("Example complete.")


if __name__ == "__main__":
    main()
