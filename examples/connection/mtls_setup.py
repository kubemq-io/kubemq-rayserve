"""
mTLS Setup — Mutual TLS with Client Certificates

Demonstrates:
    - Full mTLS configuration with client and CA certificates
    - tls_cert_file for client certificate
    - tls_key_file for client private key
    - tls_ca_file for CA certificate
    - Handling connection errors when certs are not available

Usage:
    python examples/connection/mtls_setup.py

Requirements:
    - KubeMQ broker running with mTLS enabled
    - pip install kubemq-rayserve
    - Client certificate (client.pem), client key (client-key.pem), CA cert (ca.pem)

Note:
    This example constructs a valid mTLS config but will fail to connect
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

    # mTLS configuration with client cert, key, and CA cert
    config = KubeMQAdapterConfig(
        address=BROKER,
        tls=True,
        tls_cert_file="client.pem",
        tls_key_file="client-key.pem",
        tls_ca_file="ca.pem",
    )

    print("mTLS Configuration:")
    print(f"  tls={config.tls}")
    print(f"  tls_cert_file={config.tls_cert_file!r}")
    print(f"  tls_key_file={config.tls_key_file!r}")
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
        print("  - client.pem: Client certificate signed by the CA")
        print("  - client-key.pem: Client private key")
        print("  - ca.pem: CA certificate that signed both broker and client certs")
    finally:
        try:
            adapter.stop_consumer()
        except Exception:
            pass

    print("Example complete.")


if __name__ == "__main__":
    main()
