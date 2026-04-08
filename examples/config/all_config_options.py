"""
All Configuration Options

Demonstrates:
    - Every KubeMQAdapterConfig field with explicit values
    - Pydantic model validation and defaults
    - Configuration inspection

Usage:
    python examples/config/all_config_options.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

from kubemq_rayserve import KubeMQAdapterConfig


def on_dlq_handler(task_id: str, error: str) -> None:
    print(f"DLQ: {task_id}: {error}")


def main():
    config = KubeMQAdapterConfig(
        # Connection
        address="localhost:50000",
        client_id="my-worker-01",
        auth_token="",
        # TLS / mTLS
        tls=False,
        tls_cert_file="",
        tls_key_file="",
        tls_ca_file="",
        # Result backend
        result_channel_prefix="rayserve-result-",
        result_expiry_seconds=3600,  # 1 hour
        # Consumer tuning
        consumer_poll_timeout_seconds=1,
        max_send_size=4_194_304,  # 4 MB
        max_receive_size=4_194_304,  # 4 MB
        # Sync inference
        sync_inference_timeout=30,
        # Callbacks
        on_dlq=on_dlq_handler,
        # Reserved
        visibility_timeout=120,
    )

    print("KubeMQAdapterConfig fields:")
    for field_name, field_info in config.model_fields.items():
        value = getattr(config, field_name)
        print(f"  {field_name}: {value!r}")

    print("\nJSON-serializable config:")
    print(f"  {config.model_dump_json(indent=2)}")

    print("\nExample complete.")


if __name__ == "__main__":
    main()
