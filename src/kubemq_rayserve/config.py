"""KubeMQ adapter configuration for Ray Serve."""

from __future__ import annotations

from collections.abc import Callable

from pydantic import BaseModel, Field


class KubeMQAdapterConfig(BaseModel):
    """Configuration for KubeMQTaskProcessorAdapter.

    Passed via TaskProcessorConfig.adapter_config when deploying
    a Ray Serve @task_consumer.

    Attributes:
        address: KubeMQ broker address (host:port).
        client_id: Client identifier. Auto-generated UUID suffix if empty.
        auth_token: JWT authentication token.
        tls: Enable TLS connection.
        tls_cert_file: mTLS client certificate path.
        tls_key_file: mTLS client key path.
        tls_ca_file: CA certificate path.
        result_channel_prefix: Prefix for result storage channels.
        result_expiry_seconds: Result TTL in seconds (1 hour default).
        visibility_timeout: Reserved for future SDK support — not currently wired.
        consumer_poll_timeout_seconds: Queue poll wait timeout (seconds).
        max_send_size: Max message send size in bytes.
        max_receive_size: Max message receive size in bytes.
        sync_inference_timeout: Default timeout for sync Query inference (seconds).
        on_dlq: Callback when task moves to DLQ: (task_id, error).
    """

    address: str = "localhost:50000"
    client_id: str = ""
    auth_token: str = Field(default="", repr=False)
    tls: bool = False
    tls_cert_file: str = ""
    tls_key_file: str = ""
    tls_ca_file: str = ""
    result_channel_prefix: str = "rayserve-result-"
    result_expiry_seconds: int = Field(default=3600, ge=0, le=86400)
    visibility_timeout: int = 120  # Reserved for future SDK support — not currently wired
    consumer_poll_timeout_seconds: int = 1
    max_send_size: int = 4_194_304
    max_receive_size: int = 4_194_304
    sync_inference_timeout: int = 30
    on_dlq: Callable[[str, str], None] | None = Field(default=None, exclude=True)

    model_config = {"arbitrary_types_allowed": True}
