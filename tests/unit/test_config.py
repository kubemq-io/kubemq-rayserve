"""Unit tests for KubeMQAdapterConfig."""

from __future__ import annotations

from kubemq_rayserve.config import KubeMQAdapterConfig


class TestConfig:
    """Tests for KubeMQAdapterConfig defaults and TLS."""

    def test_init_config_defaults(self):
        """U31: KubeMQAdapterConfig() -> all defaults applied correctly."""
        config = KubeMQAdapterConfig()
        assert config.address == "localhost:50000"
        assert config.client_id == ""
        assert config.auth_token == ""
        assert config.tls is False
        assert config.tls_cert_file == ""
        assert config.tls_key_file == ""
        assert config.tls_ca_file == ""
        assert config.result_channel_prefix == "rayserve-result-"
        assert config.result_expiry_seconds == 3600
        assert config.visibility_timeout == 120
        assert config.consumer_poll_timeout_seconds == 1
        assert config.max_send_size == 4_194_304
        assert config.max_receive_size == 4_194_304
        assert config.sync_inference_timeout == 30
        assert config.on_dlq is None

    def test_init_tls_config(self):
        """U32: tls=True, cert paths set -> TLSConfig constructed correctly."""
        config = KubeMQAdapterConfig(
            tls=True,
            tls_cert_file="/path/to/cert.pem",
            tls_key_file="/path/to/key.pem",
            tls_ca_file="/path/to/ca.pem",
        )
        assert config.tls is True
        assert config.tls_cert_file == "/path/to/cert.pem"
        assert config.tls_key_file == "/path/to/key.pem"
        assert config.tls_ca_file == "/path/to/ca.pem"
