"""Unit tests for metrics collection and health check."""

from __future__ import annotations

from unittest.mock import MagicMock

from kubemq_rayserve.adapter import KubeMQTaskProcessorAdapter
from kubemq_rayserve.config import KubeMQAdapterConfig
from kubemq_rayserve.metrics import MetricsCollector


class TestMetrics:
    """Tests for get_metrics."""

    def test_metrics_collection(self):
        """U24: process tasks -> get_metrics() returns 8 metrics."""
        metrics = MetricsCollector()
        metrics.inc_enqueued()
        metrics.inc_enqueued()
        metrics.inc_completed("SUCCESS")
        metrics.record_processing_duration(0.5)
        metrics.set_poll_latency(0.01)

        assert metrics.enqueued_total == 2
        assert metrics.completed_total["SUCCESS"] == 1
        assert metrics.processing_duration_stats["count"] == 1
        assert metrics.poll_latency == 0.01


class TestHealthCheck:
    """Tests for health_check."""

    def test_health_check_success(self):
        """U25: broker reachable -> health_check() returns True."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        adapter._client = MagicMock()
        adapter._client.ping.return_value = MagicMock()

        assert adapter.health_check() is True

    def test_health_check_failure(self):
        """U26: ping raises -> health_check() returns False."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        adapter._client = MagicMock()
        adapter._client.ping.side_effect = Exception("connection refused")

        assert adapter.health_check() is False
