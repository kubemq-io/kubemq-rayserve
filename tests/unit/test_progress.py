"""Unit tests for progress tracking."""

from __future__ import annotations

from unittest.mock import MagicMock

from kubemq_rayserve.adapter import KubeMQTaskProcessorAdapter
from kubemq_rayserve.config import KubeMQAdapterConfig


class TestProgress:
    """Tests for report_progress."""

    def test_progress_tracking(self):
        """U22: report_progress(task_id, 50, 'halfway') -> EventMessage published."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        adapter._queue_name = "test-queue"

        mock_pubsub = MagicMock()
        adapter.__dict__["_pubsub_client"] = mock_pubsub

        adapter.report_progress("task-1", 50, "halfway")
        mock_pubsub.send_event.assert_called_once()
        call_args = mock_pubsub.send_event.call_args[0][0]
        assert call_args.channel == "test-queue.progress"
        assert call_args.tags["task_id"] == "task-1"
        assert call_args.tags["pct"] == "50"
