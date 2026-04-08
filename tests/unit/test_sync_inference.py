"""Unit tests for sync inference (query_task_sync)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
from kubemq.core.exceptions import KubeMQTimeoutError

from kubemq_rayserve.adapter import KubeMQTaskProcessorAdapter
from kubemq_rayserve.config import KubeMQAdapterConfig


@pytest.fixture
def adapter_with_cq():
    """Adapter with mocked CQ client for sync inference."""
    config = KubeMQAdapterConfig()
    adapter = KubeMQTaskProcessorAdapter(config)
    adapter._queue_name = "test-queue"

    mock_cq = MagicMock()
    # Set cached_property directly
    adapter.__dict__["_cq_client"] = mock_cq
    return adapter, mock_cq


class TestSyncInference:
    """Tests for query_task_sync."""

    def test_sync_inference_success(self, adapter_with_cq):
        """U19: valid query, handler returns -> TaskResult with result."""
        adapter, mock_cq = adapter_with_cq
        response = MagicMock()
        response.body = json.dumps(
            {
                "status": "SUCCESS",
                "result": {"predictions": [{"label": "cat", "confidence": 0.97}]},
                "error": None,
            }
        ).encode("utf-8")
        mock_cq.send_query.return_value = response

        result = adapter.query_task_sync("classify", kwargs={"image": "test.jpg"})
        assert result.status == "SUCCESS"
        assert result.result == {"predictions": [{"label": "cat", "confidence": 0.97}]}

    def test_sync_inference_timeout(self, adapter_with_cq):
        """U20: query timeout exceeded -> KubeMQTimeoutError raised."""
        adapter, mock_cq = adapter_with_cq
        mock_cq.send_query.side_effect = KubeMQTimeoutError("deadline exceeded")

        with pytest.raises(KubeMQTimeoutError):
            adapter.query_task_sync("classify", timeout=1)

    def test_sync_inference_no_handler(self, adapter_with_cq):
        """U21: unknown task_name -> error response returned."""
        adapter, mock_cq = adapter_with_cq
        # Simulate what happens when no subscriber exists
        mock_cq.send_query.side_effect = KubeMQTimeoutError("no subscriber")

        with pytest.raises(KubeMQTimeoutError):
            adapter.query_task_sync("unknown_task")


class TestHandleSyncQuery:
    """Tests for _handle_sync_query (consumer-side handler)."""

    @pytest.fixture
    def handler_adapter(self):
        """Adapter with mocked CQ client and a registered task handler."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        adapter._queue_name = "test-queue"

        mock_cq = MagicMock()
        adapter.__dict__["_cq_client"] = mock_cq

        def classify_handler(*args, **kwargs):
            return {"label": "cat", "confidence": 0.97}

        adapter._task_handlers = {"classify": classify_handler}
        return adapter, mock_cq

    def test_handle_sync_query_success(self, handler_adapter):
        """Handler receives valid query, dispatches to handler, sends correct QueryResponse."""
        adapter, mock_cq = handler_adapter

        query = MagicMock()
        query.body = json.dumps(
            {
                "task_name": "classify",
                "args": [],
                "kwargs": {"image": "test.jpg"},
            }
        ).encode("utf-8")
        query.reply_channel = "reply-channel"
        query.id = "req-123"

        adapter._handle_sync_query(query)

        mock_cq.send_response_message.assert_called_once()
        response = mock_cq.send_response_message.call_args[0][0]
        assert response.query_received is query
        assert response.is_executed is True
        body = json.loads(response.body)
        assert body["status"] == "SUCCESS"

    def test_handle_sync_query_unknown_handler(self, handler_adapter):
        """Handler receives query for unregistered task, sends FAILURE response."""
        adapter, mock_cq = handler_adapter

        query = MagicMock()
        query.body = json.dumps(
            {
                "task_name": "nonexistent",
                "args": [],
                "kwargs": {},
            }
        ).encode("utf-8")
        query.reply_channel = "reply-channel"
        query.id = "req-456"

        adapter._handle_sync_query(query)

        mock_cq.send_response_message.assert_called_once()
        response = mock_cq.send_response_message.call_args[0][0]
        body = json.loads(response.body)
        assert body["status"] == "FAILURE"
        assert "Unknown task handler" in body["error"]

    def test_handle_sync_query_handler_exception(self, handler_adapter):
        """Handler raises exception, sends FAILURE response with error detail."""
        adapter, mock_cq = handler_adapter
        adapter._task_handlers["classify"] = MagicMock(side_effect=ValueError("bad input data"))

        query = MagicMock()
        query.body = json.dumps(
            {
                "task_name": "classify",
                "args": [],
                "kwargs": {},
            }
        ).encode("utf-8")
        query.reply_channel = "reply-channel"
        query.id = "req-789"

        adapter._handle_sync_query(query)

        mock_cq.send_response_message.assert_called_once()
        response = mock_cq.send_response_message.call_args[0][0]
        body = json.loads(response.body)
        assert body["status"] == "FAILURE"
        assert "ValueError" in body["error"]
        assert "bad input data" in body["error"]
