"""Unit tests for ResultBackend."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from kubemq.core.exceptions import (
    KubeMQChannelError,
    KubeMQConnectionError,
    KubeMQConnectionNotReadyError,
    KubeMQTimeoutError,
)

from kubemq_rayserve.result_backend import ResultBackend


@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def backend(mock_client):
    return ResultBackend(
        client=mock_client,
        result_channel_prefix="rayserve-result-",
        result_expiry_seconds=3600,
    )


class TestStoreResult:
    """Tests for result storage."""

    def test_result_store_success(self, backend, mock_client):
        """U10: valid TaskResult -> purge + send succeeds."""
        backend.store_result("task-1", "SUCCESS", result={"label": "cat"})
        mock_client.ack_all_queue_messages.assert_called_once()
        mock_client.send_queue_message.assert_called_once()

    def test_result_store_purge_channel_not_found(self, backend, mock_client):
        """U11: purge raises NOT_FOUND -> skip purge, send succeeds."""
        exc = KubeMQChannelError("not found")
        exc.code = MagicMock()
        with patch("kubemq_rayserve.result_backend._is_not_found", return_value=True):
            mock_client.ack_all_queue_messages.side_effect = exc
            backend.store_result("task-1", "SUCCESS")
            mock_client.send_queue_message.assert_called_once()

    def test_result_store_send_retry(self, backend, mock_client):
        """U12: send fails twice, succeeds third -> result stored."""
        mock_client.send_queue_message.side_effect = [
            Exception("fail-1"),
            Exception("fail-2"),
            None,  # success on third attempt
        ]
        with patch("kubemq_rayserve.result_backend.time.sleep"):
            backend.store_result("task-1", "SUCCESS")
        assert mock_client.send_queue_message.call_count == 3

    def test_result_store_fallback(self, backend, mock_client):
        """U13: send fails 3 times -> FAILURE stored in in-memory fallback."""
        mock_client.send_queue_message.side_effect = Exception("persistent failure")
        with patch("kubemq_rayserve.result_backend.time.sleep"):
            backend.store_result("task-1", "SUCCESS")
        assert "task-1" in backend._fallback
        assert backend._fallback["task-1"]["status"] == "FAILURE"


class TestGetResult:
    """Tests for result retrieval."""

    def test_result_retrieve_success(self, backend, mock_client):
        """U14: existing result -> TaskResult with SUCCESS status."""
        msg = MagicMock()
        msg.body = json.dumps(
            {
                "task_id": "task-1",
                "status": "SUCCESS",
                "result": {"label": "cat"},
            }
        ).encode("utf-8")
        peek_result = MagicMock()
        peek_result.is_error = False
        peek_result.messages = [msg]
        mock_client.peek_queue_messages.return_value = peek_result

        result = backend.get_result("task-1")
        assert result["status"] == "SUCCESS"
        assert result["result"] == {"label": "cat"}

    def test_result_retrieve_pending(self, backend, mock_client):
        """U15: no messages -> TaskResult(status=PENDING)."""
        peek_result = MagicMock()
        peek_result.is_error = False
        peek_result.messages = []
        mock_client.peek_queue_messages.return_value = peek_result

        result = backend.get_result("task-1")
        assert result["status"] == "PENDING"

    def test_result_retrieve_is_error_not_found(self, backend, mock_client):
        """U16: peek returns is_error=True -> PENDING."""
        peek_result = MagicMock()
        peek_result.is_error = True
        peek_result.error = "channel not found"
        peek_result.messages = []
        mock_client.peek_queue_messages.return_value = peek_result

        result = backend.get_result("task-1")
        assert result["status"] == "PENDING"

    def test_result_expiry(self, backend, mock_client):
        """U17: result expired -> PENDING after expiry."""
        # After KubeMQ expiration, peek returns empty/error
        with patch("kubemq_rayserve.result_backend._is_not_found", return_value=True):
            mock_client.peek_queue_messages.side_effect = KubeMQChannelError("not found")
            mock_client.peek_queue_messages.side_effect.code = MagicMock()
            result = backend.get_result("task-1")
            assert result["status"] == "PENDING"


class TestIsNotFound:
    """Tests for _is_not_found helper (lines 37-39)."""

    def test_is_not_found_true(self):
        """Exception with NOT_FOUND code returns True."""
        from kubemq.core.exceptions import ErrorCode

        from kubemq_rayserve.result_backend import _is_not_found

        exc = KubeMQChannelError("not found")
        exc.code = ErrorCode.NOT_FOUND
        assert _is_not_found(exc) is True

    def test_is_not_found_false(self):
        """Exception without NOT_FOUND code returns False."""
        from kubemq_rayserve.result_backend import _is_not_found

        exc = KubeMQChannelError("other error")
        exc.code = None
        assert _is_not_found(exc) is False

    def test_is_not_found_no_code_attr(self):
        """Exception without code attribute returns False."""
        from kubemq_rayserve.result_backend import _is_not_found

        exc = Exception("plain error")
        assert _is_not_found(exc) is False


class TestRetriesTotalProperty:
    """Tests for retries_total property (line 67)."""

    def test_retries_total_initial(self, backend):
        """Initial retries_total is 0."""
        assert backend.retries_total == 0

    def test_retries_total_increments(self, backend, mock_client):
        """retries_total increments on each retry."""
        mock_client.send_queue_message.side_effect = [
            Exception("fail"),
            None,
        ]
        with patch("kubemq_rayserve.result_backend.time.sleep"):
            backend.store_result("task-1", "SUCCESS")
        assert backend.retries_total == 1


class TestStorePurgeConnectionError:
    """Tests for store_result purge connection errors (lines 105-107)."""

    def test_purge_connection_error_continues(self, backend, mock_client):
        """Connection error during purge is logged but store continues."""
        mock_client.ack_all_queue_messages.side_effect = KubeMQConnectionError("conn down")
        backend.store_result("task-1", "SUCCESS")
        # send_queue_message still called
        mock_client.send_queue_message.assert_called_once()

    def test_purge_connection_not_ready_continues(self, backend, mock_client):
        """ConnectionNotReady error during purge is logged but store continues."""
        mock_client.ack_all_queue_messages.side_effect = KubeMQConnectionNotReadyError("not ready")
        backend.store_result("task-1", "SUCCESS")
        mock_client.send_queue_message.assert_called_once()

    def test_purge_channel_error_not_found_suppressed(self, backend, mock_client):
        """Channel NOT_FOUND during purge is suppressed (new channel)."""
        with patch("kubemq_rayserve.result_backend._is_not_found", return_value=True):
            exc = KubeMQChannelError("not found")
            mock_client.ack_all_queue_messages.side_effect = exc
            backend.store_result("task-1", "SUCCESS")
            mock_client.send_queue_message.assert_called_once()

    def test_purge_channel_error_not_not_found_logged(self, backend, mock_client):
        """Channel error that is NOT NOT_FOUND is logged as warning."""
        with patch("kubemq_rayserve.result_backend._is_not_found", return_value=False):
            exc = KubeMQChannelError("other error")
            mock_client.ack_all_queue_messages.side_effect = exc
            backend.store_result("task-1", "SUCCESS")
            mock_client.send_queue_message.assert_called_once()

    def test_purge_timeout_error_not_found(self, backend, mock_client):
        """Timeout error with NOT_FOUND code is suppressed."""
        with patch("kubemq_rayserve.result_backend._is_not_found", return_value=True):
            exc = KubeMQTimeoutError("timeout not found")
            mock_client.ack_all_queue_messages.side_effect = exc
            backend.store_result("task-1", "SUCCESS")
            mock_client.send_queue_message.assert_called_once()


class TestStoreFallbackEviction:
    """Tests for fallback eviction (lines 138-139)."""

    def test_fallback_eviction_when_full(self, backend, mock_client):
        """When fallback is full, oldest entry is evicted."""
        from kubemq_rayserve.result_backend import _FALLBACK_MAX_ENTRIES

        # Fill fallback to max
        for i in range(_FALLBACK_MAX_ENTRIES):
            backend._fallback[f"old-task-{i}"] = {"status": "FAILURE"}

        # Now fail a store so it falls back
        mock_client.send_queue_message.side_effect = Exception("fail")
        with patch("kubemq_rayserve.result_backend.time.sleep"):
            backend.store_result("new-task", "SUCCESS")

        assert "new-task" in backend._fallback
        # Oldest entry should have been evicted
        assert "old-task-0" not in backend._fallback
        assert len(backend._fallback) == _FALLBACK_MAX_ENTRIES


class TestGetResultFallback:
    """Tests for get_result fallback path (line 165)."""

    def test_get_result_from_fallback(self, backend, mock_client):
        """Result in fallback is returned without querying broker."""
        backend._fallback["task-fb"] = {
            "task_id": "task-fb",
            "status": "FAILURE",
            "result": None,
            "error": "storage failed",
        }
        result = backend.get_result("task-fb")
        assert result["status"] == "FAILURE"
        assert result["error"] == "storage failed"
        # Peek should NOT be called
        mock_client.peek_queue_messages.assert_not_called()


class TestGetResultConnectionErrors:
    """Tests for get_result connection error re-raise (lines 188-190)."""

    def test_get_result_connection_error_reraises(self, backend, mock_client):
        """KubeMQConnectionError is re-raised to caller."""
        mock_client.peek_queue_messages.side_effect = KubeMQConnectionError("conn down")
        with pytest.raises(KubeMQConnectionError):
            backend.get_result("task-1")

    def test_get_result_connection_not_ready_reraises(self, backend, mock_client):
        """KubeMQConnectionNotReadyError is re-raised to caller."""
        mock_client.peek_queue_messages.side_effect = KubeMQConnectionNotReadyError("not ready")
        with pytest.raises(KubeMQConnectionNotReadyError):
            backend.get_result("task-1")

    def test_get_result_channel_error_not_not_found_reraises(self, backend, mock_client):
        """Channel error that is NOT NOT_FOUND is re-raised."""
        with patch("kubemq_rayserve.result_backend._is_not_found", return_value=False):
            exc = KubeMQChannelError("other error")
            mock_client.peek_queue_messages.side_effect = exc
            with pytest.raises(KubeMQChannelError):
                backend.get_result("task-1")
