"""Unit tests for internal serialization helpers."""

from __future__ import annotations

import json

import pytest

from kubemq_rayserve._internal.serialization import (
    deserialize_result,
    deserialize_task_payload,
    generate_task_id,
    serialize_result,
    serialize_sync_query,
    serialize_sync_response,
    serialize_task_payload,
)


class TestSerializeTaskPayload:
    """Tests for serialize_task_payload and deserialize_task_payload."""

    def test_round_trip(self):
        """Serialize then deserialize produces equivalent payload."""
        data = serialize_task_payload("tid-1", "classify", ["img.jpg"], {"threshold": 0.9})
        payload = deserialize_task_payload(data)
        assert payload["task_id"] == "tid-1"
        assert payload["task_name"] == "classify"
        assert payload["args"] == ["img.jpg"]
        assert payload["kwargs"] == {"threshold": 0.9}
        assert "created_at" in payload

    def test_none_args_kwargs(self):
        """None args/kwargs default to empty list/dict."""
        data = serialize_task_payload("tid-2", "predict", None, None)
        payload = deserialize_task_payload(data)
        assert payload["args"] == []
        assert payload["kwargs"] == {}

    def test_non_serializable_raises_type_error(self):
        """Non-JSON-serializable args raise TypeError with descriptive message."""
        with pytest.raises(TypeError, match="Task argument serialization failed"):
            serialize_task_payload("tid-3", "predict", [object()], None)


class TestSerializeResult:
    """Tests for serialize_result and deserialize_result."""

    def test_success_round_trip(self):
        """Serialize/deserialize SUCCESS result round-trip."""
        data = serialize_result("tid-1", "SUCCESS", result={"label": "cat"})
        result = deserialize_result(data)
        assert result["task_id"] == "tid-1"
        assert result["status"] == "SUCCESS"
        assert result["result"] == {"label": "cat"}
        assert "completed_at" in result

    def test_failure_with_error(self):
        """Serialize/deserialize FAILURE result with error string."""
        data = serialize_result("tid-2", "FAILURE", error="RuntimeError: CUDA OOM")
        result = deserialize_result(data)
        assert result["status"] == "FAILURE"
        assert result["error"] == "RuntimeError: CUDA OOM"
        assert result["result"] is None

    def test_non_serializable_result_fallback(self):
        """Non-serializable result is caught and stored as FAILURE with error."""
        data = serialize_result("tid-ns", "SUCCESS", result=object())
        result = deserialize_result(data)
        assert result["status"] == "FAILURE"
        assert result["result"] is None
        assert "Result serialization failed" in result["error"]
        assert result["task_id"] == "tid-ns"

    def test_with_created_at(self):
        """Result with created_at preserves it."""
        data = serialize_result(
            "tid-ca", "SUCCESS", result=42, created_at="2026-04-07T00:00:00+00:00"
        )
        result = deserialize_result(data)
        assert result["created_at"] == "2026-04-07T00:00:00+00:00"


class TestSerializeSyncQuery:
    """Tests for serialize_sync_query."""

    def test_round_trip(self):
        """Serialize produces valid JSON with expected keys."""
        data = serialize_sync_query("classify", ["img.jpg"], {"model": "resnet"})
        payload = json.loads(data)
        assert payload["task_name"] == "classify"
        assert payload["args"] == ["img.jpg"]
        assert payload["kwargs"] == {"model": "resnet"}

    def test_non_serializable_raises_type_error(self):
        """Non-serializable query args raise TypeError."""
        with pytest.raises(TypeError, match="Task argument serialization failed"):
            serialize_sync_query("classify", [object()], None)


class TestSerializeSyncResponse:
    """Tests for serialize_sync_response."""

    def test_success_response(self):
        """SUCCESS response serializes correctly."""
        data = serialize_sync_response("SUCCESS", result={"label": "cat"})
        payload = json.loads(data)
        assert payload["status"] == "SUCCESS"
        assert payload["result"] == {"label": "cat"}
        assert payload["error"] is None

    def test_failure_response(self):
        """FAILURE response serializes correctly."""
        data = serialize_sync_response("FAILURE", error="handler crashed")
        payload = json.loads(data)
        assert payload["status"] == "FAILURE"
        assert payload["error"] == "handler crashed"


class TestGenerateTaskId:
    """Tests for generate_task_id."""

    def test_unique_ids(self):
        """Each call returns a unique UUID string."""
        ids = {generate_task_id() for _ in range(100)}
        assert len(ids) == 100

    def test_format(self):
        """Task ID is a valid UUID4 string."""
        tid = generate_task_id()
        assert len(tid) == 36
        assert tid.count("-") == 4
