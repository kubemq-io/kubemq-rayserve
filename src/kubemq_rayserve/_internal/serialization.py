"""JSON serialization with strict error handling for task payloads."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


def serialize_task_payload(
    task_id: str,
    task_name: str,
    args: list[Any] | None,
    kwargs: dict[str, Any] | None,
) -> bytes:
    """Serialize a task enqueue payload as JSON bytes.

    Args:
        task_id: UUID4 task identifier.
        task_name: Registered handler name.
        args: Positional arguments (may be None).
        kwargs: Keyword arguments (may be None).

    Returns:
        JSON-encoded bytes.

    Raises:
        TypeError: If args or kwargs contain non-JSON-serializable objects.
    """
    payload = {
        "task_id": task_id,
        "task_name": task_name,
        "args": args or [],
        "kwargs": kwargs or {},
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        return json.dumps(payload).encode("utf-8")
    except TypeError as exc:
        raise TypeError(
            f"Task argument serialization failed: {exc}. "
            "Convert complex objects before enqueueing (e.g., ndarray.tolist()). "
            "Pluggable serializers planned for v1.1."
        ) from exc


def deserialize_task_payload(data: bytes) -> dict[str, Any]:
    """Deserialize a task payload from JSON bytes.

    Args:
        data: JSON-encoded bytes.

    Returns:
        Dict with task_id, task_name, args, kwargs, created_at.

    Raises:
        json.JSONDecodeError: If data is not valid JSON.
    """
    return json.loads(data)


def serialize_result(
    task_id: str,
    status: str,
    result: Any = None,
    error: str | None = None,
    created_at: str | None = None,
) -> bytes:
    """Serialize a task result as JSON bytes.

    Args:
        task_id: UUID4 task identifier.
        status: One of PENDING, STARTED, SUCCESS, FAILURE, CANCELLED.
        result: Handler return value (JSON-serializable, or None).
        error: Error string (present on FAILURE).
        created_at: Original task creation timestamp.

    Returns:
        JSON-encoded bytes.
    """
    payload: dict[str, Any] = {
        "task_id": task_id,
        "status": status,
        "result": result,
        "created_at": created_at or datetime.now(timezone.utc).isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    if error is not None:
        payload["error"] = error
    try:
        return json.dumps(payload).encode("utf-8")
    except TypeError as exc:
        # Non-serializable result: store error description instead
        payload["result"] = None
        payload["error"] = f"Result serialization failed: {exc}"
        payload["status"] = "FAILURE"
        return json.dumps(payload).encode("utf-8")


def deserialize_result(data: bytes) -> dict[str, Any]:
    """Deserialize a task result from JSON bytes.

    Args:
        data: JSON-encoded bytes.

    Returns:
        Dict with task_id, status, result, error, created_at, completed_at.
    """
    return json.loads(data)


def serialize_sync_query(
    task_name: str,
    args: list[Any] | None,
    kwargs: dict[str, Any] | None,
) -> bytes:
    """Serialize a sync inference query payload.

    Args:
        task_name: Registered handler name.
        args: Positional arguments.
        kwargs: Keyword arguments.

    Returns:
        JSON-encoded bytes.

    Raises:
        TypeError: If args or kwargs contain non-JSON-serializable objects.
    """
    payload = {
        "task_name": task_name,
        "args": args or [],
        "kwargs": kwargs or {},
    }
    try:
        return json.dumps(payload).encode("utf-8")
    except TypeError as exc:
        raise TypeError(
            f"Task argument serialization failed: {exc}. "
            "Convert complex objects before enqueueing (e.g., ndarray.tolist()). "
            "Pluggable serializers planned for v1.1."
        ) from exc


def serialize_sync_response(
    status: str,
    result: Any = None,
    error: str | None = None,
) -> bytes:
    """Serialize a sync inference response payload.

    Args:
        status: SUCCESS or FAILURE.
        result: Handler return value or None.
        error: Error string or None.

    Returns:
        JSON-encoded bytes.
    """
    payload: dict[str, Any] = {
        "status": status,
        "result": result,
        "error": error,
    }
    return json.dumps(payload).encode("utf-8")


def generate_task_id() -> str:
    """Generate a UUID4 task identifier."""
    return str(uuid4())
