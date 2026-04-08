"""Queue-peek result storage following the kubemq-celery backend pattern."""

from __future__ import annotations

import logging
import time
from typing import Any

from kubemq import QueueMessage, QueuesClient
from kubemq.core.exceptions import (
    KubeMQChannelError,
    KubeMQConnectionError,
    KubeMQConnectionNotReadyError,
    KubeMQTimeoutError,
)

from kubemq_rayserve._internal.serialization import deserialize_result, serialize_result

logger = logging.getLogger("ray.serve.kubemq")

# Max entries in fallback dict to prevent OOM on persistent broker failure
_FALLBACK_MAX_ENTRIES = 10_000

# Retry constants for result storage
_STORE_MAX_RETRIES = 3
_STORE_RETRY_BACKOFF_SECONDS = 1.0

# Short wait for ack_all purge (matches kubemq-celery DEFAULT_ACK_ALL_PURGE_WAIT_SECONDS)
_ACK_ALL_PURGE_WAIT_SECONDS = 1


def _is_not_found(exc: BaseException) -> bool:
    """Check if a KubeMQ exception has a NOT_FOUND error code.

    Pattern source: kubemq-celery/src/kubemq_celery/utils.py:is_not_found
    """
    from kubemq.core.exceptions import ErrorCode

    return getattr(exc, "code", None) == ErrorCode.NOT_FOUND


class ResultBackend:
    """Queue-peek result backend for Ray Serve task results.

    Stores results as KubeMQ Queue messages on per-task channels.
    Retrieves via non-destructive peek_queue_messages().
    Falls back to in-memory dict on persistent broker failure.

    Pattern source: kubemq-celery/src/kubemq_celery/backend.py:_store_result, _get_task_meta_for
    """

    def __init__(
        self,
        client: QueuesClient,
        result_channel_prefix: str,
        result_expiry_seconds: int,
    ) -> None:
        self._client = client
        self._prefix = result_channel_prefix
        self._expiry = result_expiry_seconds
        self._fallback: dict[str, dict[str, Any]] = {}
        self._retries_total = 0

    @property
    def retries_total(self) -> int:
        """Total result storage retry count (for metrics)."""
        return self._retries_total

    def _result_channel(self, task_id: str) -> str:
        """Generate result queue channel name for a task."""
        return f"{self._prefix}{task_id}"

    def store_result(
        self,
        task_id: str,
        status: str,
        result: Any = None,
        error: str | None = None,
        created_at: str | None = None,
    ) -> None:
        """Store a task result via purge-then-write.

        On persistent failure (3 retries), falls back to in-memory storage.
        NEVER re-raises — ML inference is expensive; result loss is preferable
        to task reprocessing.

        Args:
            task_id: Task identifier.
            status: Result status (PENDING, STARTED, SUCCESS, FAILURE, CANCELLED).
            result: Handler return value.
            error: Error description string.
            created_at: Original task creation timestamp.
        """
        channel = self._result_channel(task_id)
        body = serialize_result(task_id, status, result, error, created_at)

        # Step 1: Purge previous result (state transitions overwrite)
        try:
            self._client.ack_all_queue_messages(
                channel=channel,
                wait_time_seconds=_ACK_ALL_PURGE_WAIT_SECONDS,
            )
        except (KubeMQChannelError, KubeMQTimeoutError) as exc:
            if not _is_not_found(exc):
                logger.warning("Purge before store_result failed for task %s: %s", task_id, exc)
        except (KubeMQConnectionError, KubeMQConnectionNotReadyError) as exc:
            logger.warning("Connection error during result purge for task %s: %s", task_id, exc)

        # Step 2: Send new result with retry
        for attempt in range(1, _STORE_MAX_RETRIES + 1):
            try:
                msg = QueueMessage(
                    channel=channel,
                    body=body,
                    expiration_in_seconds=self._expiry,
                )
                self._client.send_queue_message(msg)
                return
            except Exception as exc:
                self._retries_total += 1  # GIL-atomic for CPython; acceptable for metric counter
                if attempt < _STORE_MAX_RETRIES:
                    logger.warning(
                        "Result storage retry attempt=%d task_id=%s error=%s",
                        attempt,
                        task_id,
                        exc,
                    )
                    time.sleep(_STORE_RETRY_BACKOFF_SECONDS)
                else:
                    logger.error(
                        "Result storage failed after %d retries task_id=%s",
                        _STORE_MAX_RETRIES,
                        task_id,
                    )
                    # Fallback to in-memory (with eviction guard)
                    if len(self._fallback) >= _FALLBACK_MAX_ENTRIES:
                        # Evict oldest entry
                        oldest_key = next(iter(self._fallback))
                        del self._fallback[oldest_key]
                    self._fallback[task_id] = {
                        "task_id": task_id,
                        "status": "FAILURE",
                        "result": None,
                        "error": f"Result storage failed: {exc}",
                    }

    def get_result(self, task_id: str) -> dict[str, Any]:
        """Retrieve a task result via non-destructive peek.

        Returns a dict with at minimum {status, result}. Returns
        {status: "PENDING", result: None} when no result exists yet.

        Args:
            task_id: Task identifier.

        Returns:
            Dict with task_id, status, result, and optional error fields.

        Raises:
            KubeMQConnectionError: On connectivity issues (caller should handle).
            KubeMQConnectionNotReadyError: On connectivity issues.
        """
        # Check in-memory fallback first
        if task_id in self._fallback:
            return self._fallback[task_id]

        channel = self._result_channel(task_id)
        try:
            result = self._client.peek_queue_messages(
                channel=channel,
                max_messages=1,
                wait_timeout_in_seconds=1,
            )
            if getattr(result, "is_error", False) is True:
                # Channel not found or other peek error — treat as PENDING
                logger.debug(
                    "Peek error on %s: %s (treating as PENDING)",
                    channel,
                    getattr(result, "error", ""),
                )
                return {"task_id": task_id, "status": "PENDING", "result": None}
            if result.messages:
                return deserialize_result(result.messages[0].body)
        except (KubeMQChannelError, KubeMQTimeoutError) as exc:
            if _is_not_found(exc):
                # Channel doesn't exist — task still pending
                return {"task_id": task_id, "status": "PENDING", "result": None}
            raise
        except (KubeMQConnectionError, KubeMQConnectionNotReadyError):
            raise

        return {"task_id": task_id, "status": "PENDING", "result": None}
