"""In-memory metrics collection for the adapter."""

from __future__ import annotations

import threading


class MetricsCollector:
    """Thread-safe in-memory metrics for KubeMQTaskProcessorAdapter.

    Tracks counter and histogram metrics. Gauge metrics (queue_depth,
    in_flight, dlq_depth, poll_latency) are queried live in get_metrics().

    Thread safety: all mutations use a threading.Lock.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._enqueued_total = 0
        self._completed_total: dict[str, int] = {"SUCCESS": 0, "FAILURE": 0}
        self._duration_min = float("inf")
        self._duration_max = 0.0
        self._duration_sum = 0.0
        self._duration_count = 0
        self._poll_latency = 0.0

    @property
    def enqueued_total(self) -> int:
        """Total tasks enqueued."""
        with self._lock:
            return self._enqueued_total

    @property
    def completed_total(self) -> dict[str, int]:
        """Total tasks completed by status."""
        with self._lock:
            return dict(self._completed_total)

    @property
    def processing_duration_stats(self) -> dict[str, float]:
        """Processing duration statistics (min, max, avg, count).

        Uses O(1) memory running statistics instead of storing all durations.
        """
        with self._lock:
            if self._duration_count == 0:
                return {"min": 0.0, "max": 0.0, "avg": 0.0, "count": 0}
            return {
                "min": self._duration_min,
                "max": self._duration_max,
                "avg": self._duration_sum / self._duration_count,
                "count": self._duration_count,
            }

    @property
    def poll_latency(self) -> float:
        """Last consumer poll latency in seconds."""
        with self._lock:
            return self._poll_latency

    def inc_enqueued(self) -> None:
        """Increment tasks_enqueued_total counter."""
        with self._lock:
            self._enqueued_total += 1

    def inc_completed(self, status: str) -> None:
        """Increment tasks_completed_total counter for a status.

        Args:
            status: "SUCCESS" or "FAILURE".
        """
        with self._lock:
            self._completed_total[status] = self._completed_total.get(status, 0) + 1

    def record_processing_duration(self, duration_seconds: float) -> None:
        """Record a task processing duration.

        Uses O(1) running statistics — no unbounded list growth.

        Args:
            duration_seconds: Time from handler start to handler return.
        """
        with self._lock:
            self._duration_count += 1
            self._duration_sum += duration_seconds
            if duration_seconds < self._duration_min:
                self._duration_min = duration_seconds
            if duration_seconds > self._duration_max:
                self._duration_max = duration_seconds

    def set_poll_latency(self, latency_seconds: float) -> None:
        """Set the last consumer poll latency.

        Args:
            latency_seconds: Time for the last receive_queue_messages call.
        """
        with self._lock:
            self._poll_latency = latency_seconds
