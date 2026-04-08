"""End-to-end tests requiring a live KubeMQ broker + Ray Serve cluster."""

from __future__ import annotations

import pytest

# E2E tests verify full Ray Serve integration.
# Implementation deferred to Phase 11 — follows spec Section 6.3.

pytestmark = pytest.mark.e2e


@pytest.mark.skip(reason="Requires live broker + Ray Serve — deferred to Phase 11")
def test_full_task_lifecycle():
    """Full @task_consumer -> enqueue -> process -> result cycle."""


@pytest.mark.skip(reason="Requires live broker + Ray Serve — deferred to Phase 11")
def test_sync_inference_e2e():
    """Sync inference via query_task_sync end-to-end."""


@pytest.mark.skip(reason="Requires live broker + Ray Serve — deferred to Phase 11")
def test_progress_tracking_e2e():
    """Progress tracking observable by external subscriber."""


@pytest.mark.skip(reason="Requires live broker + Ray Serve — deferred to Phase 11")
def test_consumer_stop_redelivery():
    """Consumer stop with nack causes messages to be redelivered."""


@pytest.mark.skip(reason="Requires live broker + Ray Serve — deferred to Phase 11")
def test_autoscaling_replica_scaling():
    """Autoscaling policy triggers replica scaling."""
