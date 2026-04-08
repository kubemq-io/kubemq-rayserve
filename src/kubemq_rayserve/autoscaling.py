"""Custom autoscaling policy for Ray Serve based on KubeMQ queue depth."""

from __future__ import annotations

import atexit
import hashlib
import logging
import math
import threading
from typing import Any

from kubemq import ClientConfig, QueuesClient
from kubemq.common.channel_stats import QueuesChannel

logger = logging.getLogger("ray.serve.kubemq")

# Module-level client cache to avoid creating new gRPC connections
# on each policy invocation (~10s interval from Ray Serve).
# Protected by _client_cache_lock per PY-156 (threading.Lock for resource protection).
_client_cache: dict[str, QueuesClient] = {}
_client_cache_lock = threading.Lock()


def _get_or_create_client(address: str, auth_token: str) -> QueuesClient:
    """Get or create a cached QueuesClient for the autoscaling policy.

    Cache key is address:hash(auth_token) to support multiple broker
    connections without embedding credentials in dict keys.
    Thread-safe: uses _client_cache_lock to prevent duplicate client creation.

    Args:
        address: KubeMQ broker address.
        auth_token: JWT auth token (may be empty).

    Returns:
        Cached or newly created QueuesClient.
    """
    token_hash = hashlib.sha256(auth_token.encode()).hexdigest()[:16] if auth_token else ""
    key = f"{address}:{token_hash}"
    with _client_cache_lock:
        if key not in _client_cache:
            config = ClientConfig(
                address=address,
                client_id=f"rayserve-autoscaler-{token_hash[:8] or 'noauth'}",
                auth_token=auth_token or None,
            )
            _client_cache[key] = QueuesClient(config=config)
        return _client_cache[key]


def _cleanup_clients() -> None:
    """Close all cached clients on process exit."""
    with _client_cache_lock:
        for client in _client_cache.values():
            try:
                client.close()
            except Exception:
                pass
        _client_cache.clear()


atexit.register(_cleanup_clients)


def kubemq_queue_depth_policy(
    ctxs: dict[str, Any],
    kubemq_address: str = "localhost:50000",
    queue_name: str = "",
    tasks_per_replica: int = 5,
    auth_token: str = "",
) -> tuple[dict[str, int], dict]:
    """Scale Ray Serve replicas based on KubeMQ queue depth.

    Queries KubeMQ queue depth via the Python SDK and returns
    desired replica counts. Intended for use with Ray Serve's
    custom autoscaling policy API.

    Args:
        ctxs: Dict mapping deployment_id to AutoscalingContext.
        kubemq_address: KubeMQ broker address (host:port).
        queue_name: Queue channel name to monitor.
        tasks_per_replica: Number of tasks per replica for scaling calculation.
        auth_token: JWT authentication token.

    Returns:
        (decisions, state) -- decisions maps deployment_id to desired
        replica count. Relies on Ray Serve's built-in downscale_delay_s
        for scale-to-zero cooldown.
    """
    decisions: dict[str, int] = {}
    state: dict = {}

    try:
        client = _get_or_create_client(kubemq_address, auth_token)
        channels: list[QueuesChannel] = client.list_queues_channels(channel_search=queue_name)

        queue_depth = 0
        for ch in channels:
            if ch.name == queue_name:
                queue_depth = ch.incoming.waiting
                break

        # min_replicas=1: scale-to-zero is not supported; Ray Serve's
        # downscale_delay_s handles cooldown when queue is empty.
        desired_replicas = max(1, math.ceil(queue_depth / tasks_per_replica))

        for deployment_id in ctxs:
            decisions[deployment_id] = desired_replicas

        state["queue_depth"] = queue_depth
        state["desired_replicas"] = desired_replicas

    except Exception as exc:
        logger.warning("Autoscaling policy error (returning current counts): %s", exc)
        # No-op fallback: return current replica counts unchanged
        for deployment_id, ctx in ctxs.items():
            decisions[deployment_id] = getattr(ctx, "current_num_replicas", 1)

    return decisions, state
