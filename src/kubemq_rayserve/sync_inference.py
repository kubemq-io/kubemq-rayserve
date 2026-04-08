"""Sync inference support via KubeMQ Queries.

This module provides the query subscription handler logic and sync
query dispatch. The actual integration is in adapter.py — this module
provides reusable helpers if the sync inference logic needs to be
tested or extended independently.

For v1.0.0, all sync inference code lives in adapter.py. This module
is reserved for future extraction if the sync inference handler grows
in complexity (e.g., batched queries, priority routing).
"""

from __future__ import annotations

__all__: list[str] = []
