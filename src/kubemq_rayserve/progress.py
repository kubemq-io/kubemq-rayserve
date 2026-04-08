"""Task progress tracking via KubeMQ Events.

This module provides the progress tracking helper. The actual
report_progress() method is on the adapter class — this module
provides reusable helpers for progress event construction.

For v1.0.0, all progress code lives in adapter.py. This module
is reserved for future extraction if progress tracking grows
(e.g., progress aggregation, persistence, query support).
"""

from __future__ import annotations

__all__: list[str] = []
