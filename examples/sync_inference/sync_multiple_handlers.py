"""
Sync Multiple Handlers — Sync Dispatch to Different Handlers

Demonstrates:
    - Registering multiple handlers
    - Dispatching sync queries to different handlers by task_name
    - Each handler returns a different result shape
    - All using query_task_sync()

Usage:
    python examples/sync_inference/sync_multiple_handlers.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

import os
import uuid

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")


def sentiment(text: str) -> dict:
    """Simulated sentiment analysis."""
    positive_words = {"good", "great", "excellent", "happy", "love"}
    words = set(text.lower().split())
    score = len(words & positive_words) / max(len(words), 1)
    label = "positive" if score > 0.2 else "neutral"
    return {"label": label, "score": round(score, 2)}


def tokenize(text: str) -> dict:
    """Simulated tokenization."""
    tokens = text.split()
    return {"tokens": tokens, "count": len(tokens)}


def language_detect(text: str) -> dict:
    """Simulated language detection."""
    # Simple heuristic: check for common words
    spanish = {"el", "la", "de", "en", "es", "hola"}
    words = set(text.lower().split())
    lang = "es" if words & spanish else "en"
    return {"language": lang, "confidence": 0.95}


def main():
    channel = f"example-sync-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER, sync_inference_timeout=10)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(sentiment, name="sentiment")
    adapter.register_task_handler(tokenize, name="tokenize")
    adapter.register_task_handler(language_detect, name="language_detect")
    adapter.start_consumer()

    try:
        text = "This is a great and excellent product"

        # Dispatch to sentiment handler
        print("Sync query -> sentiment handler...")
        result = adapter.query_task_sync(
            task_name="sentiment",
            args=[text],
            timeout=10,
        )
        print(f"  status={result.status}")
        print(f"  result={result.result}")

        # Dispatch to tokenize handler
        print("\nSync query -> tokenize handler...")
        result = adapter.query_task_sync(
            task_name="tokenize",
            args=[text],
            timeout=10,
        )
        print(f"  status={result.status}")
        print(f"  result={result.result}")

        # Dispatch to language_detect handler
        print("\nSync query -> language_detect handler...")
        result = adapter.query_task_sync(
            task_name="language_detect",
            args=[text],
            timeout=10,
        )
        print(f"  status={result.status}")
        print(f"  result={result.result}")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
