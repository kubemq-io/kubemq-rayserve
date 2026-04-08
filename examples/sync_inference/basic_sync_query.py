"""
Basic Sync Query — Synchronous Request-Response Inference

Demonstrates:
    - Synchronous inference via query_task_sync()
    - KubeMQ Queries for blocking request-response
    - Immediate result without polling

Usage:
    python examples/sync_inference/basic_sync_query.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

import os
import uuid

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")


def translate(text: str, target_lang: str = "es") -> dict:
    """Simulated translation."""
    translations = {"hello": "hola", "world": "mundo", "good morning": "buenos dias"}
    translated = translations.get(text.lower(), f"[translated:{text}]")
    return {"original": text, "translated": translated, "target_lang": target_lang}


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
    adapter.register_task_handler(translate, name="translate")
    adapter.start_consumer()

    try:
        print("Sending sync inference query...")
        result = adapter.query_task_sync(
            task_name="translate",
            args=["Hello"],
            kwargs={"target_lang": "es"},
            timeout=10,
        )
        print(f"  status={result.status}")
        print(f"  result={result.result}")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
