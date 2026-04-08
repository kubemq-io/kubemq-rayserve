"""
Multiple Handlers — Async Queue-Based Inference

Demonstrates:
    - Registering 3 different task handlers on the same adapter
    - Dispatching tasks to the correct handler by task_name
    - Each handler processes its own type of work

Usage:
    python examples/async_inference/multiple_handlers.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

import os
import time
import uuid

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")


def classify(text: str) -> dict:
    """Text classification handler."""
    time.sleep(0.1)
    return {"handler": "classify", "label": "positive", "text": text}


def summarize(text: str) -> dict:
    """Text summarization handler."""
    time.sleep(0.1)
    words = text.split()
    summary = " ".join(words[:5]) + "..." if len(words) > 5 else text
    return {"handler": "summarize", "summary": summary}


def translate(text: str, target_lang: str = "es") -> dict:
    """Translation handler (simulated)."""
    time.sleep(0.1)
    return {
        "handler": "translate",
        "original": text,
        "target": target_lang,
        "translated": f"[{target_lang}] {text}",
    }


def main():
    channel = f"example-async-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())

    # Register 3 handlers
    adapter.register_task_handler(classify, name="classify")
    adapter.register_task_handler(summarize, name="summarize")
    adapter.register_task_handler(translate, name="translate")
    adapter.start_consumer()

    try:
        sample_text = "The quick brown fox jumps over the lazy dog near the river bank"

        # Dispatch to each handler by task_name
        print("Enqueuing tasks for 3 different handlers...")
        r1 = adapter.enqueue_task_sync("classify", args=[sample_text])
        print(f"  classify task_id={r1.id}")

        r2 = adapter.enqueue_task_sync("summarize", args=[sample_text])
        print(f"  summarize task_id={r2.id}")

        r3 = adapter.enqueue_task_sync(
            "translate", args=[sample_text], kwargs={"target_lang": "fr"}
        )
        print(f"  translate task_id={r3.id}")

        # Poll for all results
        print("\nPolling for results...")
        pending = {r1.id: "classify", r2.id: "summarize", r3.id: "translate"}
        for attempt in range(30):
            for task_id in list(pending.keys()):
                status = adapter.get_task_status_sync(task_id)
                if status.status in ("SUCCESS", "FAILURE"):
                    print(f"  {pending[task_id]}: status={status.status}")
                    print(f"    result={status.result}")
                    del pending[task_id]
            if not pending:
                break
            time.sleep(0.5)

        if pending:
            print(f"  Timed out waiting for: {list(pending.values())}")
    finally:
        adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
