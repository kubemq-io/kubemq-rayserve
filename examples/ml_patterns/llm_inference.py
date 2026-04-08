"""
LLM Inference — Prompt to Completion with Progress Reporting

Demonstrates:
    - LLM-style prompt -> completion inference pattern
    - Progress reporting during token generation (simulated)
    - Explicit task_id kwarg pattern for progress tracking
    - Simulated streaming-like progress updates

Usage:
    python examples/ml_patterns/llm_inference.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

import hashlib
import os
import time
import uuid

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")

# Module-level adapter reference for handler closure
adapter: KubeMQTaskProcessorAdapter | None = None


def generate_completion(
    prompt: str,
    max_tokens: int = 50,
    temperature: float = 0.7,
    task_id: str = "",
) -> dict:
    """Simulated LLM text generation with progress reporting."""
    # In production: model = transformers.AutoModelForCausalLM.from_pretrained("llama-2-7b")
    # In production: tokenizer = transformers.AutoTokenizer.from_pretrained("llama-2-7b")
    assert adapter is not None

    # Simulated token vocabulary for deterministic output
    vocab = [
        "the",
        "a",
        "is",
        "was",
        "in",
        "to",
        "and",
        "of",
        "that",
        "it",
        "for",
        "with",
        "on",
        "as",
        "this",
        "an",
        "be",
        "at",
        "by",
        "from",
        "which",
        "data",
        "model",
        "system",
        "process",
        "output",
        "input",
        "result",
        "using",
        "based",
        "can",
        "will",
        "have",
        "been",
        "are",
        "more",
        "also",
        "each",
        "into",
        "like",
        "new",
        "use",
        "way",
        "most",
        "time",
        "very",
        "when",
        "over",
        "such",
        "only",
        "its",
        "about",
    ]

    # Report starting progress
    if task_id:
        adapter.report_progress(task_id, 0, "Tokenizing prompt")

    time.sleep(0.05)  # Simulate tokenization

    # Generate tokens with progress reporting
    generated_tokens = []
    prompt_hash = hashlib.md5(prompt.encode()).hexdigest()

    for i in range(max_tokens):
        # Simulate per-token generation latency
        time.sleep(0.02)  # Simulate 20ms per token

        # Deterministic token selection based on prompt hash and position
        idx = (int(prompt_hash[i % len(prompt_hash)], 16) + i) % len(vocab)
        generated_tokens.append(vocab[idx])

        # Report progress every 10 tokens
        pct = int((i + 1) / max_tokens * 100)
        if (i + 1) % 10 == 0 and task_id:
            adapter.report_progress(task_id, pct, f"Generated {i + 1}/{max_tokens} tokens")

    completion_text = " ".join(generated_tokens)

    # Report completion
    if task_id:
        adapter.report_progress(task_id, 100, "Generation complete")

    return {
        "prompt": prompt,
        "completion": completion_text,
        "tokens_generated": max_tokens,
        "model": "simulated-llm-7b",
        "temperature": temperature,
        "finish_reason": "length",
    }


def poll_result(adpt: KubeMQTaskProcessorAdapter, task_id: str, timeout: int = 30) -> dict | None:
    """Poll until task completes or timeout."""
    start = time.perf_counter()
    while (time.perf_counter() - start) < timeout:
        status = adpt.get_task_status_sync(task_id)
        if status.status in ("SUCCESS", "FAILURE"):
            return {"status": status.status, "result": status.result}
        time.sleep(0.5)
    return None


def main():
    global adapter

    channel = f"example-ml-llm-{uuid.uuid4().hex[:8]}"

    config = KubeMQAdapterConfig(address=BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 1
        failed_task_queue_name = f"{channel}.dlq"
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(generate_completion, name="generate")
    adapter.start_consumer()

    try:
        prompts = [
            {
                "prompt": "Explain how message queues work in distributed systems",
                "max_tokens": 40,
                "temperature": 0.7,
            },
            {
                "prompt": "What are the benefits of using Ray Serve for model serving",
                "max_tokens": 30,
                "temperature": 0.5,
            },
            {
                "prompt": "Describe the architecture of a scalable ML pipeline",
                "max_tokens": 50,
                "temperature": 0.8,
            },
        ]

        print("LLM Inference Pipeline")
        print("  Model: simulated-llm-7b")
        print(f"  Queue: {channel}")

        for i, params in enumerate(prompts):
            print(f"\n--- Prompt {i + 1} ---")
            print(f"  Prompt: {params['prompt']}")
            print(f"  Max tokens: {params['max_tokens']}, Temperature: {params['temperature']}")

            # Use explicit task_id kwarg for progress reporting
            known_task_id = f"llm-task-{uuid.uuid4().hex[:8]}"
            result = adapter.enqueue_task_sync(
                "generate",
                kwargs={
                    "prompt": params["prompt"],
                    "max_tokens": params["max_tokens"],
                    "temperature": params["temperature"],
                    "task_id": known_task_id,
                },
            )
            print(f"  Enqueued: task_id={result.id}, progress_id={known_task_id}")

            outcome = poll_result(adapter, result.id)
            if outcome and outcome["status"] == "SUCCESS":
                r = outcome["result"]
                print(f"  Completion ({r['tokens_generated']} tokens): {r['completion'][:80]}...")
                print(f"  Finish reason: {r['finish_reason']}")
            elif outcome:
                print(f"  Failed: {outcome}")
            else:
                print("  Timed out waiting for completion.")
    finally:
        adapter.stop_consumer()

    print("\nExample complete.")


if __name__ == "__main__":
    main()
