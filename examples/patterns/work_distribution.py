"""
Work Distribution Pattern — N Consumers on Same Queue

Demonstrates:
    - Multiple adapter consumers attached to the same KubeMQ channel
    - KubeMQ's built-in load balancing distributes tasks across consumers
    - Each consumer processes a subset of the total tasks
    - Automatic work distribution without explicit routing

Usage:
    python examples/patterns/work_distribution.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve
"""

from __future__ import annotations

import os
import threading
import time
import uuid

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")

NUM_CONSUMERS = 3
NUM_TASKS = 9

# Track which consumer processes each task (thread-safe)
_processing_log: list[tuple[int, str]] = []
_log_lock = threading.Lock()


def make_handler(consumer_id: int):
    """Create a handler function that logs which consumer processes the task."""

    def process_work(item_id: str) -> dict:
        time.sleep(0.2)  # Simulate work
        with _log_lock:
            _processing_log.append((consumer_id, item_id))
        return {"consumer_id": consumer_id, "item_id": item_id, "processed": True}

    return process_work


def main():
    run_id = uuid.uuid4().hex[:8]
    channel = f"example-workdist-{run_id}"

    # --- 1. Create N consumers on the SAME channel ---
    consumers: list[KubeMQTaskProcessorAdapter] = []
    for i in range(NUM_CONSUMERS):
        config = KubeMQAdapterConfig(
            address=BROKER,
            client_id=f"work-dist-consumer-{i}",
        )
        adapter = KubeMQTaskProcessorAdapter(config)

        class _Cfg:
            queue_name = channel
            max_retries = 1
            failed_task_queue_name = f"{channel}.dlq"
            unprocessable_task_queue_name = ""

        adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
        adapter.register_task_handler(make_handler(i), name="process_work")
        adapter.start_consumer()
        consumers.append(adapter)
        print(f"  Consumer {i} started on shared channel={channel}")

    try:
        # --- 2. Enqueue tasks using the first consumer's adapter (any would work) ---
        producer = consumers[0]
        task_ids: list[str] = []

        print(f"\nEnqueuing {NUM_TASKS} tasks to shared channel...")
        for t in range(NUM_TASKS):
            item_id = f"item-{t:03d}"
            result = producer.enqueue_task_sync("process_work", kwargs={"item_id": item_id})
            task_ids.append(result.id)
            print(f"  Enqueued: {item_id} (task_id={result.id})")

        # --- 3. Wait for all tasks to complete ---
        print("\nWaiting for all tasks to complete...")
        timeout = 30
        start = time.perf_counter()
        completed = set()

        while len(completed) < NUM_TASKS and (time.perf_counter() - start) < timeout:
            for task_id in task_ids:
                if task_id in completed:
                    continue
                # Check status via producer (result backend is shared)
                status = producer.get_task_status_sync(task_id)
                if status.status in ("SUCCESS", "FAILURE"):
                    completed.add(task_id)
            time.sleep(0.5)

        # --- 4. Report distribution ---
        print("\nWork distribution results:")
        print(f"  Total tasks: {NUM_TASKS}")
        print(f"  Consumers: {NUM_CONSUMERS}")
        print(f"  Completed: {len(completed)}")
        print(f"  Duration: {time.perf_counter() - start:.2f}s")

        # Show per-consumer breakdown
        with _log_lock:
            consumer_counts: dict[int, int] = {}
            for consumer_id, item_id in _processing_log:
                consumer_counts[consumer_id] = consumer_counts.get(consumer_id, 0) + 1

        print("\n  Per-consumer breakdown:")
        for cid in range(NUM_CONSUMERS):
            count = consumer_counts.get(cid, 0)
            print(f"    Consumer {cid}: {count} tasks")

    finally:
        # --- 5. Cleanup: stop all consumers ---
        for adapter in consumers:
            adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
