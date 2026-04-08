"""
Priority Queues Pattern — High / Medium / Low Priority Processing

Demonstrates:
    - Three adapters on separate priority channels (high, medium, low)
    - Enqueueing tasks to different priority levels
    - Consuming from high-priority first, then medium, then low
    - Priority-based task routing and processing order

Usage:
    python examples/patterns/priority_queues.py

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

PRIORITIES = ["high", "medium", "low"]


def process_task(priority: str, task_id: str, payload: str) -> dict:
    """Simulated task processing with priority-aware logging."""
    # Simulate work: high priority = fast, low priority = slower
    delay = {"high": 0.05, "medium": 0.1, "low": 0.2}.get(priority, 0.1)
    time.sleep(delay)
    return {
        "priority": priority,
        "task_id": task_id,
        "payload": payload,
        "processed": True,
    }


def main():
    run_id = uuid.uuid4().hex[:8]

    # --- 1. Create one adapter per priority level ---
    priority_adapters: dict[str, KubeMQTaskProcessorAdapter] = {}
    for priority in PRIORITIES:
        channel = f"example-priority-{run_id}-{priority}"
        config = KubeMQAdapterConfig(address=BROKER)
        adapter = KubeMQTaskProcessorAdapter(config)

        class _Cfg:
            queue_name = channel
            max_retries = 1
            failed_task_queue_name = f"{channel}.dlq"
            unprocessable_task_queue_name = ""

        adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
        adapter.register_task_handler(process_task, name="process_task")
        adapter.start_consumer()
        priority_adapters[priority] = adapter
        print(f"  Priority '{priority}' adapter started on channel={channel}")

    try:
        # --- 2. Enqueue tasks at different priority levels ---
        tasks_to_enqueue = [
            ("low", "Log cleanup routine"),
            ("low", "Generate weekly report"),
            ("medium", "Process user upload"),
            ("high", "Handle payment webhook"),
            ("medium", "Send notification email"),
            ("high", "Critical security alert"),
            ("low", "Archive old records"),
        ]

        enqueued: list[tuple[str, str, str]] = []  # (priority, task_id, description)
        print(f"\nEnqueuing {len(tasks_to_enqueue)} tasks across priority levels...")
        for priority, description in tasks_to_enqueue:
            adapter = priority_adapters[priority]
            task_ref = uuid.uuid4().hex[:8]
            result = adapter.enqueue_task_sync(
                "process_task",
                kwargs={"priority": priority, "task_id": task_ref, "payload": description},
            )
            enqueued.append((priority, result.id, description))
            print(f"  [{priority:6s}] Enqueued: '{description}' (task_id={result.id})")

        # --- 3. Process in priority order: high first, then medium, then low ---
        print("\nProcessing in priority order (high -> medium -> low)...")
        time.sleep(2)  # Allow processing time

        for priority in PRIORITIES:
            priority_tasks = [(tid, desc) for p, tid, desc in enqueued if p == priority]
            adapter = priority_adapters[priority]

            print(f"\n  --- {priority.upper()} priority ({len(priority_tasks)} tasks) ---")
            for task_id, description in priority_tasks:
                status = adapter.get_task_status_sync(task_id)
                print(f"    '{description}': status={status.status}")

        # --- 4. Summary ---
        counts = {p: sum(1 for pr, _, _ in enqueued if pr == p) for p in PRIORITIES}
        print("\nPriority Queues complete:")
        for priority in PRIORITIES:
            print(f"  {priority}: {counts[priority]} tasks")
        print(f"  Total: {len(enqueued)} tasks")

    finally:
        # --- 5. Cleanup: stop all priority adapters ---
        for priority in PRIORITIES:
            priority_adapters[priority].stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
