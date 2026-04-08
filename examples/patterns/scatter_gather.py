"""
Scatter-Gather Pattern — Distribute Work and Aggregate Results

Demonstrates:
    - Creating multiple adapter instances on separate channels (scatter)
    - Enqueuing tasks to each worker channel in parallel
    - Polling all worker results until complete (gather)
    - Aggregating results from multiple channels into a single summary

Usage:
    python examples/patterns/scatter_gather.py

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

NUM_WORKERS = 3


def analyze_chunk(chunk_id: int, data: str) -> dict:
    """Simulated analysis of a data chunk."""
    time.sleep(0.1)  # Simulate work
    word_count = len(data.split())
    return {"chunk_id": chunk_id, "word_count": word_count, "status": "analyzed"}


def main():
    run_id = uuid.uuid4().hex[:8]

    # --- 1. Create one adapter per worker channel (scatter targets) ---
    workers: list[tuple[str, KubeMQTaskProcessorAdapter]] = []
    for i in range(NUM_WORKERS):
        channel = f"example-scatter-{run_id}-worker-{i}"
        config = KubeMQAdapterConfig(address=BROKER)
        adapter = KubeMQTaskProcessorAdapter(config)

        class _Cfg:
            queue_name = channel
            max_retries = 1
            failed_task_queue_name = f"{channel}.dlq"
            unprocessable_task_queue_name = ""

        adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
        adapter.register_task_handler(analyze_chunk, name="analyze_chunk")
        adapter.start_consumer()
        workers.append((channel, adapter))
        print(f"  Worker {i} started on channel={channel}")

    try:
        # --- 2. Scatter: distribute chunks across workers ---
        data_chunks = [
            "The quick brown fox jumps over the lazy dog",
            "Ray Serve provides scalable model serving infrastructure",
            "KubeMQ enables reliable message queue communication patterns",
        ]

        task_map: dict[str, tuple[int, KubeMQTaskProcessorAdapter]] = {}
        for i, chunk in enumerate(data_chunks):
            worker_idx = i % NUM_WORKERS
            _, adapter = workers[worker_idx]
            result = adapter.enqueue_task_sync("analyze_chunk", args=[i, chunk])
            task_map[result.id] = (worker_idx, adapter)
            print(f"  Scattered chunk {i} -> worker {worker_idx} (task_id={result.id})")

        # --- 3. Gather: poll all tasks until complete ---
        print("\nGathering results...")
        gathered: dict[str, dict] = {}
        timeout = 30
        start = time.perf_counter()

        while len(gathered) < len(task_map) and (time.perf_counter() - start) < timeout:
            for task_id, (worker_idx, adapter) in task_map.items():
                if task_id in gathered:
                    continue
                status = adapter.get_task_status_sync(task_id)
                if status.status in ("SUCCESS", "FAILURE"):
                    gathered[task_id] = {
                        "worker": worker_idx,
                        "status": status.status,
                        "result": status.result,
                    }
                    print(f"  Gathered task_id={task_id} from worker {worker_idx}: {status.status}")
            time.sleep(0.5)

        # --- 4. Aggregate: combine results ---
        print("\nAggregating...")
        total_words = 0
        for task_id, data in gathered.items():
            if data["status"] == "SUCCESS" and data["result"]:
                total_words += data["result"].get("word_count", 0)

        print("\nScatter-Gather complete:")
        print(f"  Workers: {NUM_WORKERS}")
        print(f"  Tasks scattered: {len(task_map)}")
        print(f"  Tasks gathered: {len(gathered)}")
        print(f"  Total word count (aggregated): {total_words}")
        print(f"  Duration: {time.perf_counter() - start:.2f}s")

    finally:
        # --- 5. Cleanup: stop all workers ---
        for channel, adapter in workers:
            adapter.stop_consumer()

    print("Example complete.")


if __name__ == "__main__":
    main()
