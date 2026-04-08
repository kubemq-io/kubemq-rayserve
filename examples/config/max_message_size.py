"""
Max Message Size Configuration

Demonstrates:
    - max_send_size and max_receive_size configuration
    - Setting custom size limits for large payloads
    - Default size (4 MB) vs custom sizes

Usage:
    python examples/config/max_message_size.py

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


def process_data(data: str) -> dict:
    """Process a data payload and return size info."""
    return {"input_size": len(data), "processed": True}


def main():
    channel = f"example-config-{uuid.uuid4().hex[:8]}"

    # Configure with custom message size limits
    # Default: 4_194_304 (4 MB)
    # Custom: 8_388_608 (8 MB) for larger payloads
    custom_send_size = 8_388_608  # 8 MB
    custom_receive_size = 8_388_608  # 8 MB

    config = KubeMQAdapterConfig(
        address=BROKER,
        max_send_size=custom_send_size,
        max_receive_size=custom_receive_size,
    )
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
    adapter.register_task_handler(process_data, name="process_data")
    adapter.start_consumer()

    try:
        print("Message Size Configuration:")
        send_mb = custom_send_size / 1024 / 1024
        recv_mb = custom_receive_size / 1024 / 1024
        print(f"  max_send_size:    {custom_send_size:>12,} bytes ({send_mb:.0f} MB)")
        print(f"  max_receive_size: {custom_receive_size:>12,} bytes ({recv_mb:.0f} MB)")
        print(f"  Default:          {4_194_304:>12,} bytes (4 MB)")
        print()

        # Send messages of increasing size
        test_sizes = [1_000, 10_000, 100_000, 500_000]

        for size in test_sizes:
            payload = "x" * size
            print(f"Sending {size:>10,} byte payload...", end=" ")

            try:
                result = adapter.enqueue_task_sync("process_data", args=[payload])

                # Wait for result
                for _ in range(30):
                    status = adapter.get_task_status_sync(result.id)
                    if status.status in ("SUCCESS", "FAILURE"):
                        break
                    time.sleep(0.5)

                if status.status == "SUCCESS":
                    print(f"OK (result: {status.result})")
                else:
                    print(f"FAILED: {status.status}")
            except Exception as e:
                print(f"ERROR: {e}")

        print("\nGuidelines:")
        print("  - Default 4 MB is sufficient for most inference payloads")
        print("  - Increase for image/audio/document processing tasks")
        print("  - Both send and receive limits should match")
        print("  - Broker may also have its own max message size limit")
    finally:
        adapter.stop_consumer()

    print("\nExample complete.")


if __name__ == "__main__":
    main()
