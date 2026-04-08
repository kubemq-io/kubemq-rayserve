"""
Connection Error — Handle Broker Unreachable

Demonstrates:
    - Attempting to connect to an unreachable KubeMQ broker
    - Catching the connection exception
    - Proper error handling pattern for connection failures

Usage:
    python examples/error_handling/connection_error.py

Requirements:
    - pip install kubemq-rayserve
    - (No broker needed — this example demonstrates the failure case)
"""

from __future__ import annotations

import os
import uuid

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

# Intentionally use an unreachable address
UNREACHABLE_BROKER = "nonexistent-host:50000"
BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")


def handler(data: str) -> dict:
    return {"echo": data}


def main():
    channel = f"example-error-{uuid.uuid4().hex[:8]}"

    # --- Attempt 1: Unreachable broker ---
    print(f"Attempting connection to unreachable broker: {UNREACHABLE_BROKER}")
    config = KubeMQAdapterConfig(address=UNREACHABLE_BROKER)
    adapter = KubeMQTaskProcessorAdapter(config)

    class _Cfg:
        queue_name = channel
        max_retries = 0
        failed_task_queue_name = ""
        unprocessable_task_queue_name = ""

    try:
        adapter.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
        adapter.register_task_handler(handler, name="handler")
        adapter.start_consumer()
        # Try an operation that requires the broker
        adapter.enqueue_task_sync("handler", args=["test"])
    except Exception as exc:
        print(f"  Caught expected error: {type(exc).__name__}: {exc}")
        print("  This is the expected behavior when the broker is unreachable.")
    finally:
        try:
            adapter.stop_consumer()
        except Exception:
            pass  # Consumer may not have started

    # --- Attempt 2: Valid broker (if available) ---
    print(f"\nAttempting connection to valid broker: {BROKER}")
    config_ok = KubeMQAdapterConfig(address=BROKER)
    adapter_ok = KubeMQTaskProcessorAdapter(config_ok)

    try:
        adapter_ok.initialize(consumer_concurrency=1, task_processor_config=_Cfg())
        adapter_ok.register_task_handler(handler, name="handler")
        adapter_ok.start_consumer()

        result = adapter_ok.enqueue_task_sync("handler", args=["test"])
        print(f"  Success: task_id={result.id} status={result.status}")
    except Exception as exc:
        print(f"  Connection failed (broker may not be running): {exc}")
    finally:
        try:
            adapter_ok.stop_consumer()
        except Exception:
            pass

    print("Example complete.")


if __name__ == "__main__":
    main()
