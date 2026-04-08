"""
Flask Application — KubeMQ Ray Serve Adapter

Demonstrates:
    - Full Flask app with adapter lifecycle (before_request / atexit)
    - POST /tasks endpoint for async task enqueue
    - GET /tasks/<task_id> endpoint for status polling
    - POST /tasks/<task_id>/cancel endpoint for soft cancel
    - GET /health endpoint for broker health check
    - GET /metrics endpoint for adapter metrics

Usage:
    pip install flask
    python examples/web_frameworks/flask_app.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve flask
"""

from __future__ import annotations

import atexit
import os
import uuid

from flask import Flask, jsonify, request

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")
CHANNEL = f"example-flask-{uuid.uuid4().hex[:8]}"

# --- Adapter setup (module-level) ---
config = KubeMQAdapterConfig(address=BROKER)
adapter = KubeMQTaskProcessorAdapter(config)
_adapter_started = False


def echo_handler(text: str = "") -> dict:
    """Simple echo handler for demonstration."""
    return {"echo": text, "source": "flask-app"}


def _ensure_adapter_started():
    """Initialize and start the adapter once on first request."""
    global _adapter_started
    if _adapter_started:
        return

    class _Cfg:
        queue_name = CHANNEL
        max_retries = 3
        failed_task_queue_name = f"{CHANNEL}.dlq"
        unprocessable_task_queue_name = f"{CHANNEL}.unprocessable"

    adapter.initialize(consumer_concurrency=2, task_processor_config=_Cfg())
    adapter.register_task_handler(echo_handler, name="echo")
    adapter.start_consumer()
    _adapter_started = True
    print(f"Adapter started on channel={CHANNEL}")


def _shutdown_adapter():
    """Stop the adapter on process exit."""
    global _adapter_started
    if _adapter_started:
        adapter.stop_consumer()
        _adapter_started = False
        print("Adapter stopped.")


# Register shutdown hook
atexit.register(_shutdown_adapter)

# --- Flask app ---
app = Flask(__name__)


@app.before_request
def before_request():
    """Ensure the adapter is running before handling any request."""
    _ensure_adapter_started()


# --- 1. GET /health ---
@app.route("/health", methods=["GET"])
def health():
    healthy = adapter.health_check()
    return jsonify({"healthy": healthy})


# --- 2. POST /tasks ---
@app.route("/tasks", methods=["POST"])
def enqueue_task():
    data = request.get_json()
    if not data or "task_name" not in data:
        return jsonify({"error": "task_name is required"}), 400

    task_name = data["task_name"]
    args = data.get("args")
    kwargs = data.get("kwargs")

    result = adapter.enqueue_task_sync(
        task_name=task_name,
        args=args,
        kwargs=kwargs,
    )
    return jsonify({"task_id": result.id, "status": result.status})


# --- 3. GET /tasks/<task_id> ---
@app.route("/tasks/<task_id>", methods=["GET"])
def get_task_status(task_id: str):
    status = adapter.get_task_status_sync(task_id)
    return jsonify(
        {
            "task_id": task_id,
            "status": status.status,
            "result": status.result,
        }
    )


# --- 4. POST /tasks/<task_id>/cancel ---
@app.route("/tasks/<task_id>/cancel", methods=["POST"])
def cancel_task(task_id: str):
    cancelled = adapter.cancel_task(task_id)
    return jsonify({"task_id": task_id, "cancelled": cancelled})


# --- 5. GET /metrics ---
@app.route("/metrics", methods=["GET"])
def get_metrics():
    return jsonify(adapter.get_metrics())


def _self_test():
    """Run a quick self-test: start server, exercise endpoints, then shut down."""
    import threading
    import time

    import requests

    server = threading.Thread(
        target=lambda: app.run(host="127.0.0.1", port=8000, debug=False, use_reloader=False),
        daemon=True,
    )
    server.start()
    time.sleep(2)  # Wait for server to start

    base = "http://127.0.0.1:8000"
    try:
        # Health check
        r = requests.get(f"{base}/health", timeout=5)
        print(f"GET /health -> {r.status_code} {r.json()}")

        # Enqueue a task
        r = requests.post(
            f"{base}/tasks",
            json={"task_name": "echo", "kwargs": {"text": "hello flask"}},
            timeout=5,
        )
        data = r.json()
        task_id = data["task_id"]
        print(f"POST /tasks -> {r.status_code} {data}")

        # Poll for result
        time.sleep(2)
        r = requests.get(f"{base}/tasks/{task_id}", timeout=5)
        print(f"GET /tasks/{task_id} -> {r.status_code} {r.json()}")

        # Metrics
        r = requests.get(f"{base}/metrics", timeout=5)
        print(f"GET /metrics -> {r.status_code}")

        print("\nFlask self-test PASSED.")
    except Exception as exc:
        print(f"\nFlask self-test FAILED: {exc}")
    finally:
        os._exit(0)


if __name__ == "__main__":
    _self_test()
