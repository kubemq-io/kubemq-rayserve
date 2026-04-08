"""
FastAPI Application — KubeMQ Ray Serve Adapter

Demonstrates:
    - Full FastAPI app with adapter lifecycle (startup/shutdown)
    - POST /tasks endpoint for async task enqueue
    - GET /tasks/{task_id} endpoint for status polling
    - POST /tasks/{task_id}/cancel endpoint for soft cancel
    - GET /tasks/{task_id}/progress SSE endpoint for real-time progress
    - GET /health endpoint for broker health check
    - GET /metrics endpoint for adapter metrics
    - POST /sync-inference endpoint for synchronous query inference

Usage:
    pip install fastapi uvicorn
    python examples/web_frameworks/fastapi_app.py

Requirements:
    - KubeMQ broker running on localhost:50000
    - pip install kubemq-rayserve fastapi uvicorn
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from kubemq import (
    CancellationToken,
    ClientConfig,
    EventReceived,
    EventsSubscription,
    PubSubClient,
)
from pydantic import BaseModel, Field

from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

BROKER = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")
CHANNEL = f"example-fastapi-{uuid.uuid4().hex[:8]}"

# --- Adapter setup (module-level for lifespan access) ---
# Note: Module-level adapter works for single-process deployment.
# For --reload, use the lifespan pattern exclusively or manage adapter
# state within the lifespan context (e.g., store on app.state).
config = KubeMQAdapterConfig(address=BROKER)
adapter = KubeMQTaskProcessorAdapter(config)


def echo_handler(text: str = "") -> dict:
    """Simple echo handler for demonstration."""
    return {"echo": text, "source": "fastapi-app"}


# --- Request / Response models ---
class EnqueueRequest(BaseModel):
    task_name: str
    args: list | None = None
    kwargs: dict | None = None


class SyncInferenceRequest(BaseModel):
    task_name: str
    args: list | None = None
    kwargs: dict | None = None
    timeout: int = Field(default=30, ge=1, le=300)


# --- Lifespan: adapter startup and shutdown ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize adapter on startup, stop on shutdown."""

    class _Cfg:
        queue_name = CHANNEL
        max_retries = 3
        failed_task_queue_name = f"{CHANNEL}.dlq"
        unprocessable_task_queue_name = f"{CHANNEL}.unprocessable"

    adapter.initialize(consumer_concurrency=2, task_processor_config=_Cfg())
    adapter.register_task_handler(echo_handler, name="echo")
    adapter.start_consumer()
    print(f"Adapter started on channel={CHANNEL}")
    yield
    adapter.stop_consumer()
    print("Adapter stopped.")


app = FastAPI(title="KubeMQ Ray Serve Example", lifespan=lifespan)


# --- 1. GET /health ---
@app.get("/health")
def health():
    healthy = adapter.health_check()
    return {"healthy": healthy}


# --- 2. POST /tasks ---
@app.post("/tasks")
def enqueue_task(req: EnqueueRequest):
    result = adapter.enqueue_task_sync(
        task_name=req.task_name,
        args=req.args,
        kwargs=req.kwargs,
    )
    return {"task_id": result.id, "status": result.status}


# --- 3. GET /tasks/{task_id} ---
@app.get("/tasks/{task_id}")
def get_task_status(task_id: str):
    status = adapter.get_task_status_sync(task_id)
    return {"task_id": task_id, "status": status.status, "result": status.result}


# --- 4. POST /tasks/{task_id}/cancel ---
@app.post("/tasks/{task_id}/cancel")
def cancel_task(task_id: str):
    cancelled = adapter.cancel_task(task_id)
    return {"task_id": task_id, "cancelled": cancelled}


# --- 5. GET /tasks/{task_id}/progress (SSE) ---
@app.get("/tasks/{task_id}/progress")
async def task_progress_sse(task_id: str):
    """Server-Sent Events stream for real-time progress updates.

    Subscribes to the {CHANNEL}.progress PubSub channel and filters
    events by task_id tag. Yields SSE data frames until 100% or timeout.
    """
    progress_queue: asyncio.Queue[dict] = asyncio.Queue()
    cancel_token = CancellationToken()
    loop = asyncio.get_running_loop()

    def on_event(event: EventReceived) -> None:
        """Thread-safe callback: SDK fires this from a background thread."""
        try:
            data = json.loads(event.body)
            if data.get("task_id") == task_id:
                loop.call_soon_threadsafe(progress_queue.put_nowait, data)
        except (json.JSONDecodeError, TypeError):
            pass

    def on_error(error: str) -> None:
        pass  # Logged internally by SDK

    # Subscribe to progress channel using public config fields (avoid private API)
    pubsub = PubSubClient(
        config=ClientConfig(
            address=BROKER,
            client_id="fastapi-progress-subscriber",
        )
    )
    subscription = EventsSubscription(
        channel=f"{CHANNEL}.progress",
        on_receive_event_callback=on_event,
        on_error_callback=on_error,
    )
    pubsub.subscribe_to_events(subscription, cancel_token)

    async def event_stream():
        try:
            timeout = 120  # max SSE duration seconds
            while timeout > 0:
                try:
                    data = await asyncio.wait_for(progress_queue.get(), timeout=5.0)
                    yield f"data: {json.dumps(data)}\n\n"
                    if data.get("pct", 0) >= 100:
                        break
                    timeout -= 5
                except asyncio.TimeoutError:
                    yield f"data: {json.dumps({'heartbeat': True})}\n\n"
                    timeout -= 5
        finally:
            cancel_token.cancel()
            try:
                pubsub.close()
            except Exception:
                pass

    return StreamingResponse(event_stream(), media_type="text/event-stream")


# --- 6. GET /metrics ---
@app.get("/metrics")
def get_metrics():
    return adapter.get_metrics()


# --- 7. POST /sync-inference ---
@app.post("/sync-inference")
def sync_inference(req: SyncInferenceRequest):
    result = adapter.query_task_sync(
        task_name=req.task_name,
        args=req.args,
        kwargs=req.kwargs,
        timeout=req.timeout,
    )
    return {"status": result.status, "result": result.result}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
