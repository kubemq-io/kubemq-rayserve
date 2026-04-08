# kubemq-rayserve

KubeMQ adapter for [Ray Serve](https://docs.ray.io/en/latest/serve/index.html) async inference. Provides `KubeMQTaskProcessorAdapter` -- the first non-Celery adapter for Ray Serve's `TaskProcessorAdapter` framework, enabling queue-based async and sync ML inference backed by [KubeMQ](https://kubemq.io/).

## Features

- **Async Inference** -- Enqueue tasks via KubeMQ Queues, poll for results via queue-peek backend
- **Sync Inference** -- Blocking request-response via KubeMQ Queries (`query_task_sync`)
- **Progress Tracking** -- Real-time task progress events via KubeMQ Events
- **DLQ Monitoring** -- Callback-based dead letter queue alerts (`on_dlq`)
- **Autoscaling** -- Custom autoscaling policy driven by KubeMQ queue depth (`kubemq_queue_depth_policy`)
- **Result Backend** -- Queue-peek result storage with purge-then-write pattern, automatic retry, and in-memory fallback
- **Health Checks** -- Broker connectivity check via SDK `ping()`
- **Metrics** -- 8-metric dict covering queue depth, in-flight count, DLQ depth, enqueue/complete counters, processing duration, retry count, and poll latency

## Quick Start

### Installation

```bash
uv pip install kubemq-rayserve
```

Or for development:

```bash
git clone https://github.com/kubemq/kubemq-rayserve.git
cd kubemq-rayserve
uv sync --group dev
```

### Prerequisites

- Python >= 3.10
- Running KubeMQ broker (default: `localhost:50000`)
- Ray Serve >= 2.50.0

Start a local KubeMQ broker:

```bash
docker run -d --name kubemq -p 50000:50000 -p 9090:9090 kubemq/kubemq-community:latest
```

### Minimal Example

```python
import time
from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

# 1. Define a task handler
def classify(text: str) -> dict:
    return {"label": "POSITIVE", "score": 0.95}

# 2. Configure and initialize the adapter
config = KubeMQAdapterConfig(address="localhost:50000")
adapter = KubeMQTaskProcessorAdapter(config)

# In production, Ray Serve calls initialize() with TaskProcessorConfig.
# For standalone usage:
class TaskConfig:
    queue_name = "inference-tasks"
    max_retries = 3
    failed_task_queue_name = "inference-tasks-dlq"
    unprocessable_task_queue_name = ""

adapter.initialize(consumer_concurrency=2, task_processor_config=TaskConfig())
adapter.register_task_handler(classify, name="classify")
adapter.start_consumer()

# 3. Enqueue a task
result = adapter.enqueue_task_sync(task_name="classify", args=["great product"])
print(f"Task ID: {result.id}, Status: {result.status}")

# 4. Poll for result
for _ in range(30):
    status = adapter.get_task_status_sync(result.id)
    if status.status == "SUCCESS":
        print(f"Result: {status.result}")
        break
    time.sleep(0.5)

# 5. Cleanup
adapter.stop_consumer()
```

### Ray Serve Deployment (Production)

```python
from ray import serve
from kubemq_rayserve import KubeMQAdapterConfig, KubeMQTaskProcessorAdapter

@serve.deployment(ray_actor_options={"num_gpus": 1})
@task_consumer(
    task_processor_config=TaskProcessorConfig(
        adapter_class=KubeMQTaskProcessorAdapter,
        adapter_config=KubeMQAdapterConfig(
            address="kubemq:50000",
            auth_token="your-jwt-token",
        ),
        queue_name="inference-tasks",
        max_retries=3,
        failed_task_queue_name="inference-tasks-dlq",
    )
)
class ModelDeployment:
    def __init__(self):
        self.model = load_model()

    def __call__(self, data: str) -> dict:
        return self.model.predict(data)
```

## Configuration Reference

`KubeMQAdapterConfig` is a Pydantic `BaseModel` with the following fields:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `address` | `str` | `"localhost:50000"` | KubeMQ broker address (host:port) |
| `client_id` | `str` | `""` | Client identifier. Auto-generated UUID suffix if empty |
| `auth_token` | `str` | `""` | JWT authentication token |
| `tls` | `bool` | `False` | Enable TLS connection |
| `tls_cert_file` | `str` | `""` | mTLS client certificate path |
| `tls_key_file` | `str` | `""` | mTLS client key path |
| `tls_ca_file` | `str` | `""` | CA certificate path |
| `result_channel_prefix` | `str` | `"rayserve-result-"` | Prefix for result storage queue channels |
| `result_expiry_seconds` | `int` | `3600` | Result TTL (1 hour default) |
| `visibility_timeout` | `int` | `120` | Reserved for future SDK support |
| `consumer_poll_timeout_seconds` | `int` | `1` | Queue poll wait timeout (seconds) |
| `max_send_size` | `int` | `4194304` | Max message send size in bytes (4MB) |
| `max_receive_size` | `int` | `4194304` | Max message receive size in bytes (4MB) |
| `sync_inference_timeout` | `int` | `30` | Default timeout for sync Query inference (seconds) |
| `on_dlq` | `Callable` | `None` | Callback `(task_id: str, error: str) -> None` when task moves to DLQ |

## API Reference

### KubeMQTaskProcessorAdapter

Implements Ray Serve's `TaskProcessorAdapter` ABC (7 abstract + 4 optional methods) plus extension methods.

#### Core Methods (TaskProcessorAdapter)

| Method | Description |
|--------|-------------|
| `initialize(consumer_concurrency, **kwargs)` | Create SDK clients, store concurrency config |
| `register_task_handler(func, name)` | Register a callable as a named task handler |
| `enqueue_task_sync(task_name, args, kwargs)` | Serialize task, send to KubeMQ queue, return `TaskResult(status="PENDING")` |
| `get_task_status_sync(task_id)` | Peek result channel, return `TaskResult` with current status |
| `start_consumer(**kwargs)` | Start queue polling thread + Query subscription thread |
| `stop_consumer(timeout)` | Cancel token, stop threads, close SDK clients |
| `health_check()` | Returns `True` if broker is reachable via `ping()` |
| `cancel_task_sync(task_id)` | Soft cancel -- overwrites result with CANCELLED status |
| `get_metrics_sync()` | Returns 8-metric dict (see Metrics section) |

#### Extension Methods

| Method | Description |
|--------|-------------|
| `query_task_sync(task_name, args, kwargs, timeout)` | Synchronous inference via KubeMQ Query. Blocks until response or timeout |
| `report_progress(task_id, pct, detail)` | Publish progress event via KubeMQ Events on `{queue_name}.progress` |

### kubemq_queue_depth_policy

```python
kubemq_queue_depth_policy(
    ctxs: dict[str, Any],
    kubemq_address: str = "localhost:50000",
    queue_name: str = "",
    tasks_per_replica: int = 5,
    auth_token: str = "",
) -> tuple[dict[str, int], dict]
```

Custom autoscaling policy for Ray Serve. Queries KubeMQ queue depth and returns desired replica counts: `ceil(queue_depth / tasks_per_replica)`.

## Metrics

`get_metrics_sync()` returns a dict with 8 metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `queue_depth` | Gauge | Current waiting messages in the task queue |
| `in_flight` | Gauge | Currently processing tasks |
| `dlq_depth` | Gauge | Messages in the dead letter queue |
| `tasks_enqueued_total` | Counter | Total tasks enqueued since startup |
| `tasks_completed_total` | Counter | Total tasks completed by status (`SUCCESS`/`FAILURE`) |
| `task_processing_duration_seconds` | Histogram | Processing time stats (min, max, avg, count) |
| `result_storage_retries_total` | Counter | Total result storage retry attempts |
| `consumer_poll_latency_seconds` | Gauge | Last consumer poll duration |

## Competitive Comparison

| Feature | kubemq-rayserve | CeleryTaskProcessorAdapter |
|---------|----------------|---------------------------|
| Message Broker | KubeMQ (Kubernetes-native) | Redis / RabbitMQ |
| Sync Inference | Built-in (`query_task_sync`) | Not available |
| Progress Tracking | Built-in (`report_progress`) | Requires custom signals |
| Autoscaling Policy | `kubemq_queue_depth_policy` | Manual HPA configuration |
| DLQ Monitoring | `on_dlq` callback | Requires Celery signals + custom code |
| Result Backend | Queue-peek (no extra infra) | Requires separate Redis/DB |
| KEDA Integration | Native (kubemq-keda-connector) | Requires custom KEDA scaler |
| Kubernetes-native | Yes (single binary, no Erlang) | No (Redis/RabbitMQ dependencies) |
| Health Check | SDK `ping()` | Celery `inspect().ping()` |
| Setup Complexity | 1 dependency (KubeMQ) | 2+ dependencies (broker + backend) |

## Examples

| Example | Description |
|---------|-------------|
| [`basic_async_inference.py`](examples/basic_async_inference.py) | Minimal: configure, enqueue, poll result |
| [`gpu_image_classifier.py`](examples/gpu_image_classifier.py) | GPU worker with queue-based image classification |
| [`multi_model_pipeline.py`](examples/multi_model_pipeline.py) | Two chained deployments: preprocessor + classifier |
| [`sync_inference.py`](examples/sync_inference.py) | Synchronous inference via `query_task_sync()` |
| [`task_progress_tracking.py`](examples/task_progress_tracking.py) | Real-time progress events during long tasks |
| [`dlq_monitoring.py`](examples/dlq_monitoring.py) | DLQ callback alerting on permanent failures |
| [`autoscaling_queue_depth.py`](examples/autoscaling_queue_depth.py) | Custom autoscaling policy demo |
| [`keda_autoscaling/`](examples/keda_autoscaling/) | KEDA ScaledObject + RayService YAML references |

Run any example (requires a live KubeMQ broker):

```bash
cd kubemq-rayserve
uv run python examples/basic_async_inference.py
```

## Architecture

```
Producer (HTTP/gRPC)
    |
    v
KubeMQTaskProcessorAdapter.enqueue_task_sync()
    |
    v
KubeMQ Queue ("inference-tasks")
    |
    v
Consumer Thread (_consumer_loop)
    |-- receive_queue_messages() (poll)
    |-- deserialize task payload
    |-- dispatch to registered handler
    |-- store result (purge-then-write)
    |-- ack / nack message
    |
    v
Result Backend (queue-peek)
    |
    v
get_task_status_sync() -> TaskResult
```

For sync inference, a parallel path uses KubeMQ Queries:

```
query_task_sync()
    |
    v
KubeMQ Query (CQClient)
    |
    v
Query Subscription Thread (_handle_sync_query)
    |-- dispatch to handler
    |-- send response
    |
    v
TaskResult (returned synchronously)
```

## Development

```bash
# Install dev dependencies
uv sync --group dev

# Run unit tests
uv run pytest tests/unit/ -v

# Run with coverage
uv run pytest --cov=kubemq_rayserve --cov-report=term

# Lint and format
uv run ruff check .
uv run ruff format .

# Integration tests (requires live KubeMQ broker)
uv run pytest tests/integration/ -v -m integration

# E2E tests (requires live KubeMQ broker + Ray Serve)
uv run pytest tests/e2e/ -v -m e2e
```

## License

MIT -- see [LICENSE](LICENSE).
