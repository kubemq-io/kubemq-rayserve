# KubeMQ Ray Serve Integration -- Examples

Comprehensive, runnable examples for the kubemq-rayserve integration. Every example is self-contained, uses unique channel names, and cleans up after itself.

---

## Prerequisites

1. **KubeMQ broker** running on `localhost:50000`:
   ```bash
   docker run -d --name kubemq -p 50000:50000 -p 8080:8080 -p 9090:9090 kubemq/kubemq-community:latest
   ```

2. **Install the integration package**:
   ```bash
   pip install kubemq-rayserve
   ```

---

## Quick Start

Start with the quickstart examples:

```bash
# Async inference (enqueue + poll)
python examples/quickstart/hello_world.py

# Sync inference (request-response via Queries)
python examples/quickstart/sync_hello_world.py

# Progress tracking
python examples/quickstart/progress_hello_world.py
```

---

## Category Table

| Category | Description | Files |
|----------|-------------|-------|
| `quickstart/` | Minimal, copy-paste-ready examples | 3 |
| `connection/` | Connection patterns: default, custom, TLS, mTLS, auth, env vars | 8 |
| `async_inference/` | Async queue-based inference: enqueue, batch, args, handlers | 11 |
| `sync_inference/` | Synchronous request-response via KubeMQ Queries | 4 |
| `progress_tracking/` | Real-time task progress reporting and monitoring | 4 |
| `error_handling/` | Error handling, retries, DLQ, graceful shutdown | 10 |
| `cancel_task/` | Task cancellation patterns | 2 |
| `autoscaling/` | Queue depth autoscaling policy for Ray Serve | 4 |
| `metrics/` | Metrics collection, dashboards, queue monitoring | 4 |
| `config/` | Configuration options and tuning | 5 |
| `raw_sdk/` | Direct KubeMQ Python SDK usage (power users) | 8 |
| `ml_patterns/` | ML inference patterns: batch, A/B testing, GPU, pipelines | 8 |
| `web_frameworks/` | FastAPI and Flask full applications | 2 |
| `patterns/` | Distributed patterns: fan-out, scatter-gather, pipeline | 5 |
| `kubernetes/` | Kubernetes deployment manifests (YAML) | 7 |

**Total: 76 files** (68 Python + 7 YAML + 1 README)

---

## Full Example Index

### quickstart/

| File | Description |
|------|-------------|
| `hello_world.py` | Minimal async inference: enqueue, poll, get result |
| `sync_hello_world.py` | Minimal sync inference via KubeMQ Queries |
| `progress_hello_world.py` | Minimal progress tracking with event subscription |

### connection/

| File | Description |
|------|-------------|
| `basic_connection.py` | Default localhost:50000 connection with health check |
| `custom_address.py` | Connect to a custom broker address |
| `custom_client_id.py` | Named client identification for monitoring |
| `health_check.py` | health_check() and health_check_sync() usage |
| `auth_token.py` | JWT authentication token configuration |
| `tls_setup.py` | TLS encrypted connection with CA certificate |
| `mtls_setup.py` | Mutual TLS with client certificates |
| `env_var_config.py` | All configuration from environment variables |

### async_inference/

| File | Description |
|------|-------------|
| `basic_enqueue.py` | Single task: enqueue, poll, print result |
| `batch_enqueue.py` | Enqueue multiple tasks, collect all results |
| `task_with_args.py` | Positional arguments |
| `task_with_kwargs.py` | Keyword arguments |
| `task_with_mixed_args.py` | Both positional and keyword arguments |
| `multiple_handlers.py` | Register and dispatch to multiple handlers |
| `handler_return_types.py` | Different return types (dict, list, str, int, None) |
| `long_running_task.py` | Long-running task with extended polling |
| `result_expiry.py` | Result TTL expiration behavior |
| `consumer_concurrency.py` | Parallel message processing |
| `result_polling_patterns.py` | Polling strategies: tight loop, backoff, timeout |

### sync_inference/

| File | Description |
|------|-------------|
| `basic_sync_query.py` | Synchronous request-response via Queries |
| `sync_with_timeout.py` | Custom timeout for sync inference |
| `sync_vs_async_comparison.py` | Side-by-side latency comparison |
| `sync_multiple_handlers.py` | Sync dispatch to different handlers |

### progress_tracking/

| File | Description |
|------|-------------|
| `basic_progress.py` | report_progress() during task execution |
| `progress_subscriber.py` | External subscriber monitoring progress events |
| `progress_with_detail.py` | Progress with rich detail messages |
| `multi_stage_progress.py` | Progress across pipeline stages |

### error_handling/

| File | Description |
|------|-------------|
| `connection_error.py` | Handle broker unreachable |
| `reconnection_behavior.py` | SDK auto-reconnect behavior during consume |
| `graceful_shutdown.py` | stop_consumer() with clean shutdown |
| `serialization_error.py` | Non-JSON-serializable args (TypeError) |
| `handler_exception.py` | Handler raises, task retried |
| `retries_exhausted_dlq.py` | Max retries exhausted, DLQ routing |
| `dlq_monitoring_callback.py` | on_dlq callback for alerting |
| `timeout_handling.py` | Sync inference timeout |
| `result_storage_failure.py` | Result backend fallback behavior |
| `malformed_message.py` | Invalid JSON, unprocessable queue routing |

### cancel_task/

| File | Description |
|------|-------------|
| `soft_cancel.py` | cancel_task() overwrites result with CANCELLED |
| `cancel_and_check_status.py` | Cancel then verify status transition |

### autoscaling/

| File | Description |
|------|-------------|
| `queue_depth_policy.py` | kubemq_queue_depth_policy() basic usage |
| `custom_tasks_per_replica.py` | Tune the scaling factor |
| `autoscaling_with_auth.py` | Policy with authenticated broker |
| `monitor_scaling_decisions.py` | Log scaling decisions and queue depth over time |

### metrics/

| File | Description |
|------|-------------|
| `basic_metrics.py` | get_metrics() after processing tasks |
| `metrics_dashboard.py` | Periodic metrics collection and formatted display |
| `queue_depth_monitoring.py` | Monitor queue depth over time |
| `processing_duration_stats.py` | Track handler execution time statistics |

### config/

| File | Description |
|------|-------------|
| `all_config_options.py` | Every KubeMQAdapterConfig field demonstrated |
| `result_channel_prefix.py` | Custom result channel naming |
| `consumer_poll_timeout.py` | Tune poll frequency |
| `max_message_size.py` | Configure send/receive size limits |
| `sync_inference_timeout.py` | Default sync query timeout |

### raw_sdk/

| File | Description |
|------|-------------|
| `direct_queue_send.py` | QueuesClient.send_queue_message() directly |
| `direct_queue_receive.py` | QueuesClient.receive_queue_messages() directly |
| `direct_queue_peek.py` | QueuesClient.peek_queue_messages() directly |
| `direct_event_pubsub.py` | PubSubClient send/subscribe events |
| `direct_query_request.py` | CQClient send_query / subscribe_to_queries |
| `direct_channel_list.py` | QueuesClient.list_queues_channels() |
| `adapter_plus_raw_sdk.py` | Mix adapter API with direct SDK calls |
| `custom_result_query.py` | Query result channels directly for debugging |

### ml_patterns/

| File | Description |
|------|-------------|
| `batch_inference.py` | Batch processing with parallel consumers |
| `ab_model_testing.py` | A/B model testing with separate queues |
| `model_versioning.py` | Hot-swap model versions without downtime |
| `gpu_multi_model.py` | Multiple GPU models on different queues |
| `image_classification_pipeline.py` | Preprocess, classify, postprocess pipeline |
| `embedding_pipeline.py` | Text to embedding vector generation |
| `llm_inference.py` | LLM prompt to completion inference |
| `text_to_speech.py` | TTS inference with binary result handling |

### web_frameworks/

| File | Description |
|------|-------------|
| `fastapi_app.py` | Full FastAPI app: health, enqueue, status, cancel, progress SSE, metrics |
| `flask_app.py` | Full Flask app: health, enqueue, status, cancel, metrics |

### patterns/

| File | Description |
|------|-------------|
| `fan_out.py` | One task triggers multiple downstream tasks |
| `scatter_gather.py` | Scatter work, gather and aggregate results |
| `pipeline_chain.py` | Multi-stage processing pipeline |
| `priority_queues.py` | Separate queues for different priorities |
| `work_distribution.py` | Distribute work across multiple consumers |

### kubernetes/

| File | Description |
|------|-------------|
| `kubemq-broker.yaml` | KubeMQ StatefulSet deployment |
| `ray-cluster.yaml` | RayCluster with KubeMQ configuration |
| `ray-service.yaml` | RayService with adapter deployment |
| `configmap-adapter.yaml` | ConfigMap for adapter configuration |
| `secret-auth.yaml` | Secret for auth token and TLS certificates |
| `keda-scaled-object.yaml` | KEDA ScaledObject for queue-based scaling |
| `helm-values.yaml` | Helm values for production deployment |

---

## Running Examples

All examples default to a KubeMQ broker at `localhost:50000`. Override with the `KUBEMQ_ADDRESS` environment variable:

```bash
# Default (localhost:50000)
python examples/quickstart/hello_world.py

# Custom broker address
KUBEMQ_ADDRESS=my-broker:50000 python examples/quickstart/hello_world.py
```

Each example uses a unique UUID-based channel name to avoid conflicts between concurrent runs.

---

## Web Framework Examples

### FastAPI

```bash
pip install fastapi uvicorn
python examples/web_frameworks/fastapi_app.py
# Server starts at http://localhost:8000
```

### Flask

```bash
pip install flask
python examples/web_frameworks/flask_app.py
# Server starts at http://localhost:5000
```
