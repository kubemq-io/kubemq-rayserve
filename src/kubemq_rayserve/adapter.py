"""KubeMQ TaskProcessorAdapter for Ray Serve async inference."""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Callable
from functools import cached_property
from typing import Any
from uuid import uuid4

from kubemq import (
    CancellationToken,
    ClientConfig,
    CQClient,
    EventMessage,
    PubSubClient,
    QueryMessage,
    QueryReceived,
    QueryResponse,
    QueueMessage,
    QueuesClient,
)
from kubemq.core.config import TLSConfig
from kubemq.core.exceptions import (
    KubeMQConnectionError,
    KubeMQConnectionNotReadyError,
)
from ray.serve.schema import TaskResult
from ray.serve.task_consumer import TaskProcessorAdapter

from kubemq_rayserve._internal.serialization import (
    deserialize_task_payload,
    generate_task_id,
    serialize_sync_query,
    serialize_sync_response,
    serialize_task_payload,
)
from kubemq_rayserve.config import KubeMQAdapterConfig
from kubemq_rayserve.metrics import MetricsCollector
from kubemq_rayserve.result_backend import ResultBackend

logger = logging.getLogger("ray.serve.kubemq")


class KubeMQTaskProcessorAdapter(TaskProcessorAdapter):
    """KubeMQ-backed TaskProcessorAdapter for Ray Serve.

    Implements all 7 abstract methods + 4 optional methods of the
    TaskProcessorAdapter ABC, plus extension methods for sync inference
    and progress tracking.

    Lifecycle:
        __init__(config) -> stores config only
        initialize(consumer_concurrency) -> creates SDK clients
        register_task_handler(func, name) -> stores in _task_handlers
        start_consumer() -> starts queue polling + query subscription
        stop_consumer() -> cancels token, stops threads, closes clients
    """

    _cancel_token: CancellationToken | None = None

    def __init__(self, config: KubeMQAdapterConfig) -> None:
        """Store configuration without creating connections.

        Args:
            config: KubeMQ adapter configuration.
        """
        self._config = config
        self._task_handlers: dict[str, Callable] = {}
        self._running = False
        self._consumer_thread: threading.Thread | None = None
        self._consumer_concurrency = 1
        self._queue_name = ""
        self._max_retries = 0
        self._failed_task_queue_name = ""
        self._unprocessable_task_queue_name = ""
        self._client: QueuesClient | None = None
        self._result_backend: ResultBackend | None = None
        self._metrics: MetricsCollector | None = None
        self._in_flight: set[str] = set()
        self._in_flight_lock = threading.Lock()

    def _build_client_config(self) -> ClientConfig:
        """Build a KubeMQ SDK ClientConfig from adapter config.

        Pattern source: kubemq-celery/src/kubemq_celery/backend.py:92-114
        Validation finding: ClientConfig.tls is TLSConfig object, not bool.
        """
        tls_config = TLSConfig(
            enabled=self._config.tls,
            cert_file=self._config.tls_cert_file or None,
            key_file=self._config.tls_key_file or None,
            ca_file=self._config.tls_ca_file or None,
        )
        client_id = self._config.client_id or f"rayserve-{uuid4().hex[:8]}"
        return ClientConfig(
            address=self._config.address,
            client_id=client_id,
            auth_token=self._config.auth_token or None,
            tls=tls_config,
            max_send_size=self._config.max_send_size,
            max_receive_size=self._config.max_receive_size,
        )

    def initialize(self, consumer_concurrency: int = 1, **kwargs: Any) -> None:
        """Create KubeMQ SDK client and store concurrency config.

        Called by Ray Serve after instantiation. Creates a single QueuesClient
        used for both task distribution and result storage.

        Args:
            consumer_concurrency: Max concurrent tasks (maps to poll batch size).
            **kwargs: Additional keyword arguments (includes task_processor_config).
        """
        self._consumer_concurrency = consumer_concurrency

        # Extract TaskProcessorConfig fields from kwargs
        task_processor_config = kwargs.get("task_processor_config")
        if task_processor_config is not None:
            self._queue_name = getattr(task_processor_config, "queue_name", "")
            self._max_retries = getattr(task_processor_config, "max_retries", 0)
            self._failed_task_queue_name = getattr(
                task_processor_config, "failed_task_queue_name", ""
            )
            self._unprocessable_task_queue_name = getattr(
                task_processor_config, "unprocessable_task_queue_name", ""
            )

        config = self._build_client_config()
        self._client = QueuesClient(config=config)
        self._result_backend = ResultBackend(
            client=self._client,
            result_channel_prefix=self._config.result_channel_prefix,
            result_expiry_seconds=self._config.result_expiry_seconds,
        )
        self._metrics = MetricsCollector()
        logger.info(
            "KubeMQ adapter initialized address=%s client_id=%s",
            self._config.address,
            config.client_id,
        )

    @cached_property
    def _pubsub_client(self) -> PubSubClient:
        """Lazy PubSubClient for progress events."""
        config = self._build_client_config()
        return PubSubClient(config=config)

    @cached_property
    def _cq_client(self) -> CQClient:
        """Lazy CQClient for sync inference."""
        config = self._build_client_config()
        return CQClient(config=config)

    def register_task_handle(self, func: Callable, name: str | None = None) -> None:
        """Store handler function by name in internal dict.

        Args:
            func: The task handler callable.
            name: Handler name. If None, uses func.__name__.
        """
        handler_name = name or func.__name__
        self._task_handlers[handler_name] = func
        logger.info("Registered task handler: %s", handler_name)

    # Alias for backward compatibility with plan/tests
    register_task_handler = register_task_handle

    def enqueue_task(
        self, task_name: str, args: Any = None, kwargs: Any = None, **options: Any
    ) -> TaskResult:
        """Enqueue a task (delegates to enqueue_task_sync).

        Args:
            task_name: Registered handler name.
            args: Positional arguments.
            kwargs: Keyword arguments.
            **options: Additional options.

        Returns:
            TaskResult with PENDING status.
        """
        return self.enqueue_task_sync(task_name, args, kwargs, **options)

    def enqueue_task_sync(
        self, task_name: str, args: Any = None, kwargs: Any = None, **options: Any
    ) -> TaskResult:
        """Serialize task as JSON, send to KubeMQ queue, return TaskResult.

        Args:
            task_name: Registered handler name.
            args: Positional arguments.
            kwargs: Keyword arguments.
            **options: Additional options.

        Returns:
            TaskResult with PENDING status and backend_task_id.

        Raises:
            ValueError: If task_name is empty.
            TypeError: If args/kwargs are not JSON-serializable.
            KubeMQConnectionError: If broker is unreachable.
            KubeMQAuthenticationError: If auth token is invalid.
        """
        if not task_name:
            raise ValueError("task_name must not be empty")

        if self._client is None:
            raise RuntimeError("Call initialize() before enqueue_task")
        if self._result_backend is None:
            raise RuntimeError("Call initialize() before enqueue_task")
        if self._metrics is None:
            raise RuntimeError("Call initialize() before enqueue_task")

        task_id = generate_task_id()
        body = serialize_task_payload(task_id, task_name, args, kwargs)

        msg_kwargs: dict[str, Any] = {
            "channel": self._queue_name,
            "body": body,
        }
        if self._max_retries > 0 and self._failed_task_queue_name:
            msg_kwargs["max_receive_count"] = self._max_retries
            msg_kwargs["max_receive_queue"] = self._failed_task_queue_name

        # Store PENDING result BEFORE sending to queue so that a fast
        # consumer cannot overwrite SUCCESS with a late PENDING store.
        self._result_backend.store_result(task_id, "PENDING")

        msg = QueueMessage(**msg_kwargs)
        self._client.send_queue_message(msg)
        self._metrics.inc_enqueued()

        return TaskResult(id=task_id, status="PENDING", result=None)

    def get_task_status(self, task_id: str) -> TaskResult:
        """Get task status (delegates to get_task_status_sync).

        Args:
            task_id: Task identifier.

        Returns:
            TaskResult with current status.
        """
        return self.get_task_status_sync(task_id)

    def get_task_status_sync(self, task_id: str) -> TaskResult:
        """Peek result channel for task_id, return TaskResult.

        Args:
            task_id: Task identifier.

        Returns:
            TaskResult with current status and result.
        """
        if self._result_backend is None:
            raise RuntimeError("Call initialize() before get_task_status")
        meta = self._result_backend.get_result(task_id)
        return TaskResult(
            id=task_id,
            status=meta.get("status", "PENDING"),
            result=meta.get("result"),
        )

    def start_consumer(self, **kwargs: Any) -> None:
        """Start consumer thread (queue polling) AND Query subscription.

        The consumer thread polls the queue for tasks and dispatches them
        to registered handlers. The Query subscription handles sync
        inference requests on the same channel.

        Args:
            **kwargs: Additional keyword arguments.
        """
        if self._client is None:
            raise RuntimeError("Call initialize() before start_consumer")

        self._running = True
        self._cancel_token = CancellationToken()

        # Start consumer thread for queue polling
        self._consumer_thread = threading.Thread(
            target=self._consumer_loop,
            name="kubemq-rayserve-consumer",
            daemon=True,
        )
        self._consumer_thread.start()

        # Start Query subscription for sync inference
        self._start_query_subscription()

        logger.info(
            "Consumer started queue=%s concurrency=%d",
            self._queue_name,
            self._consumer_concurrency,
        )

    def _start_query_subscription(self) -> None:
        """Start Query subscription for sync inference on the same channel.

        Uses CancellationToken for clean shutdown. SDK built-in subscription
        recovery (PY-119) handles reconnection without adapter logic.
        """
        from kubemq import QueriesSubscription

        subscription = QueriesSubscription(
            channel=self._queue_name,
            on_receive_query_callback=self._handle_sync_query,
            on_error_callback=self._handle_query_error,
        )
        self._cq_client.subscribe_to_queries(subscription, self._cancel_token)

    def _handle_sync_query(self, query: QueryReceived) -> None:
        """Handle incoming sync inference query.

        Deserializes the query, calls the registered handler, and sends
        a response back via CQClient.send_response_message().
        """
        try:
            payload = json.loads(query.body)
            task_name = payload.get("task_name", "")
            args = payload.get("args", [])
            kwargs = payload.get("kwargs", {})

            handler = self._task_handlers.get(task_name)
            if handler is None:
                response_body = serialize_sync_response(
                    "FAILURE", error=f"Unknown task handler: {task_name}"
                )
            else:
                try:
                    result = handler(*args, **kwargs)
                    response_body = serialize_sync_response("SUCCESS", result=result)
                except Exception as exc:
                    response_body = serialize_sync_response(
                        "FAILURE", error=f"{type(exc).__name__}: {exc}"
                    )

            response = QueryResponse(
                query_received=query,
                body=response_body,
                is_executed=True,
            )
            self._cq_client.send_response_message(response)
        except Exception as exc:
            logger.error(
                "Error handling sync query channel=%s request_id=%s: %s",
                getattr(query, "channel", "unknown"),
                getattr(query, "id", "unknown"),
                exc,
            )

    def _handle_query_error(self, error: str) -> None:
        """Handle Query subscription errors."""
        logger.warning("Query subscription error: %s", error)

    def _consumer_loop(self) -> None:
        """Main consumer loop — polls queue and dispatches tasks."""
        if self._client is None:
            raise RuntimeError("Call initialize() before starting consumer loop")
        if self._result_backend is None:
            raise RuntimeError("Call initialize() before starting consumer loop")
        if self._metrics is None:
            raise RuntimeError("Call initialize() before starting consumer loop")

        while self._running:
            try:
                poll_start = time.perf_counter()
                response = self._client.receive_queue_messages(
                    channel=self._queue_name,
                    max_messages=self._consumer_concurrency,
                    wait_timeout_in_seconds=self._config.consumer_poll_timeout_seconds,
                    auto_ack=False,
                )
                poll_duration = time.perf_counter() - poll_start
                self._metrics.set_poll_latency(poll_duration)

                if response.is_error:
                    logger.debug("Consumer poll error: %s", response.error)
                    continue

                for message in response.messages:
                    if not self._running:
                        # Shutting down — nack remaining messages
                        message.nack()
                        continue
                    self._process_message(message)

            except (KubeMQConnectionError, KubeMQConnectionNotReadyError) as exc:
                logger.warning("Consumer connection error (will retry): %s", exc)
                if self._running:
                    time.sleep(1)
            except Exception as exc:
                logger.error("Consumer loop unexpected error: %s", exc)
                if self._running:
                    time.sleep(1)

    def _process_message(self, message: Any) -> None:
        """Process a single queue message.

        Deserializes, dispatches to handler, stores result, acks/nacks.
        """
        if self._result_backend is None:
            raise RuntimeError("Call initialize() before processing messages")
        if self._metrics is None:
            raise RuntimeError("Call initialize() before processing messages")

        task_id = ""
        created_at = None
        try:
            payload = deserialize_task_payload(message.body)
            task_id = payload.get("task_id", "")
            task_name = payload.get("task_name", "")
            args = payload.get("args", [])
            kwargs = payload.get("kwargs", {})
            created_at = payload.get("created_at")
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.warning("Malformed message (acking to unprocessable): %s", exc)
            message.ack()
            # Move to unprocessable queue via re-queue if configured
            if self._unprocessable_task_queue_name:
                try:
                    msg = QueueMessage(
                        channel=self._unprocessable_task_queue_name,
                        body=message.body,
                    )
                    if self._client is None:
                        raise RuntimeError("Client not initialized")
                    self._client.send_queue_message(msg)
                except Exception as send_exc:
                    logger.error("Failed to send to unprocessable queue: %s", send_exc)
            return

        # Track in-flight
        with self._in_flight_lock:
            self._in_flight.add(task_id)

        handler = self._task_handlers.get(task_name)
        if handler is None:
            logger.warning("No handler registered for task: %s", task_name)
            message.ack()
            with self._in_flight_lock:
                self._in_flight.discard(task_id)
            return

        handler_start = time.perf_counter()
        try:
            result = handler(*args, **kwargs)
            handler_duration = time.perf_counter() - handler_start
            self._metrics.record_processing_duration(handler_duration)

            # Store SUCCESS result
            self._result_backend.store_result(
                task_id, "SUCCESS", result=result, created_at=created_at
            )
            self._metrics.inc_completed("SUCCESS")
            message.ack()
        except Exception as exc:
            handler_duration = time.perf_counter() - handler_start
            self._metrics.record_processing_duration(handler_duration)

            receive_count = getattr(message, "receive_count", 0)
            if self._max_retries > 0 and receive_count < self._max_retries:
                # Retries remaining — nack for redelivery
                logger.warning(
                    "Handler error (retries remaining=%d): task_id=%s error=%s",
                    self._max_retries - receive_count,
                    task_id,
                    exc,
                )
                message.nack()
            else:
                # Retries exhausted — store FAILURE, nack to DLQ
                error_str = f"{type(exc).__name__}: {exc}"
                self._result_backend.store_result(
                    task_id, "FAILURE", error=error_str, created_at=created_at
                )
                self._metrics.inc_completed("FAILURE")
                message.nack()

                # Fire DLQ callback
                if self._config.on_dlq is not None:
                    try:
                        self._config.on_dlq(task_id, error_str)
                    except Exception as cb_exc:
                        logger.error("on_dlq callback error: %s", cb_exc)
        finally:
            with self._in_flight_lock:
                self._in_flight.discard(task_id)

    def stop_consumer(self, timeout: float = 10.0) -> None:
        """Stop consumer thread, nack in-flight, close clients.

        Order: cancel token -> wait for subscription -> set _running=False
        -> nack in-flight -> close client.

        Args:
            timeout: Maximum time in seconds to wait for consumer to stop.
        """
        # Step 1: Cancel query subscription token
        if self._cancel_token is not None:
            self._cancel_token.cancel()

        # Step 2: Set running to False to stop consumer loop
        self._running = False

        # Step 3: Wait for consumer thread to exit
        if self._consumer_thread is not None:
            self._consumer_thread.join(timeout=5.0)

        # Step 4: Nack any in-flight messages (best effort)
        # In-flight messages are nacked within _process_message when _running is False

        # Step 5: Close clients
        if self._client is not None:
            try:
                self._client.close()
            except Exception as exc:
                logger.warning("Error closing QueuesClient: %s", exc)

        # Close lazy-created clients if they exist
        if "_pubsub_client" in self.__dict__:
            try:
                self._pubsub_client.close()
            except Exception as exc:
                logger.warning("Error closing PubSubClient: %s", exc)

        if "_cq_client" in self.__dict__:
            try:
                self._cq_client.close()
            except Exception as exc:
                logger.warning("Error closing CQClient: %s", exc)

        logger.info("Consumer stopped")

    def health_check_sync(self) -> list[dict]:
        """Check broker connectivity via ping.

        Returns:
            List with single health status dict.
        """
        try:
            if self._client is not None:
                self._client.ping()
                return [{"healthy": True}]
            return [{"healthy": False, "error": "client not initialized"}]
        except Exception as exc:
            return [{"healthy": False, "error": str(exc)}]

    def health_check(self) -> bool:
        """Convenience: returns True if broker is reachable."""
        result = self.health_check_sync()
        return bool(result and result[0].get("healthy", False))

    def cancel_task_sync(self, task_id: str) -> bool:
        """Soft cancel — overwrite result with CANCELLED status.

        The in-progress task continues processing but result is overwritten.

        Args:
            task_id: Task to cancel.

        Returns:
            True (always succeeds as soft cancel).
        """
        if self._result_backend is None:
            raise RuntimeError("Call initialize() before cancel_task")
        self._result_backend.store_result(task_id, "CANCELLED")
        return True

    def get_metrics_sync(self) -> dict[str, Any]:
        """Return 8 metrics dict.

        Gauge metrics query KubeMQ on each call. Counter/histogram
        metrics are tracked in-memory.

        Returns:
            Dict with queue_depth, in_flight, dlq_depth, tasks_enqueued_total,
            tasks_completed_total, task_processing_duration_seconds,
            result_storage_retries_total, consumer_poll_latency_seconds.
        """
        if self._metrics is None:
            raise RuntimeError("Call initialize() before get_metrics")
        if self._result_backend is None:
            raise RuntimeError("Call initialize() before get_metrics")

        # Gauge metrics — query KubeMQ
        queue_depth = 0
        dlq_depth = 0
        try:
            if self._client is not None:
                channels = self._client.list_queues_channels(channel_search=self._queue_name)
                for ch in channels:
                    if ch.name == self._queue_name:
                        queue_depth = ch.incoming.waiting
                    elif ch.name == self._failed_task_queue_name:
                        dlq_depth = ch.incoming.waiting
        except Exception as exc:
            logger.debug("Error querying queue depth: %s", exc)

        with self._in_flight_lock:
            in_flight_count = len(self._in_flight)

        return {
            "queue_depth": queue_depth,
            "in_flight": in_flight_count,
            "dlq_depth": dlq_depth,
            "tasks_enqueued_total": self._metrics.enqueued_total,
            "tasks_completed_total": self._metrics.completed_total,
            "task_processing_duration_seconds": self._metrics.processing_duration_stats,
            "result_storage_retries_total": self._result_backend.retries_total,
            "consumer_poll_latency_seconds": self._metrics.poll_latency,
        }

    # Aliases for backward compatibility with plan/tests
    get_metrics = get_metrics_sync
    cancel_task = cancel_task_sync

    # --- Extension methods (NOT in TaskProcessorAdapter ABC) ---

    def query_task_sync(
        self,
        task_name: str,
        args: Any = None,
        kwargs: Any = None,
        timeout: int = 30,
        **options: Any,
    ) -> TaskResult:
        """Send KubeMQ Query for synchronous inference.

        Blocks until handler responds or timeout. Uses CQClient.send_query().

        Args:
            task_name: Registered handler name.
            args: Positional arguments.
            kwargs: Keyword arguments.
            timeout: Query timeout in seconds.
            **options: Additional options.

        Returns:
            TaskResult with result from handler.

        Raises:
            KubeMQTimeoutError: If query times out.
            TypeError: If args/kwargs are not JSON-serializable.
        """
        body = serialize_sync_query(task_name, args, kwargs)
        query = QueryMessage(
            channel=self._queue_name,
            body=body,
            timeout_in_seconds=timeout or self._config.sync_inference_timeout,
        )
        response = self._cq_client.send_query(query)

        try:
            result_data = json.loads(response.body)
        except (json.JSONDecodeError, TypeError):
            return TaskResult(
                id=generate_task_id(),
                status="FAILURE",
                result=None,
            )
        return TaskResult(
            id=generate_task_id(),
            status=result_data.get("status", "FAILURE"),
            result=result_data.get("result"),
        )

    def report_progress(self, task_id: str, pct: float, detail: str = "") -> None:
        """Publish a progress update via KubeMQ Events.

        Callable from within task handlers to report progress.

        Args:
            task_id: Task identifier.
            pct: Progress percentage (0-100).
            detail: Optional progress detail string.
        """
        body = json.dumps({"task_id": task_id, "pct": pct, "detail": detail}).encode("utf-8")
        event = EventMessage(
            channel=f"{self._queue_name}.progress",
            body=body,
            tags={"task_id": task_id, "pct": str(int(pct))},
        )
        try:
            self._pubsub_client.send_event(event)
        except Exception as exc:
            logger.warning("Failed to send progress event: %s", exc)
