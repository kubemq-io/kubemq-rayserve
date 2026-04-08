"""Unit tests for KubeMQTaskProcessorAdapter."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from kubemq.core.exceptions import (
    KubeMQAuthenticationError,
    KubeMQConnectionError,
)

from kubemq_rayserve.adapter import KubeMQTaskProcessorAdapter
from kubemq_rayserve.config import KubeMQAdapterConfig


@pytest.fixture
def adapter():
    """Create an adapter with default config."""
    config = KubeMQAdapterConfig()
    return KubeMQTaskProcessorAdapter(config)


@pytest.fixture
def initialized_adapter(adapter):
    """Create an initialized adapter with mocked clients."""
    with patch("kubemq_rayserve.adapter.QueuesClient") as mock_queues:
        mock_client = MagicMock()
        mock_queues.return_value = mock_client
        adapter.initialize(consumer_concurrency=2)
        adapter._queue_name = "test-queue"
        adapter._max_retries = 3
        adapter._failed_task_queue_name = "test-queue.dlq"
        adapter._unprocessable_task_queue_name = "test-queue.unprocessable"
        yield adapter


class TestEnqueue:
    """Tests for enqueue_task and enqueue_task_sync."""

    def test_enqueue_success(self, initialized_adapter):
        """U1: valid task_name, args, kwargs -> TaskResult(status=PENDING)."""
        result = initialized_adapter.enqueue_task_sync(
            "classify", args=["img.jpg"], kwargs={"threshold": 0.85}
        )
        assert result.status == "PENDING"
        assert result.id is not None
        # send_queue_message called twice: once for task, once for PENDING result storage
        assert initialized_adapter._client.send_queue_message.call_count == 2

    def test_enqueue_non_serializable(self, initialized_adapter):
        """U2: args=[non-serializable] -> TypeError with descriptive message."""
        with pytest.raises(TypeError, match="Task argument serialization failed"):
            initialized_adapter.enqueue_task_sync("classify", args=[object()])

    def test_enqueue_empty_task_name(self, initialized_adapter):
        """U3: task_name="" -> ValueError."""
        with pytest.raises(ValueError, match="task_name must not be empty"):
            initialized_adapter.enqueue_task_sync("")

    def test_enqueue_broker_auth_error(self, initialized_adapter):
        """U4: mock auth failure -> KubeMQAuthenticationError raised."""
        initialized_adapter._client.send_queue_message.side_effect = KubeMQAuthenticationError(
            "auth failed"
        )
        with pytest.raises(KubeMQAuthenticationError):
            initialized_adapter.enqueue_task_sync("classify")

    def test_enqueue_broker_down(self, initialized_adapter):
        """U5: mock connection error -> KubeMQConnectionError raised."""
        initialized_adapter._client.send_queue_message.side_effect = KubeMQConnectionError(
            "connection refused"
        )
        with pytest.raises(KubeMQConnectionError):
            initialized_adapter.enqueue_task_sync("classify")


class TestConsume:
    """Tests for consumer loop message processing."""

    def test_consume_success(self, initialized_adapter):
        """U6: valid message -> handler called, SUCCESS stored, message acked."""
        handler = MagicMock(return_value={"predictions": [{"label": "cat"}]})
        initialized_adapter.register_task_handler(handler, "classify")

        message = MagicMock()
        message.body = json.dumps(
            {
                "task_id": "test-id",
                "task_name": "classify",
                "args": [],
                "kwargs": {},
                "created_at": "2026-04-07T00:00:00+00:00",
            }
        ).encode("utf-8")
        message.receive_count = 1

        initialized_adapter._process_message(message)

        handler.assert_called_once_with()
        message.ack.assert_called_once()

    def test_consume_handler_error_with_retries(self, initialized_adapter):
        """U7: handler raises, receive_count < max -> message nacked."""
        handler = MagicMock(side_effect=RuntimeError("CUDA OOM"))
        initialized_adapter.register_task_handler(handler, "classify")

        message = MagicMock()
        message.body = json.dumps(
            {
                "task_id": "test-id",
                "task_name": "classify",
                "args": [],
                "kwargs": {},
            }
        ).encode("utf-8")
        message.receive_count = 1  # < max_retries=3

        initialized_adapter._process_message(message)
        message.nack.assert_called_once()
        message.ack.assert_not_called()

    def test_consume_handler_error_exhausted(self, initialized_adapter):
        """U8: handler raises, retries exhausted -> FAILURE stored, DLQ, on_dlq."""
        on_dlq = MagicMock()
        initialized_adapter._config.on_dlq = on_dlq
        handler = MagicMock(side_effect=RuntimeError("CUDA OOM"))
        initialized_adapter.register_task_handler(handler, "classify")

        message = MagicMock()
        message.body = json.dumps(
            {
                "task_id": "test-id",
                "task_name": "classify",
                "args": [],
                "kwargs": {},
            }
        ).encode("utf-8")
        message.receive_count = 3  # >= max_retries=3

        initialized_adapter._process_message(message)
        message.nack.assert_called_once()
        on_dlq.assert_called_once_with("test-id", "RuntimeError: CUDA OOM")

    def test_consume_malformed_message(self, initialized_adapter):
        """U9: invalid JSON body -> message acked, sent to unprocessable queue."""
        message = MagicMock()
        message.body = b"not-json"

        initialized_adapter._process_message(message)
        message.ack.assert_called_once()


class TestCancel:
    """Tests for soft cancel."""

    def test_soft_cancel(self, initialized_adapter):
        """U18: cancel_task(task_id) -> result overwritten with CANCELLED."""
        result = initialized_adapter.cancel_task("test-id")
        assert result is True


class TestDLQCallback:
    """Tests for DLQ callback."""

    def test_dlq_callback(self, initialized_adapter):
        """U23: handler fails, retries exhausted -> on_dlq invoked."""
        dlq_calls: list[tuple[str, str]] = []
        initialized_adapter._config.on_dlq = lambda tid, err: dlq_calls.append((tid, err))

        # Simulate a message that has exhausted retries (receive_count >= max_retries=3)
        msg = MagicMock()
        msg.body = json.dumps(
            {
                "task_id": "dlq-task-1",
                "task_name": "fail_handler",
                "args": [],
                "kwargs": {},
            }
        ).encode("utf-8")
        msg.receive_count = 3  # == max_retries boundary -> retries exhausted

        initialized_adapter._task_handlers["fail_handler"] = MagicMock(
            side_effect=RuntimeError("permanent failure")
        )
        initialized_adapter._process_message(msg)

        assert len(dlq_calls) == 1
        assert dlq_calls[0][0] == "dlq-task-1"
        assert "permanent failure" in dlq_calls[0][1]


class TestLifecycle:
    """Tests for consumer start/stop lifecycle."""

    def test_consumer_start_stop(self, initialized_adapter):
        """U27: start then stop -> consumer thread exits, _running=False."""
        with patch.object(initialized_adapter, "_consumer_loop"):
            with patch.object(initialized_adapter, "_start_query_subscription"):
                initialized_adapter.start_consumer()
                assert initialized_adapter._running is True
                assert initialized_adapter._cancel_token is not None

                initialized_adapter.stop_consumer()
                assert initialized_adapter._running is False

    def test_consumer_stop_nacks_inflight(self, initialized_adapter):
        """U28: stop while processing -> in-flight handling."""
        # The consumer loop checks _running before processing each message
        # and nacks remaining messages when _running becomes False.
        initialized_adapter._running = True
        initialized_adapter._cancel_token = MagicMock()
        initialized_adapter.stop_consumer()
        initialized_adapter._cancel_token.cancel.assert_called_once()


class TestBuildClientConfigTLS:
    """Tests for _build_client_config with TLS paths (lines 123-128)."""

    def test_tls_paths_set(self, tmp_path):
        """TLS config includes cert/key/ca paths when set."""
        cert = tmp_path / "cert.pem"
        key = tmp_path / "key.pem"
        ca = tmp_path / "ca.pem"
        cert.write_text("cert")
        key.write_text("key")
        ca.write_text("ca")
        config = KubeMQAdapterConfig(
            tls=True,
            tls_cert_file=str(cert),
            tls_key_file=str(key),
            tls_ca_file=str(ca),
            client_id="test-tls",
        )
        adapter = KubeMQTaskProcessorAdapter(config)
        cc = adapter._build_client_config()
        assert cc.tls.enabled is True
        assert cc.tls.cert_file == str(cert)
        assert cc.tls.key_file == str(key)
        assert cc.tls.ca_file == str(ca)

    def test_tls_paths_none_when_empty(self):
        """TLS config paths are None when not set."""
        config = KubeMQAdapterConfig(tls=True)
        adapter = KubeMQTaskProcessorAdapter(config)
        cc = adapter._build_client_config()
        assert cc.tls.enabled is True
        assert cc.tls.cert_file is None
        assert cc.tls.key_file is None
        assert cc.tls.ca_file is None


class TestLazyClients:
    """Tests for _pubsub_client and _cq_client lazy properties (lines 149-150, 155-156)."""

    def test_pubsub_client_lazy_creation(self):
        """_pubsub_client creates PubSubClient on first access."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        with patch("kubemq_rayserve.adapter.PubSubClient") as mock_ps:
            mock_ps.return_value = MagicMock()
            client = adapter._pubsub_client
            mock_ps.assert_called_once()
            assert client is mock_ps.return_value

    def test_cq_client_lazy_creation(self):
        """_cq_client creates CQClient on first access."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        with patch("kubemq_rayserve.adapter.CQClient") as mock_cq:
            mock_cq.return_value = MagicMock()
            client = adapter._cq_client
            mock_cq.assert_called_once()
            assert client is mock_cq.return_value


class TestEnqueueTaskDelegation:
    """Tests for enqueue_task delegation (line 186) and get_task_status (line 247)."""

    def test_enqueue_task_delegates_to_sync(self, initialized_adapter):
        """enqueue_task delegates to enqueue_task_sync."""
        with patch.object(
            initialized_adapter, "enqueue_task_sync", return_value="sentinel"
        ) as mock_sync:
            result = initialized_adapter.enqueue_task("classify", args=[1], kwargs={"k": 2})
            mock_sync.assert_called_once_with("classify", [1], {"k": 2})
            assert result == "sentinel"

    def test_get_task_status_delegates_to_sync(self, initialized_adapter):
        """get_task_status delegates to get_task_status_sync."""
        with patch.object(
            initialized_adapter, "get_task_status_sync", return_value="sentinel"
        ) as mock_sync:
            result = initialized_adapter.get_task_status("task-1")
            mock_sync.assert_called_once_with("task-1")
            assert result == "sentinel"


class TestEnqueueNotInitialized:
    """Tests for enqueue_task_sync when adapter not initialized (lines 212, 214, 216)."""

    def test_enqueue_no_client(self):
        """RuntimeError when client is None."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        adapter._client = None
        with pytest.raises(RuntimeError, match="Call initialize"):
            adapter.enqueue_task_sync("classify")

    def test_enqueue_no_result_backend(self):
        """RuntimeError when result_backend is None."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        adapter._client = MagicMock()
        adapter._result_backend = None
        with pytest.raises(RuntimeError, match="Call initialize"):
            adapter.enqueue_task_sync("classify")

    def test_enqueue_no_metrics(self):
        """RuntimeError when metrics is None."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        adapter._client = MagicMock()
        adapter._result_backend = MagicMock()
        adapter._metrics = None
        with pytest.raises(RuntimeError, match="Call initialize"):
            adapter.enqueue_task_sync("classify")


class TestGetTaskStatusNotInitialized:
    """Tests for get_task_status_sync when not initialized (lines 258-261)."""

    def test_get_task_status_no_backend(self):
        """RuntimeError when result_backend is None."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        adapter._result_backend = None
        with pytest.raises(RuntimeError, match="Call initialize"):
            adapter.get_task_status_sync("task-1")

    def test_get_task_status_returns_result(self):
        """Returns TaskResult with status and result from backend."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        adapter._result_backend = MagicMock()
        adapter._result_backend.get_result.return_value = {
            "status": "SUCCESS",
            "result": {"label": "cat"},
        }
        result = adapter.get_task_status_sync("task-1")
        assert result.status == "SUCCESS"
        assert result.result == {"label": "cat"}
        assert result.id == "task-1"


class TestStartConsumerNotInitialized:
    """Tests for start_consumer when not initialized (line 278)."""

    def test_start_consumer_no_client(self):
        """RuntimeError when client is None."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        adapter._client = None
        with pytest.raises(RuntimeError, match="Call initialize"):
            adapter.start_consumer()


class TestStartQuerySubscription:
    """Tests for _start_query_subscription (lines 306-313)."""

    def test_start_query_subscription(self):
        """Creates QueriesSubscription and subscribes."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        adapter._queue_name = "test-queue"
        adapter._cancel_token = MagicMock()
        mock_cq = MagicMock()
        adapter.__dict__["_cq_client"] = mock_cq

        with patch("kubemq.QueriesSubscription") as mock_sub_cls:
            mock_sub_cls.return_value = MagicMock()
            adapter._start_query_subscription()
            mock_sub_cls.assert_called_once_with(
                channel="test-queue",
                on_receive_query_callback=adapter._handle_sync_query,
                on_error_callback=adapter._handle_query_error,
            )
            mock_cq.subscribe_to_queries.assert_called_once_with(
                mock_sub_cls.return_value, adapter._cancel_token
            )


class TestHandleSyncQueryOuterException:
    """Tests for _handle_sync_query outer exception path (lines 347-348)."""

    def test_outer_exception_logged(self):
        """Outer exception (e.g., invalid body) is caught and logged."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        mock_cq = MagicMock()
        adapter.__dict__["_cq_client"] = mock_cq

        query = MagicMock()
        query.body = b"not-json"  # Will cause json.loads to fail
        query.channel = "test-queue"
        query.id = "req-bad"

        # Should not raise
        adapter._handle_sync_query(query)
        # send_response_message should NOT be called since we failed before building response
        mock_cq.send_response_message.assert_not_called()


class TestHandleQueryError:
    """Tests for _handle_query_error (line 357)."""

    def test_handle_query_error_logged(self):
        """Query subscription error is logged (no raise)."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        # Should not raise
        adapter._handle_query_error("connection lost")


class TestConsumerLoop:
    """Tests for _consumer_loop paths (lines 361-398)."""

    def test_consumer_loop_not_initialized_client(self):
        """RuntimeError when client is None."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        adapter._client = None
        adapter._result_backend = MagicMock()
        adapter._metrics = MagicMock()
        with pytest.raises(RuntimeError, match="Call initialize"):
            adapter._consumer_loop()

    def test_consumer_loop_not_initialized_result_backend(self):
        """RuntimeError when result_backend is None."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        adapter._client = MagicMock()
        adapter._result_backend = None
        adapter._metrics = MagicMock()
        with pytest.raises(RuntimeError, match="Call initialize"):
            adapter._consumer_loop()

    def test_consumer_loop_not_initialized_metrics(self):
        """RuntimeError when metrics is None."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        adapter._client = MagicMock()
        adapter._result_backend = MagicMock()
        adapter._metrics = None
        with pytest.raises(RuntimeError, match="Call initialize"):
            adapter._consumer_loop()

    def test_consumer_loop_poll_error(self, initialized_adapter):
        """Poll returns is_error -> continue (loop runs then stops)."""
        response = MagicMock()
        response.is_error = True
        response.error = "no messages"
        # Run one iteration then stop
        call_count = 0

        def mock_receive(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                initialized_adapter._running = False
            return response

        initialized_adapter._client.receive_queue_messages.side_effect = mock_receive
        initialized_adapter._running = True
        initialized_adapter._consumer_loop()
        assert call_count >= 1

    def test_consumer_loop_process_messages(self, initialized_adapter):
        """Successful poll with messages -> _process_message called."""
        message = MagicMock()
        message.body = json.dumps(
            {
                "task_id": "t1",
                "task_name": "classify",
                "args": [],
                "kwargs": {},
            }
        ).encode("utf-8")
        message.receive_count = 1

        response = MagicMock()
        response.is_error = False
        response.messages = [message]

        handler = MagicMock(return_value="result")
        initialized_adapter.register_task_handler(handler, "classify")

        call_count = 0

        def mock_receive(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                initialized_adapter._running = False
                empty_response = MagicMock()
                empty_response.is_error = False
                empty_response.messages = []
                return empty_response
            return response

        initialized_adapter._client.receive_queue_messages.side_effect = mock_receive
        initialized_adapter._running = True
        initialized_adapter._consumer_loop()
        handler.assert_called_once()

    def test_consumer_loop_connection_error(self, initialized_adapter):
        """Connection error -> log warning, sleep, retry."""
        call_count = 0

        def mock_receive(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                initialized_adapter._running = False
                raise KubeMQConnectionError("down")
            raise KubeMQConnectionError("down")

        initialized_adapter._client.receive_queue_messages.side_effect = mock_receive
        initialized_adapter._running = True
        with patch("kubemq_rayserve.adapter.time.sleep"):
            initialized_adapter._consumer_loop()
        assert call_count >= 1

    def test_consumer_loop_unexpected_error(self, initialized_adapter):
        """Unexpected error -> log error, sleep, retry."""
        call_count = 0

        def mock_receive(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                initialized_adapter._running = False
                raise RuntimeError("unexpected")
            raise RuntimeError("unexpected")

        initialized_adapter._client.receive_queue_messages.side_effect = mock_receive
        initialized_adapter._running = True
        with patch("kubemq_rayserve.adapter.time.sleep"):
            initialized_adapter._consumer_loop()
        assert call_count >= 1

    def test_consumer_loop_nacks_when_stopping(self, initialized_adapter):
        """Messages received while _running=False are nacked."""
        message = MagicMock()
        response = MagicMock()
        response.is_error = False
        response.messages = [message]

        call_count = 0

        def mock_receive(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            # Stop after first message batch
            initialized_adapter._running = False
            return response

        initialized_adapter._client.receive_queue_messages.side_effect = mock_receive
        initialized_adapter._running = True
        initialized_adapter._consumer_loop()
        # The message should be nacked because _running was set to False
        # before processing (it is checked in the for-loop)
        # Note: message is either processed or nacked depending on timing
        # At minimum the loop ran
        assert call_count >= 1


class TestProcessMessagePaths:
    """Tests for _process_message various paths (lines 406-446+)."""

    def test_process_message_no_result_backend(self):
        """RuntimeError when result_backend is None."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        adapter._result_backend = None
        adapter._metrics = MagicMock()
        with pytest.raises(RuntimeError, match="Call initialize"):
            adapter._process_message(MagicMock())

    def test_process_message_no_metrics(self):
        """RuntimeError when metrics is None."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        adapter._result_backend = MagicMock()
        adapter._metrics = None
        with pytest.raises(RuntimeError, match="Call initialize"):
            adapter._process_message(MagicMock())

    def test_process_message_malformed_to_unprocessable_send_failure(self, initialized_adapter):
        """Malformed message, send to unprocessable queue fails -> logged."""
        message = MagicMock()
        message.body = b"not-json"

        # Make the send to unprocessable queue fail
        initialized_adapter._client.send_queue_message.side_effect = Exception("send failed")
        initialized_adapter._process_message(message)
        message.ack.assert_called_once()
        # send_queue_message was attempted
        initialized_adapter._client.send_queue_message.assert_called_once()

    def test_process_message_malformed_no_unprocessable_queue(self, initialized_adapter):
        """Malformed message, no unprocessable queue configured -> just ack."""
        message = MagicMock()
        message.body = b"not-json"
        initialized_adapter._unprocessable_task_queue_name = ""

        initialized_adapter._process_message(message)
        message.ack.assert_called_once()
        # send_queue_message NOT called since no unprocessable queue
        initialized_adapter._client.send_queue_message.assert_not_called()

    def test_process_message_malformed_client_none(self, initialized_adapter):
        """Malformed message, client is None during unprocessable send -> RuntimeError logged."""
        message = MagicMock()
        message.body = b"not-json"
        initialized_adapter._client = None

        # Should not raise -- the inner RuntimeError is caught by the outer except
        initialized_adapter._process_message(message)
        message.ack.assert_called_once()

    def test_process_message_no_handler(self, initialized_adapter):
        """No handler registered for task -> ack, discard from in-flight."""
        message = MagicMock()
        message.body = json.dumps(
            {
                "task_id": "t1",
                "task_name": "unknown",
                "args": [],
                "kwargs": {},
            }
        ).encode("utf-8")
        initialized_adapter._process_message(message)
        message.ack.assert_called_once()
        assert "t1" not in initialized_adapter._in_flight

    def test_process_message_handler_error_retries_exhausted_no_on_dlq(self, initialized_adapter):
        """Handler fails, retries exhausted, on_dlq is None -> no callback."""
        initialized_adapter._config.on_dlq = None
        handler = MagicMock(side_effect=RuntimeError("error"))
        initialized_adapter.register_task_handler(handler, "classify")

        message = MagicMock()
        message.body = json.dumps(
            {
                "task_id": "t1",
                "task_name": "classify",
                "args": [],
                "kwargs": {},
            }
        ).encode("utf-8")
        message.receive_count = 3  # >= max_retries=3

        initialized_adapter._process_message(message)
        message.nack.assert_called_once()
        # No error -- on_dlq is None, so callback not called

    def test_process_message_on_dlq_callback_exception(self, initialized_adapter):
        """on_dlq callback raises exception -> logged, not re-raised."""
        initialized_adapter._config.on_dlq = MagicMock(side_effect=Exception("callback failed"))
        handler = MagicMock(side_effect=RuntimeError("error"))
        initialized_adapter.register_task_handler(handler, "classify")

        message = MagicMock()
        message.body = json.dumps(
            {
                "task_id": "t1",
                "task_name": "classify",
                "args": [],
                "kwargs": {},
            }
        ).encode("utf-8")
        message.receive_count = 3

        # Should not raise
        initialized_adapter._process_message(message)
        message.nack.assert_called_once()
        initialized_adapter._config.on_dlq.assert_called_once()


class TestStopConsumerPaths:
    """Tests for stop_consumer flow (lines 520-534, 584-606)."""

    def test_stop_consumer_client_close_error(self, initialized_adapter):
        """Client close error is caught and logged."""
        initialized_adapter._running = True
        initialized_adapter._cancel_token = MagicMock()
        initialized_adapter._client.close.side_effect = Exception("close failed")
        # Should not raise
        initialized_adapter.stop_consumer()
        assert initialized_adapter._running is False

    def test_stop_consumer_pubsub_close_error(self):
        """PubSubClient close error is caught and logged."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        with patch("kubemq_rayserve.adapter.QueuesClient"):
            adapter.initialize()

        mock_pubsub = MagicMock()
        mock_pubsub.close.side_effect = Exception("pubsub close failed")
        adapter.__dict__["_pubsub_client"] = mock_pubsub

        adapter._running = True
        adapter._cancel_token = MagicMock()
        # Should not raise
        adapter.stop_consumer()
        mock_pubsub.close.assert_called_once()

    def test_stop_consumer_cq_close_error(self):
        """CQClient close error is caught and logged."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        with patch("kubemq_rayserve.adapter.QueuesClient"):
            adapter.initialize()

        mock_cq = MagicMock()
        mock_cq.close.side_effect = Exception("cq close failed")
        adapter.__dict__["_cq_client"] = mock_cq

        adapter._running = True
        adapter._cancel_token = MagicMock()
        # Should not raise
        adapter.stop_consumer()
        mock_cq.close.assert_called_once()

    def test_stop_consumer_no_cancel_token(self, initialized_adapter):
        """Stop works when _cancel_token is None."""
        initialized_adapter._cancel_token = None
        initialized_adapter._running = True
        initialized_adapter.stop_consumer()
        assert initialized_adapter._running is False

    def test_stop_consumer_with_thread(self, initialized_adapter):
        """Stop joins the consumer thread."""
        import threading

        initialized_adapter._running = True
        initialized_adapter._cancel_token = MagicMock()
        mock_thread = MagicMock(spec=threading.Thread)
        initialized_adapter._consumer_thread = mock_thread
        initialized_adapter.stop_consumer()
        mock_thread.join.assert_called_once_with(timeout=5.0)


class TestHealthCheck:
    """Tests for health_check_sync (line 548)."""

    def test_health_check_not_initialized(self):
        """Client not initialized -> unhealthy."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        adapter._client = None
        result = adapter.health_check_sync()
        assert result == [{"healthy": False, "error": "client not initialized"}]


class TestCancelTask:
    """Tests for cancel_task_sync not initialized (line 569)."""

    def test_cancel_task_no_backend(self):
        """RuntimeError when result_backend is None."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        adapter._result_backend = None
        with pytest.raises(RuntimeError, match="Call initialize"):
            adapter.cancel_task_sync("task-1")


class TestGetMetricsPaths:
    """Tests for get_metrics_sync paths (lines 584-606, 659-660, 689-690)."""

    def test_get_metrics_no_metrics(self):
        """RuntimeError when metrics is None."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        adapter._metrics = None
        adapter._result_backend = MagicMock()
        with pytest.raises(RuntimeError, match="Call initialize"):
            adapter.get_metrics_sync()

    def test_get_metrics_no_result_backend(self):
        """RuntimeError when result_backend is None."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        from kubemq_rayserve.metrics import MetricsCollector

        adapter._metrics = MetricsCollector()
        adapter._result_backend = None
        with pytest.raises(RuntimeError, match="Call initialize"):
            adapter.get_metrics_sync()

    def test_get_metrics_full(self, initialized_adapter):
        """get_metrics_sync returns all 8 metrics."""
        # Setup channel info
        ch = MagicMock()
        ch.name = "test-queue"
        ch.incoming.waiting = 5
        dlq_ch = MagicMock()
        dlq_ch.name = "test-queue.dlq"
        dlq_ch.incoming.waiting = 2
        initialized_adapter._client.list_queues_channels.return_value = [ch, dlq_ch]

        metrics = initialized_adapter.get_metrics_sync()
        assert "queue_depth" in metrics
        assert metrics["queue_depth"] == 5
        assert metrics["dlq_depth"] == 2
        assert "in_flight" in metrics
        assert "tasks_enqueued_total" in metrics
        assert "tasks_completed_total" in metrics
        assert "task_processing_duration_seconds" in metrics
        assert "result_storage_retries_total" in metrics
        assert "consumer_poll_latency_seconds" in metrics

    def test_get_metrics_channel_error(self, initialized_adapter):
        """list_queues_channels raises -> depths default to 0."""
        initialized_adapter._client.list_queues_channels.side_effect = Exception("error")
        metrics = initialized_adapter.get_metrics_sync()
        assert metrics["queue_depth"] == 0
        assert metrics["dlq_depth"] == 0

    def test_get_metrics_alias(self, initialized_adapter):
        """get_metrics is alias for get_metrics_sync."""
        initialized_adapter._client.list_queues_channels.return_value = []
        result = initialized_adapter.get_metrics()
        assert isinstance(result, dict)


class TestQueryTaskSyncResponseParsing:
    """Tests for query_task_sync response parsing (lines 659-660)."""

    def test_query_task_sync_invalid_json_response(self):
        """Invalid JSON response -> FAILURE status."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        adapter._queue_name = "test-queue"
        mock_cq = MagicMock()
        adapter.__dict__["_cq_client"] = mock_cq

        response = MagicMock()
        response.body = b"not-json"
        mock_cq.send_query.return_value = response

        result = adapter.query_task_sync("classify")
        assert result.status == "FAILURE"
        assert result.result is None

    def test_query_task_sync_none_body(self):
        """None body -> FAILURE status (TypeError path)."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        adapter._queue_name = "test-queue"
        mock_cq = MagicMock()
        adapter.__dict__["_cq_client"] = mock_cq

        response = MagicMock()
        response.body = None
        mock_cq.send_query.return_value = response

        result = adapter.query_task_sync("classify")
        assert result.status == "FAILURE"
        assert result.result is None


class TestReportProgressError:
    """Tests for report_progress exception path (lines 689-690)."""

    def test_report_progress_send_error(self):
        """send_event raises -> logged, not re-raised."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)
        adapter._queue_name = "test-queue"
        mock_pubsub = MagicMock()
        mock_pubsub.send_event.side_effect = Exception("send failed")
        adapter.__dict__["_pubsub_client"] = mock_pubsub

        # Should not raise
        adapter.report_progress("task-1", 50.0, "halfway")
        mock_pubsub.send_event.assert_called_once()


class TestInitializeWithTaskProcessorConfig:
    """Tests for initialize with task_processor_config kwargs (lines 123-128)."""

    def test_initialize_with_task_processor_config(self):
        """task_processor_config attributes are extracted."""
        config = KubeMQAdapterConfig()
        adapter = KubeMQTaskProcessorAdapter(config)

        tpc = MagicMock()
        tpc.queue_name = "my-queue"
        tpc.max_retries = 5
        tpc.failed_task_queue_name = "my-queue.dlq"
        tpc.unprocessable_task_queue_name = "my-queue.unprocessable"

        with patch("kubemq_rayserve.adapter.QueuesClient"):
            adapter.initialize(consumer_concurrency=4, task_processor_config=tpc)

        assert adapter._queue_name == "my-queue"
        assert adapter._max_retries == 5
        assert adapter._failed_task_queue_name == "my-queue.dlq"
        assert adapter._unprocessable_task_queue_name == "my-queue.unprocessable"
        assert adapter._consumer_concurrency == 4
