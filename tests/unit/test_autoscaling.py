"""Unit tests for kubemq_queue_depth_policy."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from kubemq_rayserve.autoscaling import kubemq_queue_depth_policy


class TestAutoscalingPolicy:
    """Tests for kubemq_queue_depth_policy."""

    def test_autoscaling_policy_scaling(self):
        """U29: queue_depth=25, tasks_per_replica=5 -> desired_replicas=5."""
        channel = MagicMock()
        channel.name = "ml-tasks"
        channel.incoming.waiting = 25

        ctxs = {"deploy-1": MagicMock()}

        with patch("kubemq_rayserve.autoscaling._get_or_create_client") as mock_get_client:
            mock_client = MagicMock()
            mock_client.list_queues_channels.return_value = [channel]
            mock_get_client.return_value = mock_client

            decisions, state = kubemq_queue_depth_policy(
                ctxs,
                kubemq_address="localhost:50000",
                queue_name="ml-tasks",
                tasks_per_replica=5,
            )

        assert decisions["deploy-1"] == 5
        assert state["queue_depth"] == 25

    def test_autoscaling_policy_error(self):
        """U30: list_queues_channels raises -> current replica count returned."""
        ctxs = {"deploy-1": MagicMock(current_num_replicas=3)}

        with patch("kubemq_rayserve.autoscaling._get_or_create_client") as mock_get_client:
            mock_client = MagicMock()
            mock_client.list_queues_channels.side_effect = Exception("broker down")
            mock_get_client.return_value = mock_client

            decisions, state = kubemq_queue_depth_policy(
                ctxs,
                kubemq_address="localhost:50000",
                queue_name="ml-tasks",
            )

        assert decisions["deploy-1"] == 3


class TestGetOrCreateClient:
    """Tests for _get_or_create_client (lines 38-48)."""

    def test_creates_new_client(self):
        """New address/token pair creates a new client."""
        from kubemq_rayserve.autoscaling import _client_cache, _get_or_create_client

        with patch("kubemq_rayserve.autoscaling.QueuesClient") as mock_cls:
            mock_cls.return_value = MagicMock()
            # Use unique address to avoid cache collision
            client = _get_or_create_client("unique-test:50000", "my-token")
            mock_cls.assert_called_once()
            assert client is mock_cls.return_value
            # Cleanup
            import hashlib

            token_hash = hashlib.sha256(b"my-token").hexdigest()[:16]
            key = f"unique-test:50000:{token_hash}"
            _client_cache.pop(key, None)

    def test_caches_client(self):
        """Same address/token returns cached client."""
        from kubemq_rayserve.autoscaling import _client_cache, _get_or_create_client

        with patch("kubemq_rayserve.autoscaling.QueuesClient") as mock_cls:
            mock_cls.return_value = MagicMock()
            c1 = _get_or_create_client("cache-test:50000", "")
            c2 = _get_or_create_client("cache-test:50000", "")
            # Only created once
            assert mock_cls.call_count == 1
            assert c1 is c2
            # Cleanup
            key = "cache-test:50000:"
            _client_cache.pop(key, None)

    def test_creates_client_with_empty_auth(self):
        """Empty auth_token uses 'noauth' in client_id."""
        from kubemq_rayserve.autoscaling import _client_cache, _get_or_create_client

        with patch("kubemq_rayserve.autoscaling.QueuesClient") as mock_cls:
            mock_cls.return_value = MagicMock()
            _get_or_create_client("noauth-test:50000", "")
            call_kwargs = mock_cls.call_args
            config = call_kwargs[1]["config"] if "config" in call_kwargs[1] else call_kwargs[0][0]
            assert "noauth" in config.client_id
            # Cleanup
            key = "noauth-test:50000:"
            _client_cache.pop(key, None)


class TestCleanupClients:
    """Tests for _cleanup_clients (lines 53-59)."""

    def test_cleanup_closes_all_clients(self):
        """All cached clients are closed and cache is cleared."""
        from kubemq_rayserve.autoscaling import _cleanup_clients, _client_cache

        mock_client1 = MagicMock()
        mock_client2 = MagicMock()
        _client_cache["cleanup-test-1"] = mock_client1
        _client_cache["cleanup-test-2"] = mock_client2

        _cleanup_clients()

        mock_client1.close.assert_called_once()
        mock_client2.close.assert_called_once()
        assert "cleanup-test-1" not in _client_cache
        assert "cleanup-test-2" not in _client_cache

    def test_cleanup_handles_close_exception(self):
        """Client close exception is swallowed."""
        from kubemq_rayserve.autoscaling import _cleanup_clients, _client_cache

        mock_client = MagicMock()
        mock_client.close.side_effect = Exception("close failed")
        _client_cache["cleanup-exc-test"] = mock_client

        # Should not raise
        _cleanup_clients()
        assert "cleanup-exc-test" not in _client_cache
