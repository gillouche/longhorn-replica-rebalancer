from unittest.mock import MagicMock, patch

from rebalancer.executor import delete_replica, wait_for_healthy

NAMESPACE = "longhorn-system"


class TestDeleteReplica:
    def test_dry_run_does_not_call_api(self):
        api = MagicMock()

        result = delete_replica(api, NAMESPACE, "replica-1", dry_run=True)

        assert result is True
        api.delete_namespaced_custom_object.assert_not_called()

    def test_deletes_replica_via_api(self):
        api = MagicMock()

        result = delete_replica(api, NAMESPACE, "replica-1", dry_run=False)

        assert result is True
        api.delete_namespaced_custom_object.assert_called_once_with(
            group="longhorn.io",
            version="v1beta2",
            namespace=NAMESPACE,
            plural="replicas",
            name="replica-1",
        )


class TestWaitForHealthy:
    def test_dry_run_returns_immediately(self):
        api = MagicMock()

        result = wait_for_healthy(api, NAMESPACE, "vol-a", dry_run=True)

        assert result is True
        api.get_namespaced_custom_object.assert_not_called()

    @patch("rebalancer.executor.time.sleep")
    @patch("rebalancer.executor.time.monotonic")
    def test_returns_true_when_volume_becomes_healthy(self, mock_monotonic, _mock_sleep):
        mock_monotonic.side_effect = [0, 0, 30]
        api = MagicMock()
        api.get_namespaced_custom_object.side_effect = [
            {"status": {"state": "attached", "robustness": "degraded"}},
            {"status": {"state": "attached", "robustness": "healthy"}},
        ]

        result = wait_for_healthy(
            api, NAMESPACE, "vol-a", timeout=60, poll_interval=10, dry_run=False
        )

        assert result is True

    @patch("rebalancer.executor.time.sleep")
    @patch("rebalancer.executor.time.monotonic")
    def test_returns_false_on_timeout(self, mock_monotonic, _mock_sleep):
        mock_monotonic.side_effect = [0, 0, 100]
        api = MagicMock()
        api.get_namespaced_custom_object.return_value = {
            "status": {"state": "attached", "robustness": "degraded"}
        }

        result = wait_for_healthy(
            api, NAMESPACE, "vol-a", timeout=60, poll_interval=10, dry_run=False
        )

        assert result is False

    @patch("rebalancer.executor.time.sleep")
    @patch("rebalancer.executor.time.monotonic")
    def test_immediately_healthy_returns_true(self, mock_monotonic, _mock_sleep):
        mock_monotonic.side_effect = [0, 0]
        api = MagicMock()
        api.get_namespaced_custom_object.return_value = {
            "status": {"state": "attached", "robustness": "healthy"}
        }

        result = wait_for_healthy(
            api, NAMESPACE, "vol-a", timeout=60, poll_interval=10, dry_run=False
        )

        assert result is True
        _mock_sleep.assert_not_called()
