from unittest.mock import MagicMock, patch

from conftest import make_node, make_replica, make_volume

from rebalancer.main import load_config, run

NAMESPACE = "longhorn-system"


class TestLoadConfig:
    def test_defaults(self):
        cfg = load_config()

        assert cfg["max_rebalances"] == 1
        assert cfg["rebuild_timeout"] == 1800
        assert cfg["poll_interval"] == 30
        assert cfg["dry_run"] is True
        assert cfg["log_level"] == "INFO"
        assert cfg["namespace"] == "longhorn-system"

    @patch.dict("os.environ", {"DRY_RUN": "false", "MAX_REBALANCES_PER_RUN": "3"})
    def test_reads_from_environment(self):
        cfg = load_config()

        assert cfg["dry_run"] is False
        assert cfg["max_rebalances"] == 3

    @patch.dict("os.environ", {"DRY_RUN": "True"})
    def test_dry_run_case_insensitive(self):
        cfg = load_config()

        assert cfg["dry_run"] is True


class TestRun:
    def _base_config(self, **overrides):
        cfg = {
            "max_rebalances": 1,
            "rebuild_timeout": 60,
            "poll_interval": 5,
            "dry_run": True,
            "log_level": "INFO",
            "namespace": NAMESPACE,
        }
        cfg.update(overrides)
        return cfg

    @patch("rebalancer.main.client")
    @patch("rebalancer.main.config")
    def test_exits_cleanly_when_fewer_than_two_nodes(self, _mock_config, mock_client):
        mock_api = MagicMock()
        mock_client.CustomObjectsApi.return_value = mock_api
        mock_api.list_namespaced_custom_object.return_value = {"items": [make_node("homeserver1")]}

        result = run(self._base_config())

        assert result == 0

    @patch("rebalancer.main.client")
    @patch("rebalancer.main.config")
    def test_exits_cleanly_when_no_healthy_volumes(self, _mock_config, mock_client):
        mock_api = MagicMock()
        mock_client.CustomObjectsApi.return_value = mock_api
        mock_api.list_namespaced_custom_object.side_effect = [
            {"items": [make_node("hs1"), make_node("hs2"), make_node("hs3")]},
            {"items": []},
        ]

        result = run(self._base_config())

        assert result == 0

    @patch("rebalancer.main.client")
    @patch("rebalancer.main.config")
    def test_exits_cleanly_when_all_balanced(self, _mock_config, mock_client):
        mock_api = MagicMock()
        mock_client.CustomObjectsApi.return_value = mock_api
        nodes = [make_node("hs1"), make_node("hs2"), make_node("hs3")]
        volumes = [make_volume("vol-a"), make_volume("vol-b"), make_volume("vol-c")]
        replicas = [
            make_replica("r1", "vol-a", "hs1"),
            make_replica("r2", "vol-a", "hs2"),
            make_replica("r3", "vol-b", "hs2"),
            make_replica("r4", "vol-b", "hs3"),
            make_replica("r5", "vol-c", "hs1"),
            make_replica("r6", "vol-c", "hs3"),
        ]
        mock_api.list_namespaced_custom_object.side_effect = [
            {"items": nodes},
            {"items": volumes},
            {"items": replicas},
        ]

        result = run(self._base_config())

        assert result == 0
        mock_api.delete_namespaced_custom_object.assert_not_called()

    @patch("rebalancer.main.client")
    @patch("rebalancer.main.config")
    def test_dry_run_logs_but_does_not_delete(self, _mock_config, mock_client):
        mock_api = MagicMock()
        mock_client.CustomObjectsApi.return_value = mock_api
        nodes = [make_node("hs1"), make_node("hs2"), make_node("hs3")]
        volumes = [make_volume("vol-a"), make_volume("vol-b")]
        replicas = [
            make_replica("r1", "vol-a", "hs1"),
            make_replica("r2", "vol-a", "hs2"),
            make_replica("r3", "vol-b", "hs1"),
            make_replica("r4", "vol-b", "hs2"),
        ]
        mock_api.list_namespaced_custom_object.side_effect = [
            {"items": nodes},
            {"items": volumes},
            {"items": replicas},
        ]

        result = run(self._base_config(dry_run=True))

        assert result == 0
        mock_api.delete_namespaced_custom_object.assert_not_called()

    @patch("rebalancer.main.client")
    @patch("rebalancer.main.config")
    def test_deletes_replica_when_not_dry_run(self, _mock_config, mock_client):
        mock_api = MagicMock()
        mock_client.CustomObjectsApi.return_value = mock_api
        nodes = [make_node("hs1"), make_node("hs2"), make_node("hs3")]
        volumes = [make_volume("vol-a"), make_volume("vol-b")]
        replicas = [
            make_replica("r1", "vol-a", "hs1"),
            make_replica("r2", "vol-a", "hs2"),
            make_replica("r3", "vol-b", "hs1"),
            make_replica("r4", "vol-b", "hs2"),
        ]
        mock_api.list_namespaced_custom_object.side_effect = [
            {"items": nodes},
            {"items": volumes},
            {"items": replicas},
        ]
        mock_api.get_namespaced_custom_object.return_value = {
            "status": {"state": "attached", "robustness": "healthy"}
        }

        result = run(self._base_config(dry_run=False))

        assert result == 0
        mock_api.delete_namespaced_custom_object.assert_called_once()

    @patch("rebalancer.main.client")
    @patch("rebalancer.main.config")
    def test_respects_max_rebalances_limit(self, _mock_config, mock_client):
        mock_api = MagicMock()
        mock_client.CustomObjectsApi.return_value = mock_api
        nodes = [make_node("hs1"), make_node("hs2"), make_node("hs3")]
        volumes = [
            make_volume("vol-a"),
            make_volume("vol-b"),
            make_volume("vol-c"),
        ]
        replicas_initial = [
            make_replica("r1", "vol-a", "hs1"),
            make_replica("r2", "vol-a", "hs2"),
            make_replica("r3", "vol-b", "hs1"),
            make_replica("r4", "vol-b", "hs2"),
            make_replica("r5", "vol-c", "hs1"),
            make_replica("r6", "vol-c", "hs2"),
        ]
        mock_api.list_namespaced_custom_object.side_effect = [
            {"items": nodes},
            {"items": volumes},
            {"items": replicas_initial},
        ]
        mock_api.get_namespaced_custom_object.return_value = {
            "status": {"state": "attached", "robustness": "healthy"}
        }

        result = run(self._base_config(dry_run=False, max_rebalances=1))

        assert result == 0
        assert mock_api.delete_namespaced_custom_object.call_count == 1

    @patch("rebalancer.main.wait_for_healthy")
    @patch("rebalancer.main.client")
    @patch("rebalancer.main.config")
    def test_aborts_on_rebuild_timeout(self, _mock_config, mock_client, mock_wait):
        mock_api = MagicMock()
        mock_client.CustomObjectsApi.return_value = mock_api
        nodes = [make_node("hs1"), make_node("hs2"), make_node("hs3")]
        volumes = [make_volume("vol-a")]
        replicas = [
            make_replica("r1", "vol-a", "hs1"),
            make_replica("r2", "vol-a", "hs2"),
        ]
        mock_api.list_namespaced_custom_object.side_effect = [
            {"items": nodes},
            {"items": volumes},
            {"items": replicas},
        ]
        mock_wait.return_value = False

        result = run(self._base_config(dry_run=False))

        assert result == 1
