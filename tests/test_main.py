from unittest.mock import MagicMock, patch

from conftest import make_node, make_nodes, make_replica, make_volume

from rebalancer.main import check_cluster_health, load_config, run

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


class TestCheckClusterHealth:
    def test_returns_true_when_all_healthy(self):
        api = MagicMock()
        nodes = [
            {**make_node("node-0"), "status": {"conditions": {"Ready": {"status": "True"}}}},
            {**make_node("node-1"), "status": {"conditions": {"Ready": {"status": "True"}}}},
        ]
        api.list_namespaced_custom_object.return_value = {
            "items": [
                make_volume("vol-a", state="attached", robustness="healthy"),
            ]
        }

        assert check_cluster_health(api, "longhorn-system", nodes) is True

    def test_returns_false_when_node_not_ready(self):
        api = MagicMock()
        nodes = [
            {**make_node("node-0"), "status": {"conditions": {"Ready": {"status": "True"}}}},
            {**make_node("node-1"), "status": {"conditions": {"Ready": {"status": "False"}}}},
        ]
        api.list_namespaced_custom_object.return_value = {"items": []}

        assert check_cluster_health(api, "longhorn-system", nodes) is False

    def test_returns_false_when_faulted_volume_exists(self):
        api = MagicMock()
        nodes = [
            {**make_node("node-0"), "status": {"conditions": {"Ready": {"status": "True"}}}},
        ]
        api.list_namespaced_custom_object.return_value = {
            "items": [
                make_volume("vol-a", state="attached", robustness="faulted"),
            ]
        }

        assert check_cluster_health(api, "longhorn-system", nodes) is False

    def test_returns_false_when_degraded_volume_exists(self):
        api = MagicMock()
        nodes = [
            {**make_node("node-0"), "status": {"conditions": {"Ready": {"status": "True"}}}},
        ]
        api.list_namespaced_custom_object.return_value = {
            "items": [
                make_volume("vol-a", state="attached", robustness="healthy"),
                make_volume("vol-b", state="attached", robustness="degraded"),
            ]
        }

        assert check_cluster_health(api, "longhorn-system", nodes) is False

    def test_skips_when_node_has_no_status(self):
        api = MagicMock()
        nodes = [make_node("node-0"), make_node("node-1")]
        api.list_namespaced_custom_object.return_value = {"items": []}

        assert check_cluster_health(api, "longhorn-system", nodes) is False


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

    def _healthy_nodes(self, count):
        return [
            {**make_node(f"node-{i}"), "status": {"conditions": {"Ready": {"status": "True"}}}}
            for i in range(count)
        ]

    @patch("rebalancer.main.client")
    @patch("rebalancer.main.config")
    def test_exits_cleanly_when_fewer_than_two_nodes(self, _mock_config, mock_client):
        mock_api = MagicMock()
        mock_client.CustomObjectsApi.return_value = mock_api
        mock_api.list_namespaced_custom_object.return_value = {"items": [make_node("node-0")]}

        result = run(self._base_config())

        assert result == 0

    @patch("rebalancer.main.check_cluster_health", return_value=True)
    @patch("rebalancer.main.client")
    @patch("rebalancer.main.config")
    def test_exits_cleanly_when_no_healthy_volumes(self, _mock_config, mock_client, _mock_health):
        mock_api = MagicMock()
        mock_client.CustomObjectsApi.return_value = mock_api
        mock_api.list_namespaced_custom_object.side_effect = [
            {"items": make_nodes(3)},
            {"items": []},
        ]

        result = run(self._base_config())

        assert result == 0

    @patch("rebalancer.main.check_cluster_health", return_value=True)
    @patch("rebalancer.main.client")
    @patch("rebalancer.main.config")
    def test_exits_cleanly_when_all_balanced(self, _mock_config, mock_client, _mock_health):
        mock_api = MagicMock()
        mock_client.CustomObjectsApi.return_value = mock_api
        nodes = make_nodes(3)
        volumes = [make_volume("vol-a"), make_volume("vol-b"), make_volume("vol-c")]
        replicas = [
            make_replica("r1", "vol-a", "node-0"),
            make_replica("r2", "vol-a", "node-1"),
            make_replica("r3", "vol-b", "node-1"),
            make_replica("r4", "vol-b", "node-2"),
            make_replica("r5", "vol-c", "node-0"),
            make_replica("r6", "vol-c", "node-2"),
        ]
        mock_api.list_namespaced_custom_object.side_effect = [
            {"items": nodes},
            {"items": volumes},
            {"items": replicas},
        ]

        result = run(self._base_config())

        assert result == 0
        mock_api.delete_namespaced_custom_object.assert_not_called()

    @patch("rebalancer.main.check_cluster_health", return_value=True)
    @patch("rebalancer.main.client")
    @patch("rebalancer.main.config")
    def test_dry_run_logs_but_does_not_delete(self, _mock_config, mock_client, _mock_health):
        mock_api = MagicMock()
        mock_client.CustomObjectsApi.return_value = mock_api
        nodes = make_nodes(3)
        volumes = [make_volume("vol-a"), make_volume("vol-b")]
        replicas = [
            make_replica("r1", "vol-a", "node-0"),
            make_replica("r2", "vol-a", "node-1"),
            make_replica("r3", "vol-b", "node-0"),
            make_replica("r4", "vol-b", "node-1"),
        ]
        mock_api.list_namespaced_custom_object.side_effect = [
            {"items": nodes},
            {"items": volumes},
            {"items": replicas},
        ]

        result = run(self._base_config(dry_run=True))

        assert result == 0
        mock_api.delete_namespaced_custom_object.assert_not_called()

    @patch("rebalancer.main.check_cluster_health", return_value=True)
    @patch("rebalancer.main.client")
    @patch("rebalancer.main.config")
    def test_deletes_replica_when_not_dry_run(self, _mock_config, mock_client, _mock_health):
        mock_api = MagicMock()
        mock_client.CustomObjectsApi.return_value = mock_api
        nodes = make_nodes(3)
        volumes = [make_volume("vol-a"), make_volume("vol-b")]
        replicas = [
            make_replica("r1", "vol-a", "node-0"),
            make_replica("r2", "vol-a", "node-1"),
            make_replica("r3", "vol-b", "node-0"),
            make_replica("r4", "vol-b", "node-1"),
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

    @patch("rebalancer.main.check_cluster_health", return_value=True)
    @patch("rebalancer.main.client")
    @patch("rebalancer.main.config")
    def test_respects_max_rebalances_limit(self, _mock_config, mock_client, _mock_health):
        mock_api = MagicMock()
        mock_client.CustomObjectsApi.return_value = mock_api
        nodes = make_nodes(3)
        volumes = [make_volume(f"vol-{c}") for c in "abc"]
        replicas = [
            make_replica(f"r{i * 2 + 1}", f"vol-{c}", "node-0") for i, c in enumerate("abc")
        ] + [make_replica(f"r{i * 2 + 2}", f"vol-{c}", "node-1") for i, c in enumerate("abc")]
        mock_api.list_namespaced_custom_object.side_effect = [
            {"items": nodes},
            {"items": volumes},
            {"items": replicas},
        ]
        mock_api.get_namespaced_custom_object.return_value = {
            "status": {"state": "attached", "robustness": "healthy"}
        }

        result = run(self._base_config(dry_run=False, max_rebalances=1))

        assert result == 0
        assert mock_api.delete_namespaced_custom_object.call_count == 1

    @patch("rebalancer.main.check_cluster_health", return_value=True)
    @patch("rebalancer.main.wait_for_healthy")
    @patch("rebalancer.main.client")
    @patch("rebalancer.main.config")
    def test_aborts_on_rebuild_timeout(self, _mock_config, mock_client, mock_wait, _mock_health):
        mock_api = MagicMock()
        mock_client.CustomObjectsApi.return_value = mock_api
        nodes = make_nodes(3)
        volumes = [make_volume("vol-a")]
        replicas = [
            make_replica("r1", "vol-a", "node-0"),
            make_replica("r2", "vol-a", "node-1"),
        ]
        mock_api.list_namespaced_custom_object.side_effect = [
            {"items": nodes},
            {"items": volumes},
            {"items": replicas},
        ]
        mock_wait.return_value = False

        result = run(self._base_config(dry_run=False))

        assert result == 1

    @patch("rebalancer.main.check_cluster_health", return_value=False)
    @patch("rebalancer.main.client")
    @patch("rebalancer.main.config")
    def test_skips_when_cluster_unhealthy(self, _mock_config, mock_client, _mock_health):
        mock_api = MagicMock()
        mock_client.CustomObjectsApi.return_value = mock_api
        mock_api.list_namespaced_custom_object.return_value = {"items": make_nodes(3)}

        result = run(self._base_config())

        assert result == 0
        mock_api.delete_namespaced_custom_object.assert_not_called()

    @patch("rebalancer.main.check_cluster_health", return_value=True)
    @patch("rebalancer.main.client")
    @patch("rebalancer.main.config")
    def test_multi_iteration_refetches_replicas(self, _mock_config, mock_client, _mock_health):
        mock_api = MagicMock()
        mock_client.CustomObjectsApi.return_value = mock_api
        nodes = make_nodes(3)
        volumes = [make_volume("vol-a"), make_volume("vol-b")]
        replicas_initial = [
            make_replica("r1", "vol-a", "node-0"),
            make_replica("r2", "vol-a", "node-1"),
            make_replica("r3", "vol-b", "node-0"),
            make_replica("r4", "vol-b", "node-1"),
        ]
        replicas_after_first = [
            make_replica("r1", "vol-a", "node-0"),
            make_replica("r2-new", "vol-a", "node-2"),
            make_replica("r3", "vol-b", "node-0"),
            make_replica("r4", "vol-b", "node-1"),
        ]
        mock_api.list_namespaced_custom_object.side_effect = [
            {"items": nodes},
            {"items": volumes},
            {"items": replicas_initial},
            {"items": replicas_after_first},
        ]
        mock_api.get_namespaced_custom_object.return_value = {
            "status": {"state": "attached", "robustness": "healthy"}
        }

        result = run(self._base_config(dry_run=False, max_rebalances=2))

        assert result == 0
        assert mock_api.delete_namespaced_custom_object.call_count == 2

    @patch("rebalancer.main.check_cluster_health", return_value=True)
    @patch("rebalancer.main.client")
    @patch("rebalancer.main.config")
    def test_stops_when_no_more_rebalanceable(self, _mock_config, mock_client, _mock_health):
        mock_api = MagicMock()
        mock_client.CustomObjectsApi.return_value = mock_api
        nodes = make_nodes(3)
        volumes = [make_volume("vol-a"), make_volume("vol-b"), make_volume("vol-c")]
        replicas_imbalanced = [
            make_replica("r1", "vol-a", "node-0"),
            make_replica("r2", "vol-a", "node-1"),
            make_replica("r3", "vol-b", "node-0"),
            make_replica("r4", "vol-b", "node-1"),
            make_replica("r5", "vol-c", "node-0"),
            make_replica("r6", "vol-c", "node-2"),
        ]
        replicas_balanced = [
            make_replica("r1", "vol-a", "node-0"),
            make_replica("r2-new", "vol-a", "node-2"),
            make_replica("r3-new", "vol-b", "node-1"),
            make_replica("r4", "vol-b", "node-2"),
            make_replica("r5", "vol-c", "node-0"),
            make_replica("r6", "vol-c", "node-1"),
        ]

        call_count = [0]
        responses = [
            {"items": nodes},
            {"items": volumes},
            {"items": replicas_imbalanced},
            {"items": replicas_balanced},
        ]

        def side_effect(**_kwargs):
            idx = min(call_count[0], len(responses) - 1)
            call_count[0] += 1
            return responses[idx]

        mock_api.list_namespaced_custom_object.side_effect = side_effect
        mock_api.get_namespaced_custom_object.return_value = {
            "status": {"state": "attached", "robustness": "healthy"}
        }

        result = run(self._base_config(dry_run=False, max_rebalances=5))

        assert result == 0
        assert mock_api.delete_namespaced_custom_object.call_count == 1

    @patch("rebalancer.main.check_cluster_health", return_value=True)
    @patch("rebalancer.main.client")
    @patch("rebalancer.main.config")
    def test_16_volumes_on_two_nodes_rebalances_one(self, _mock_config, mock_client, _mock_health):
        mock_api = MagicMock()
        mock_client.CustomObjectsApi.return_value = mock_api
        nodes = make_nodes(3)
        volumes = [make_volume(f"pvc-{i}") for i in range(16)]
        replicas = []
        for i in range(16):
            replicas.append(make_replica(f"pvc-{i}-r1", f"pvc-{i}", "node-0"))
            replicas.append(make_replica(f"pvc-{i}-r2", f"pvc-{i}", "node-1"))
        mock_api.list_namespaced_custom_object.side_effect = [
            {"items": nodes},
            {"items": volumes},
            {"items": replicas},
        ]
        mock_api.get_namespaced_custom_object.return_value = {
            "status": {"state": "attached", "robustness": "healthy"}
        }

        result = run(self._base_config(dry_run=False, max_rebalances=1))

        assert result == 0
        assert mock_api.delete_namespaced_custom_object.call_count == 1

    @patch("rebalancer.main.check_cluster_health", return_value=True)
    @patch("rebalancer.main.client")
    @patch("rebalancer.main.config")
    def test_works_with_five_nodes(self, _mock_config, mock_client, _mock_health):
        mock_api = MagicMock()
        mock_client.CustomObjectsApi.return_value = mock_api
        nodes = make_nodes(5)
        volumes = [make_volume("vol-a"), make_volume("vol-b")]
        replicas = [
            make_replica("r1", "vol-a", "node-0"),
            make_replica("r2", "vol-a", "node-1"),
            make_replica("r3", "vol-b", "node-0"),
            make_replica("r4", "vol-b", "node-1"),
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
