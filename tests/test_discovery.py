from unittest.mock import MagicMock

from conftest import make_node, make_replica, make_volume

from rebalancer.discovery import get_replicas, get_storage_nodes, get_volumes

NAMESPACE = "longhorn-system"


class TestGetStorageNodes:
    def test_returns_schedulable_nodes(self):
        api = MagicMock()
        api.list_namespaced_custom_object.return_value = {
            "items": [
                make_node("node-0", allow_scheduling=True),
                make_node("node-1", allow_scheduling=True),
                make_node("worker-0", allow_scheduling=False),
            ]
        }

        nodes = get_storage_nodes(api, NAMESPACE)

        assert len(nodes) == 2
        assert nodes[0]["metadata"]["name"] == "node-0"
        assert nodes[1]["metadata"]["name"] == "node-1"

    def test_returns_empty_when_no_schedulable_nodes(self):
        api = MagicMock()
        api.list_namespaced_custom_object.return_value = {
            "items": [
                make_node("worker-0", allow_scheduling=False),
            ]
        }

        nodes = get_storage_nodes(api, NAMESPACE)

        assert len(nodes) == 0

    def test_calls_kubernetes_api_correctly(self):
        api = MagicMock()
        api.list_namespaced_custom_object.return_value = {"items": []}

        get_storage_nodes(api, NAMESPACE)

        api.list_namespaced_custom_object.assert_called_once_with(
            group="longhorn.io",
            version="v1beta2",
            namespace=NAMESPACE,
            plural="nodes",
        )


class TestGetVolumes:
    def test_returns_healthy_attached_volumes(self):
        api = MagicMock()
        api.list_namespaced_custom_object.return_value = {
            "items": [
                make_volume("vol-a", state="attached", robustness="healthy"),
                make_volume("vol-b", state="attached", robustness="degraded"),
                make_volume("vol-c", state="detached", robustness="healthy"),
                make_volume("vol-d", state="attached", robustness="faulted"),
            ]
        }

        volumes = get_volumes(api, NAMESPACE)

        assert len(volumes) == 1
        assert volumes[0]["metadata"]["name"] == "vol-a"

    def test_returns_empty_when_no_healthy_volumes(self):
        api = MagicMock()
        api.list_namespaced_custom_object.return_value = {
            "items": [
                make_volume("vol-a", state="detached", robustness="unknown"),
            ]
        }

        volumes = get_volumes(api, NAMESPACE)

        assert len(volumes) == 0

    def test_calls_kubernetes_api_correctly(self):
        api = MagicMock()
        api.list_namespaced_custom_object.return_value = {"items": []}

        get_volumes(api, NAMESPACE)

        api.list_namespaced_custom_object.assert_called_once_with(
            group="longhorn.io",
            version="v1beta2",
            namespace=NAMESPACE,
            plural="volumes",
        )


class TestGetReplicas:
    def test_returns_all_replicas(self):
        api = MagicMock()
        api.list_namespaced_custom_object.return_value = {
            "items": [
                make_replica("r1", "vol-a", "node-0"),
                make_replica("r2", "vol-a", "node-1"),
                make_replica("r3", "vol-b", "node-0", state="stopped"),
            ]
        }

        replicas = get_replicas(api, NAMESPACE)

        assert len(replicas) == 3

    def test_calls_kubernetes_api_correctly(self):
        api = MagicMock()
        api.list_namespaced_custom_object.return_value = {"items": []}

        get_replicas(api, NAMESPACE)

        api.list_namespaced_custom_object.assert_called_once_with(
            group="longhorn.io",
            version="v1beta2",
            namespace=NAMESPACE,
            plural="replicas",
        )
