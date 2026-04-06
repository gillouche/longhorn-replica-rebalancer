import pytest


def make_node(name: str, allow_scheduling: bool = True) -> dict:
    return {
        "apiVersion": "longhorn.io/v1beta2",
        "kind": "Node",
        "metadata": {"name": name, "namespace": "longhorn-system"},
        "spec": {"allowScheduling": allow_scheduling},
    }


def make_volume(
    name: str,
    state: str = "attached",
    robustness: str = "healthy",
    replicas: int = 2,
) -> dict:
    return {
        "apiVersion": "longhorn.io/v1beta2",
        "kind": "Volume",
        "metadata": {"name": name, "namespace": "longhorn-system"},
        "spec": {"numberOfReplicas": replicas},
        "status": {"state": state, "robustness": robustness},
    }


def make_replica(
    name: str,
    volume_name: str,
    node_id: str,
    state: str = "running",
) -> dict:
    return {
        "apiVersion": "longhorn.io/v1beta2",
        "kind": "Replica",
        "metadata": {"name": name, "namespace": "longhorn-system"},
        "spec": {"volumeName": volume_name, "nodeID": node_id},
        "status": {"currentState": state},
    }


def make_nodes(count: int) -> list[dict]:
    return [make_node(f"node-{i}") for i in range(count)]


@pytest.fixture
def three_nodes():
    return make_nodes(3)


@pytest.fixture
def five_nodes():
    return make_nodes(5)
