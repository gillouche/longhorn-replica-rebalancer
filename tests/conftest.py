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


@pytest.fixture
def three_nodes():
    return [
        make_node("homeserver1"),
        make_node("homeserver2"),
        make_node("homeserver3"),
    ]


@pytest.fixture
def balanced_replicas():
    return [
        make_replica("vol-a-r1", "vol-a", "homeserver1"),
        make_replica("vol-a-r2", "vol-a", "homeserver2"),
        make_replica("vol-b-r1", "vol-b", "homeserver2"),
        make_replica("vol-b-r2", "vol-b", "homeserver3"),
    ]


@pytest.fixture
def imbalanced_replicas():
    return [
        make_replica("vol-a-r1", "vol-a", "homeserver1"),
        make_replica("vol-a-r2", "vol-a", "homeserver2"),
        make_replica("vol-b-r1", "vol-b", "homeserver1"),
        make_replica("vol-b-r2", "vol-b", "homeserver2"),
    ]
