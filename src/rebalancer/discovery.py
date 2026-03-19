import logging
from typing import Any

logger = logging.getLogger(__name__)

LONGHORN_GROUP = "longhorn.io"
LONGHORN_VERSION = "v1beta2"


def get_storage_nodes(api: Any, namespace: str) -> list[dict]:
    result = api.list_namespaced_custom_object(
        group=LONGHORN_GROUP,
        version=LONGHORN_VERSION,
        namespace=namespace,
        plural="nodes",
    )
    nodes = [
        node
        for node in result.get("items", [])
        if node.get("spec", {}).get("allowScheduling", False)
    ]
    logger.info("Found %d schedulable storage nodes", len(nodes))
    return nodes


def get_volumes(api: Any, namespace: str) -> list[dict]:
    result = api.list_namespaced_custom_object(
        group=LONGHORN_GROUP,
        version=LONGHORN_VERSION,
        namespace=namespace,
        plural="volumes",
    )
    volumes = []
    for vol in result.get("items", []):
        state = vol.get("status", {}).get("state", "")
        robustness = vol.get("status", {}).get("robustness", "")
        if state == "attached" and robustness == "healthy":
            volumes.append(vol)
        else:
            name = vol.get("metadata", {}).get("name", "unknown")
            logger.debug("Skipping volume %s (state=%s, robustness=%s)", name, state, robustness)
    logger.info("Found %d healthy attached volumes", len(volumes))
    return volumes


def get_replicas(api: Any, namespace: str) -> list[dict]:
    result = api.list_namespaced_custom_object(
        group=LONGHORN_GROUP,
        version=LONGHORN_VERSION,
        namespace=namespace,
        plural="replicas",
    )
    replicas: list[dict] = result.get("items", [])
    logger.info("Found %d replicas", len(replicas))
    return replicas


def get_volume(api: Any, namespace: str, name: str) -> dict:
    result: dict = api.get_namespaced_custom_object(
        group=LONGHORN_GROUP,
        version=LONGHORN_VERSION,
        namespace=namespace,
        plural="volumes",
        name=name,
    )
    return result
