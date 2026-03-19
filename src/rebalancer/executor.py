import logging
import time
from typing import Any

from rebalancer.discovery import LONGHORN_GROUP, LONGHORN_VERSION, get_volume

logger = logging.getLogger(__name__)


def delete_replica(
    api: Any,
    namespace: str,
    replica_name: str,
    dry_run: bool = True,
) -> bool:
    if dry_run:
        logger.info("[DRY-RUN] Would delete replica %s", replica_name)
        return True

    logger.info("Deleting replica %s", replica_name)
    api.delete_namespaced_custom_object(
        group=LONGHORN_GROUP,
        version=LONGHORN_VERSION,
        namespace=namespace,
        plural="replicas",
        name=replica_name,
    )
    logger.info("Replica %s deleted", replica_name)
    return True


def wait_for_healthy(
    api: Any,
    namespace: str,
    volume_name: str,
    timeout: int = 1800,
    poll_interval: int = 30,
    dry_run: bool = True,
) -> bool:
    if dry_run:
        logger.info("[DRY-RUN] Would wait for volume %s to become healthy", volume_name)
        return True

    deadline = time.monotonic() + timeout
    logger.info("Waiting for volume %s to become healthy (timeout=%ds)", volume_name, timeout)

    while time.monotonic() < deadline:
        volume = get_volume(api, namespace, volume_name)
        robustness = volume.get("status", {}).get("robustness", "")
        state = volume.get("status", {}).get("state", "")

        if state == "attached" and robustness == "healthy":
            logger.info("Volume %s is healthy", volume_name)
            return True

        logger.info(
            "Volume %s: state=%s, robustness=%s, waiting...",
            volume_name,
            state,
            robustness,
        )
        time.sleep(poll_interval)

    logger.error("Timeout waiting for volume %s to become healthy", volume_name)
    return False
