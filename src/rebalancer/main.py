import logging
import os
import sys

from kubernetes import client, config

from rebalancer.balancer import (
    find_imbalanced_volumes,
    map_replica_placement,
    select_donor_and_volume,
)
from rebalancer.discovery import get_replicas, get_storage_nodes, get_volumes
from rebalancer.executor import delete_replica, wait_for_healthy

logger = logging.getLogger(__name__)


def load_config() -> dict:
    return {
        "max_rebalances": int(os.environ.get("MAX_REBALANCES_PER_RUN", "1")),
        "rebuild_timeout": int(os.environ.get("REBUILD_TIMEOUT_SECONDS", "1800")),
        "poll_interval": int(os.environ.get("POLL_INTERVAL_SECONDS", "30")),
        "dry_run": os.environ.get("DRY_RUN", "true").lower() == "true",
        "log_level": os.environ.get("LOG_LEVEL", "INFO"),
        "namespace": os.environ.get("LONGHORN_NAMESPACE", "longhorn-system"),
    }


def run(cfg: dict) -> int:
    namespace = cfg["namespace"]
    dry_run = cfg["dry_run"]
    max_rebalances = cfg["max_rebalances"]

    if dry_run:
        logger.info("Running in DRY-RUN mode")

    try:
        config.load_incluster_config()
    except config.ConfigException:
        logger.info("Not in cluster, loading kubeconfig")
        config.load_kube_config()

    api = client.CustomObjectsApi()

    storage_nodes = get_storage_nodes(api, namespace)
    if len(storage_nodes) < 2:
        logger.info("Fewer than 2 storage nodes, nothing to rebalance")
        return 0

    volumes = get_volumes(api, namespace)
    if not volumes:
        logger.info("No healthy attached volumes found")
        return 0

    replicas = get_replicas(api, namespace)
    placement = map_replica_placement(volumes, replicas)
    imbalanced = find_imbalanced_volumes(placement, storage_nodes)

    if not imbalanced:
        logger.info("All volumes are balanced, nothing to do")
        return 0

    logger.info(
        "Found %d imbalanced volumes, max rebalances per run: %d",
        len(imbalanced),
        max_rebalances,
    )

    rebalanced = 0
    while rebalanced < max_rebalances:
        selection = select_donor_and_volume(placement, imbalanced, storage_nodes)
        if selection is None:
            logger.info("No more rebalanceable volumes found")
            break

        vol_name, donor_node, replica_name = selection
        logger.info(
            "Rebalance %d/%d: deleting replica %s from node %s (volume %s)",
            rebalanced + 1,
            max_rebalances,
            replica_name,
            donor_node,
            vol_name,
        )

        delete_replica(api, namespace, replica_name, dry_run=dry_run)

        healthy = wait_for_healthy(
            api,
            namespace,
            vol_name,
            timeout=cfg["rebuild_timeout"],
            poll_interval=cfg["poll_interval"],
            dry_run=dry_run,
        )
        if not healthy:
            logger.error("Volume %s did not become healthy after rebalance, aborting", vol_name)
            return 1

        rebalanced += 1

        if rebalanced < max_rebalances:
            replicas = get_replicas(api, namespace)
            placement = map_replica_placement(volumes, replicas)
            imbalanced = find_imbalanced_volumes(placement, storage_nodes)

    logger.info("Completed %d rebalance(s)", rebalanced)
    return 0


def main() -> None:
    cfg = load_config()
    logging.basicConfig(
        level=getattr(logging, cfg["log_level"]),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    sys.exit(run(cfg))


if __name__ == "__main__":
    main()
