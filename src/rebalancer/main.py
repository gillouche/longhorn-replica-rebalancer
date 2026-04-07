import json
import logging
import os
import sys
import urllib.request

from kubernetes import client, config

from rebalancer.balancer import (
    _format_size,
    count_replicas_per_node,
    find_imbalanced_volumes,
    get_volume_sizes,
    map_replica_placement,
    select_donor_and_volume,
)
from rebalancer.discovery import get_all_volumes, get_replicas, get_storage_nodes, get_volumes
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


def _is_node_ready(node: dict) -> bool:
    conditions = node.get("status", {}).get("conditions", [])
    for condition in conditions:
        if condition.get("type") == "Ready":
            return str(condition.get("status", "False")) == "True"
    return False


def _find_unhealthy_volumes(
    all_volumes: list[dict],
) -> tuple[list[str], list[str], list[str]]:
    degraded = []
    faulted = []
    rebuilding = []
    for vol in all_volumes:
        name = vol.get("metadata", {}).get("name", "unknown")
        robustness = vol.get("status", {}).get("robustness", "")
        state = vol.get("status", {}).get("state", "")
        if robustness == "faulted":
            faulted.append(name)
        elif robustness == "degraded":
            degraded.append(name)
        elif state == "attached" and robustness == "rebuilding":
            rebuilding.append(name)
    return faulted, degraded, rebuilding


def check_cluster_health(api: object, namespace: str, storage_nodes: list[dict]) -> bool:
    all_volumes = get_all_volumes(api, namespace)

    not_ready_nodes = [
        node["metadata"]["name"] for node in storage_nodes if not _is_node_ready(node)
    ]
    if not_ready_nodes:
        logger.warning(
            "Storage nodes not ready: %s. Skipping rebalance to avoid interference.",
            ", ".join(not_ready_nodes),
        )
        return False

    faulted, degraded, rebuilding = _find_unhealthy_volumes(all_volumes)

    if faulted:
        logger.warning(
            "Faulted volumes detected: %s. Skipping rebalance — cluster needs repair first.",
            ", ".join(faulted[:5]),
        )
        return False

    if degraded or rebuilding:
        logger.warning(
            "Volumes not fully healthy (degraded: %d, rebuilding: %d). "
            "Skipping rebalance to let ongoing rebuilds complete.",
            len(degraded),
            len(rebuilding),
        )
        return False

    return True


def log_cluster_summary(
    storage_nodes: list[dict],
    placement: dict[str, dict[str, list[str]]],
    imbalanced: list[str],
    volume_sizes: dict[str, int] | None = None,
) -> None:
    if volume_sizes is None:
        volume_sizes = {}
    counts = count_replicas_per_node(placement, storage_nodes)
    node_summary = ", ".join(f"{name}={count}" for name, count in sorted(counts.items()))
    logger.info("Replica distribution: %s", node_summary)
    logger.info(
        "Volumes: %d total, %d imbalanced",
        len(placement),
        len(imbalanced),
    )
    if imbalanced and volume_sizes:
        sorted_by_size = sorted(imbalanced, key=lambda v: volume_sizes.get(v, 0))
        smallest = sorted_by_size[0]
        largest = sorted_by_size[-1]
        logger.info(
            "Imbalanced volume sizes: smallest=%s (%s), largest=%s (%s)",
            smallest,
            _format_size(volume_sizes.get(smallest, 0)),
            largest,
            _format_size(volume_sizes.get(largest, 0)),
        )


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

    if not check_cluster_health(api, namespace, storage_nodes):
        return 0

    volumes = get_volumes(api, namespace)
    if not volumes:
        logger.info("No healthy attached volumes found")
        return 0

    replicas = get_replicas(api, namespace)
    placement = map_replica_placement(volumes, replicas)
    volume_sizes = get_volume_sizes(volumes)
    imbalanced = find_imbalanced_volumes(placement, storage_nodes)

    log_cluster_summary(storage_nodes, placement, imbalanced, volume_sizes)

    if not imbalanced:
        logger.info("All volumes are balanced, nothing to do")
        return 0

    logger.info(
        "Will rebalance up to %d volume(s) this run (smallest volumes first)",
        max_rebalances,
    )

    rebalanced = 0
    while rebalanced < max_rebalances:
        selection = select_donor_and_volume(placement, imbalanced, storage_nodes, volume_sizes)
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
            if not check_cluster_health(api, namespace, storage_nodes):
                logger.info(
                    "Cluster health degraded mid-run, stopping after %d rebalance(s)", rebalanced
                )
                break
            replicas = get_replicas(api, namespace)
            placement = map_replica_placement(volumes, replicas)
            imbalanced = find_imbalanced_volumes(placement, storage_nodes)
            log_cluster_summary(storage_nodes, placement, imbalanced, volume_sizes)

    logger.info("Completed %d rebalance(s)", rebalanced)
    return 0


def send_discord_notification(webhook_url: str, message: str, success: bool) -> None:
    color = 3066993 if success else 15158332
    title = "Replica Rebalancer" if success else "Replica Rebalancer FAILED"
    payload = json.dumps(
        {
            "embeds": [
                {
                    "title": title,
                    "description": message,
                    "color": color,
                }
            ]
        }
    ).encode()
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        logger.warning("Failed to send Discord notification: %s", e)


def main() -> None:
    cfg = load_config()
    logging.basicConfig(
        level=getattr(logging, cfg["log_level"]),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL", "")
    exit_code = run(cfg)
    if webhook_url and exit_code != 0:
        send_discord_notification(webhook_url, "Rebalancer failed. Check CronJob logs.", False)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
