import logging

logger = logging.getLogger(__name__)


def map_replica_placement(
    volumes: list[dict], replicas: list[dict]
) -> dict[str, dict[str, list[str]]]:
    volume_names = {v["metadata"]["name"] for v in volumes}
    placement: dict[str, dict[str, list[str]]] = {}

    for replica in replicas:
        vol_name = replica.get("spec", {}).get("volumeName", "")
        node_name = replica.get("spec", {}).get("nodeID", "")
        replica_name = replica.get("metadata", {}).get("name", "")
        current_state = replica.get("status", {}).get("currentState", "")

        if vol_name not in volume_names or current_state != "running":
            continue

        if vol_name not in placement:
            placement[vol_name] = {}
        if node_name not in placement[vol_name]:
            placement[vol_name][node_name] = []
        placement[vol_name][node_name].append(replica_name)

    return placement


def get_volume_sizes(volumes: list[dict]) -> dict[str, int]:
    sizes: dict[str, int] = {}
    for vol in volumes:
        name = vol.get("metadata", {}).get("name", "")
        size = int(vol.get("spec", {}).get("size", "0"))
        if name:
            sizes[name] = size
    return sizes


def find_imbalanced_volumes(
    placement: dict[str, dict[str, list[str]]],
    storage_nodes: list[dict],
) -> list[str]:
    node_names = {n["metadata"]["name"] for n in storage_nodes}
    node_counts = count_replicas_per_node(placement, storage_nodes)

    max_count = max(node_counts.values())
    min_count = min(node_counts.values())
    if max_count - min_count <= 1:
        logger.info("Cluster is balanced (spread=%d), nothing to rebalance", max_count - min_count)
        return []

    imbalanced = []

    for vol_name, node_replicas in placement.items():
        nodes_with_replicas = set(node_replicas.keys()) & node_names
        nodes_without = node_names - nodes_with_replicas
        if not nodes_without or len(nodes_with_replicas) < 2:
            continue
        min_count_without = min(node_counts[n] for n in nodes_without)
        max_count_with = max(node_counts[n] for n in nodes_with_replicas)
        if min_count_without < max_count_with - 1:
            imbalanced.append(vol_name)

    logger.info("Found %d imbalanced volumes", len(imbalanced))
    return imbalanced


def count_replicas_per_node(
    placement: dict[str, dict[str, list[str]]],
    storage_nodes: list[dict],
) -> dict[str, int]:
    node_names = {n["metadata"]["name"] for n in storage_nodes}
    counts: dict[str, int] = dict.fromkeys(node_names, 0)

    for node_replicas in placement.values():
        for node, replicas in node_replicas.items():
            if node in counts:
                counts[node] += len(replicas)

    return counts


def _format_size(size_bytes: int) -> str:
    if size_bytes >= 1024**3:
        return f"{size_bytes / 1024**3:.0f}GB"
    if size_bytes >= 1024**2:
        return f"{size_bytes / 1024**2:.0f}MB"
    return f"{size_bytes}B"


def _has_eligible_target(
    vol_name: str,
    donor_node: str,
    placement: dict[str, dict[str, list[str]]],
    node_names: set[str],
) -> bool:
    nodes_with_replica = set(placement.get(vol_name, {}).keys())
    nodes_without_replica = node_names - nodes_with_replica
    eligible = nodes_without_replica - {donor_node}
    return len(eligible) > 0


def select_donor_and_volume(
    placement: dict[str, dict[str, list[str]]],
    imbalanced_volumes: list[str],
    storage_nodes: list[dict],
    volume_sizes: dict[str, int] | None = None,
) -> tuple[str, str, str] | None:
    if not imbalanced_volumes:
        return None

    if volume_sizes is None:
        volume_sizes = {}

    sorted_volumes = sorted(
        imbalanced_volumes,
        key=lambda v: volume_sizes.get(v, 0),
    )

    node_names = {n["metadata"]["name"] for n in storage_nodes}
    node_counts = count_replicas_per_node(placement, storage_nodes)
    least_loaded_node = min(node_counts, key=lambda n: node_counts[n])
    donor_node = max(node_counts, key=lambda n: node_counts[n])

    for vol_name in sorted_volumes:
        node_replicas = placement.get(vol_name, {})
        if not node_replicas.get(donor_node):
            continue
        if not _has_eligible_target(vol_name, donor_node, placement, node_names):
            logger.debug(
                "Skipping volume=%s: no eligible target node after removing from %s",
                vol_name,
                donor_node,
            )
            continue
        if least_loaded_node in node_replicas:
            logger.debug(
                "Skipping volume=%s: least-loaded node %s already has a replica",
                vol_name,
                least_loaded_node,
            )
            continue
        replica_name = node_replicas[donor_node][0]
        size = volume_sizes.get(vol_name, 0)
        logger.info(
            "Selected volume=%s (%s), donor=%s (count=%d), replica=%s",
            vol_name,
            _format_size(size) if size else "unknown size",
            donor_node,
            node_counts[donor_node],
            replica_name,
        )
        return vol_name, donor_node, replica_name

    for vol_name in sorted_volumes:
        node_replicas = placement.get(vol_name, {})
        if not node_replicas:
            continue
        best_node = max(node_replicas, key=lambda n: node_counts.get(n, 0))
        if not node_replicas[best_node]:
            continue
        if not _has_eligible_target(vol_name, best_node, placement, node_names):
            continue
        if least_loaded_node in node_replicas:
            continue
        replica_name = node_replicas[best_node][0]
        size = volume_sizes.get(vol_name, 0)
        logger.info(
            "Fallback selection: volume=%s (%s), donor=%s, replica=%s",
            vol_name,
            _format_size(size) if size else "unknown size",
            best_node,
            replica_name,
        )
        return vol_name, best_node, replica_name

    return None
