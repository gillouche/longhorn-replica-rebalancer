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


def find_imbalanced_volumes(
    placement: dict[str, dict[str, list[str]]],
    storage_nodes: list[dict],
) -> list[str]:
    node_names = {n["metadata"]["name"] for n in storage_nodes}
    node_counts = count_replicas_per_node(placement, storage_nodes)
    imbalanced = []

    for vol_name, node_replicas in placement.items():
        nodes_with_replicas = set(node_replicas.keys()) & node_names
        nodes_without = node_names - nodes_with_replicas
        if not nodes_without or len(nodes_with_replicas) < 2:
            continue
        min_count_without = min(node_counts[n] for n in nodes_without)
        max_count_with = max(node_counts[n] for n in nodes_with_replicas)
        if min_count_without < max_count_with:
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


def select_donor_and_volume(
    placement: dict[str, dict[str, list[str]]],
    imbalanced_volumes: list[str],
    storage_nodes: list[dict],
) -> tuple[str, str, str] | None:
    if not imbalanced_volumes:
        return None

    node_counts = count_replicas_per_node(placement, storage_nodes)
    donor_node = max(node_counts, key=lambda n: node_counts[n])

    for vol_name in imbalanced_volumes:
        node_replicas = placement.get(vol_name, {})
        if node_replicas.get(donor_node):
            replica_name = node_replicas[donor_node][0]
            logger.info(
                "Selected volume=%s, donor=%s (count=%d), replica=%s",
                vol_name,
                donor_node,
                node_counts[donor_node],
                replica_name,
            )
            return vol_name, donor_node, replica_name

    for vol_name in imbalanced_volumes:
        node_replicas = placement.get(vol_name, {})
        if node_replicas:
            best_node = max(node_replicas, key=lambda n: node_counts.get(n, 0))
            if node_replicas[best_node]:
                replica_name = node_replicas[best_node][0]
                logger.info(
                    "Fallback selection: volume=%s, donor=%s, replica=%s",
                    vol_name,
                    best_node,
                    replica_name,
                )
                return vol_name, best_node, replica_name

    return None
