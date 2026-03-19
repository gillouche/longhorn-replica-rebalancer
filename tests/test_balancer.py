from conftest import make_replica, make_volume

from rebalancer.balancer import (
    count_replicas_per_node,
    find_imbalanced_volumes,
    map_replica_placement,
    select_donor_and_volume,
)


class TestMapReplicaPlacement:
    def test_maps_running_replicas_to_volumes_and_nodes(self):
        volumes = [make_volume("vol-a"), make_volume("vol-b")]
        replicas = [
            make_replica("r1", "vol-a", "homeserver1"),
            make_replica("r2", "vol-a", "homeserver2"),
            make_replica("r3", "vol-b", "homeserver2"),
            make_replica("r4", "vol-b", "homeserver3"),
        ]

        placement = map_replica_placement(volumes, replicas)

        assert placement == {
            "vol-a": {
                "homeserver1": ["r1"],
                "homeserver2": ["r2"],
            },
            "vol-b": {
                "homeserver2": ["r3"],
                "homeserver3": ["r4"],
            },
        }

    def test_excludes_non_running_replicas(self):
        volumes = [make_volume("vol-a")]
        replicas = [
            make_replica("r1", "vol-a", "homeserver1", state="running"),
            make_replica("r2", "vol-a", "homeserver2", state="stopped"),
        ]

        placement = map_replica_placement(volumes, replicas)

        assert placement == {"vol-a": {"homeserver1": ["r1"]}}

    def test_excludes_replicas_for_unknown_volumes(self):
        volumes = [make_volume("vol-a")]
        replicas = [
            make_replica("r1", "vol-a", "homeserver1"),
            make_replica("r2", "vol-orphan", "homeserver2"),
        ]

        placement = map_replica_placement(volumes, replicas)

        assert "vol-orphan" not in placement

    def test_empty_volumes_returns_empty(self):
        placement = map_replica_placement([], [])

        assert placement == {}


class TestFindImbalancedVolumes:
    def test_balanced_cluster_returns_empty(self, three_nodes):
        placement = {
            "vol-a": {"homeserver1": ["r1"], "homeserver2": ["r2"]},
            "vol-b": {"homeserver2": ["r3"], "homeserver3": ["r4"]},
            "vol-c": {"homeserver1": ["r5"], "homeserver3": ["r6"]},
        }

        imbalanced = find_imbalanced_volumes(placement, three_nodes)

        assert imbalanced == []

    def test_detects_imbalanced_volumes(self, three_nodes):
        placement = {
            "vol-a": {"homeserver1": ["r1"], "homeserver2": ["r2"]},
            "vol-b": {"homeserver1": ["r3"], "homeserver2": ["r4"]},
        }

        imbalanced = find_imbalanced_volumes(placement, three_nodes)

        assert set(imbalanced) == {"vol-a", "vol-b"}

    def test_volume_on_single_node_not_imbalanced(self, three_nodes):
        placement = {
            "vol-a": {"homeserver1": ["r1"]},
        }

        imbalanced = find_imbalanced_volumes(placement, three_nodes)

        assert imbalanced == []

    def test_all_three_nodes_covered_not_imbalanced(self, three_nodes):
        placement = {
            "vol-a": {
                "homeserver1": ["r1"],
                "homeserver2": ["r2"],
                "homeserver3": ["r3"],
            },
        }

        imbalanced = find_imbalanced_volumes(placement, three_nodes)

        assert imbalanced == []


class TestCountReplicasPerNode:
    def test_counts_replicas_across_all_volumes(self, three_nodes):
        placement = {
            "vol-a": {"homeserver1": ["r1"], "homeserver2": ["r2"]},
            "vol-b": {"homeserver1": ["r3"], "homeserver2": ["r4"]},
        }

        counts = count_replicas_per_node(placement, three_nodes)

        assert counts == {"homeserver1": 2, "homeserver2": 2, "homeserver3": 0}

    def test_ignores_nodes_not_in_storage_list(self, three_nodes):
        placement = {
            "vol-a": {"homeserver1": ["r1"], "rpi4b8g1": ["r2"]},
        }

        counts = count_replicas_per_node(placement, three_nodes)

        assert "rpi4b8g1" not in counts
        assert counts["homeserver1"] == 1


class TestSelectDonorAndVolume:
    def test_selects_volume_from_most_loaded_node(self, three_nodes):
        placement = {
            "vol-a": {"homeserver1": ["r1"], "homeserver2": ["r2"]},
            "vol-b": {"homeserver1": ["r3"], "homeserver2": ["r4"]},
        }
        imbalanced = ["vol-a", "vol-b"]

        result = select_donor_and_volume(placement, imbalanced, three_nodes)

        assert result is not None
        vol_name, donor_node, _replica_name = result
        assert vol_name in imbalanced
        assert donor_node in ("homeserver1", "homeserver2")

    def test_returns_none_when_no_imbalanced_volumes(self, three_nodes):
        placement = {
            "vol-a": {"homeserver1": ["r1"], "homeserver2": ["r2"]},
        }

        result = select_donor_and_volume(placement, [], three_nodes)

        assert result is None

    def test_prefers_donor_with_most_total_replicas(self, three_nodes):
        placement = {
            "vol-a": {"homeserver1": ["r1"], "homeserver2": ["r2"]},
            "vol-b": {"homeserver1": ["r3"], "homeserver2": ["r4"]},
            "vol-c": {"homeserver1": ["r5"], "homeserver3": ["r6"]},
        }
        imbalanced = ["vol-b"]

        result = select_donor_and_volume(placement, imbalanced, three_nodes)

        assert result is not None
        _, donor_node, _ = result
        assert donor_node == "homeserver1"
