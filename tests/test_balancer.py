import pytest
from conftest import make_nodes, make_replica, make_volume

from rebalancer.balancer import (
    _format_size,
    count_replicas_per_node,
    find_imbalanced_volumes,
    get_volume_sizes,
    map_replica_placement,
    select_donor_and_volume,
)


class TestMapReplicaPlacement:
    def test_maps_running_replicas_to_volumes_and_nodes(self):
        volumes = [make_volume("vol-a"), make_volume("vol-b")]
        replicas = [
            make_replica("r1", "vol-a", "node-0"),
            make_replica("r2", "vol-a", "node-1"),
            make_replica("r3", "vol-b", "node-1"),
            make_replica("r4", "vol-b", "node-2"),
        ]

        placement = map_replica_placement(volumes, replicas)

        assert placement == {
            "vol-a": {"node-0": ["r1"], "node-1": ["r2"]},
            "vol-b": {"node-1": ["r3"], "node-2": ["r4"]},
        }

    def test_excludes_non_running_replicas(self):
        volumes = [make_volume("vol-a")]
        replicas = [
            make_replica("r1", "vol-a", "node-0", state="running"),
            make_replica("r2", "vol-a", "node-1", state="stopped"),
        ]

        placement = map_replica_placement(volumes, replicas)

        assert placement == {"vol-a": {"node-0": ["r1"]}}

    def test_excludes_replicas_for_unknown_volumes(self):
        volumes = [make_volume("vol-a")]
        replicas = [
            make_replica("r1", "vol-a", "node-0"),
            make_replica("r2", "vol-orphan", "node-1"),
        ]

        placement = map_replica_placement(volumes, replicas)

        assert "vol-orphan" not in placement

    def test_empty_volumes_returns_empty(self):
        placement = map_replica_placement([], [])

        assert placement == {}


class TestFindImbalancedVolumes:
    def test_balanced_cluster_returns_empty(self, three_nodes):
        placement = {
            "vol-a": {"node-0": ["r1"], "node-1": ["r2"]},
            "vol-b": {"node-1": ["r3"], "node-2": ["r4"]},
            "vol-c": {"node-0": ["r5"], "node-2": ["r6"]},
        }

        imbalanced = find_imbalanced_volumes(placement, three_nodes)

        assert imbalanced == []

    def test_detects_imbalanced_volumes(self, three_nodes):
        placement = {
            "vol-a": {"node-0": ["r1"], "node-1": ["r2"]},
            "vol-b": {"node-0": ["r3"], "node-1": ["r4"]},
        }

        imbalanced = find_imbalanced_volumes(placement, three_nodes)

        assert set(imbalanced) == {"vol-a", "vol-b"}

    def test_volume_on_single_node_not_imbalanced(self, three_nodes):
        placement = {"vol-a": {"node-0": ["r1"]}}

        imbalanced = find_imbalanced_volumes(placement, three_nodes)

        assert imbalanced == []

    def test_all_nodes_covered_not_imbalanced(self, three_nodes):
        placement = {
            "vol-a": {"node-0": ["r1"], "node-1": ["r2"], "node-2": ["r3"]},
        }

        imbalanced = find_imbalanced_volumes(placement, three_nodes)

        assert imbalanced == []

    def test_works_with_five_nodes(self, five_nodes):
        placement = {
            "vol-a": {"node-0": ["r1"], "node-1": ["r2"]},
            "vol-b": {"node-0": ["r3"], "node-1": ["r4"]},
        }

        imbalanced = find_imbalanced_volumes(placement, five_nodes)

        assert set(imbalanced) == {"vol-a", "vol-b"}

    def test_spread_of_1_is_balanced(self, three_nodes):
        placement = {
            "vol-a": {"node-0": ["r1"], "node-1": ["r2"]},
            "vol-b": {"node-0": ["r3"], "node-2": ["r4"]},
            "vol-c": {"node-1": ["r5"], "node-2": ["r6"]},
        }

        imbalanced = find_imbalanced_volumes(placement, three_nodes)

        assert imbalanced == []

    def test_11_12_11_is_balanced(self, three_nodes):
        placement = {}
        for i in range(6):
            placement[f"vol-{i}a"] = {"node-0": [f"r{i}a1"], "node-1": [f"r{i}a2"]}
        for i in range(5):
            placement[f"vol-{i}b"] = {"node-1": [f"r{i}b1"], "node-2": [f"r{i}b2"]}
        for i in range(5):
            placement[f"vol-{i}c"] = {"node-0": [f"r{i}c1"], "node-2": [f"r{i}c2"]}

        counts = count_replicas_per_node(placement, three_nodes)
        assert counts == {"node-0": 11, "node-1": 11, "node-2": 10}

        imbalanced = find_imbalanced_volumes(placement, three_nodes)

        assert imbalanced == []

    def test_10_12_10_is_imbalanced(self, three_nodes):
        placement = {}
        for i in range(6):
            placement[f"vol-{i}a"] = {"node-0": [f"r{i}a1"], "node-1": [f"r{i}a2"]}
        for i in range(6):
            placement[f"vol-{i}b"] = {"node-1": [f"r{i}b1"], "node-2": [f"r{i}b2"]}
        for i in range(4):
            placement[f"vol-{i}c"] = {"node-0": [f"r{i}c1"], "node-2": [f"r{i}c2"]}

        counts = count_replicas_per_node(placement, three_nodes)
        assert counts == {"node-0": 10, "node-1": 12, "node-2": 10}

        imbalanced = find_imbalanced_volumes(placement, three_nodes)

        assert len(imbalanced) > 0

    def test_5_5_0_is_imbalanced(self, three_nodes):
        placement = {}
        for i in range(5):
            placement[f"vol-{i}"] = {"node-0": [f"r{i}1"], "node-1": [f"r{i}2"]}

        imbalanced = find_imbalanced_volumes(placement, three_nodes)

        assert len(imbalanced) == 5

    def test_3_3_2_is_balanced(self, three_nodes):
        placement = {
            "vol-a": {"node-0": ["r1"], "node-1": ["r2"]},
            "vol-b": {"node-0": ["r3"], "node-2": ["r4"]},
            "vol-c": {"node-1": ["r5"], "node-2": ["r6"]},
            "vol-d": {"node-0": ["r7"], "node-1": ["r8"]},
        }

        counts = count_replicas_per_node(placement, three_nodes)
        assert counts == {"node-0": 3, "node-1": 3, "node-2": 2}

        imbalanced = find_imbalanced_volumes(placement, three_nodes)

        assert imbalanced == []

    def test_4_4_0_is_imbalanced(self, three_nodes):
        placement = {}
        for i in range(4):
            placement[f"vol-{i}"] = {"node-0": [f"r{i}1"], "node-1": [f"r{i}2"]}

        counts = count_replicas_per_node(placement, three_nodes)
        assert counts == {"node-0": 4, "node-1": 4, "node-2": 0}

        imbalanced = find_imbalanced_volumes(placement, three_nodes)

        assert len(imbalanced) == 4

    @pytest.mark.parametrize("node_count", [3, 5])
    def test_perfectly_balanced_returns_empty(self, node_count):
        nodes = make_nodes(node_count)
        placement = {}
        node_pairs = [
            (f"node-{i}", f"node-{j}") for i in range(node_count) for j in range(i + 1, node_count)
        ]
        for idx, pair in enumerate(node_pairs):
            placement[f"vol-{idx}"] = {pair[0]: [f"r{idx}1"], pair[1]: [f"r{idx}2"]}

        imbalanced = find_imbalanced_volumes(placement, nodes)

        assert imbalanced == []


class TestCountReplicasPerNode:
    def test_counts_replicas_across_all_volumes(self, three_nodes):
        placement = {
            "vol-a": {"node-0": ["r1"], "node-1": ["r2"]},
            "vol-b": {"node-0": ["r3"], "node-1": ["r4"]},
        }

        counts = count_replicas_per_node(placement, three_nodes)

        assert counts == {"node-0": 2, "node-1": 2, "node-2": 0}

    def test_ignores_nodes_not_in_storage_list(self, three_nodes):
        placement = {"vol-a": {"node-0": ["r1"], "rpi-worker": ["r2"]}}

        counts = count_replicas_per_node(placement, three_nodes)

        assert "rpi-worker" not in counts
        assert counts["node-0"] == 1

    def test_works_with_five_nodes(self, five_nodes):
        placement = {
            "vol-a": {"node-0": ["r1"], "node-1": ["r2"]},
        }

        counts = count_replicas_per_node(placement, five_nodes)

        assert counts["node-0"] == 1
        assert counts["node-1"] == 1
        assert counts["node-2"] == 0
        assert counts["node-3"] == 0
        assert counts["node-4"] == 0


class TestSelectDonorAndVolume:
    def test_selects_volume_from_most_loaded_node(self, three_nodes):
        placement = {
            "vol-a": {"node-0": ["r1"], "node-1": ["r2"]},
            "vol-b": {"node-0": ["r3"], "node-1": ["r4"]},
        }
        imbalanced = ["vol-a", "vol-b"]

        result = select_donor_and_volume(placement, imbalanced, three_nodes)

        assert result is not None
        vol_name, donor_node, _ = result
        assert vol_name in imbalanced
        assert donor_node in ("node-0", "node-1")

    def test_skips_volume_with_no_eligible_target_node(self, three_nodes):
        placement = {
            "vol-stuck": {"node-0": ["r1"], "node-1": ["r2"], "node-2": ["r3"]},
            "vol-movable": {"node-0": ["r4"], "node-2": ["r5"]},
        }
        imbalanced = ["vol-stuck", "vol-movable"]

        result = select_donor_and_volume(placement, imbalanced, three_nodes)

        assert result is not None
        vol_name, _, _ = result
        assert vol_name == "vol-movable"

    def test_returns_none_when_no_volumes_have_eligible_targets(self, three_nodes):
        placement = {
            "vol-a": {"node-0": ["r1"], "node-1": ["r2"], "node-2": ["r3"]},
        }
        imbalanced = ["vol-a"]

        result = select_donor_and_volume(placement, imbalanced, three_nodes)

        assert result is None

    def test_returns_none_when_no_imbalanced_volumes(self, three_nodes):
        placement = {"vol-a": {"node-0": ["r1"], "node-1": ["r2"]}}

        result = select_donor_and_volume(placement, [], three_nodes)

        assert result is None

    def test_prefers_donor_with_most_total_replicas(self, three_nodes):
        placement = {
            "vol-a": {"node-0": ["r1"], "node-1": ["r2"]},
            "vol-b": {"node-0": ["r3"], "node-1": ["r4"]},
            "vol-c": {"node-0": ["r5"], "node-2": ["r6"]},
        }
        imbalanced = ["vol-b"]

        result = select_donor_and_volume(placement, imbalanced, three_nodes)

        assert result is not None
        _, donor_node, _ = result
        assert donor_node == "node-0"

    def test_fallback_when_donor_has_no_replica_for_imbalanced_volume(self, three_nodes):
        placement = {
            "vol-a": {"node-0": ["r1"], "node-1": ["r2"]},
            "vol-b": {"node-2": ["r3", "r4"]},
        }
        imbalanced = ["vol-b"]

        result = select_donor_and_volume(placement, imbalanced, three_nodes)

        assert result is not None
        vol_name, _, _ = result
        assert vol_name == "vol-b"

    def test_returns_none_when_all_imbalanced_volumes_have_empty_placement(self, three_nodes):
        placement = {"vol-a": {"node-0": ["r1"], "node-1": ["r2"]}}
        imbalanced = ["vol-missing"]

        result = select_donor_and_volume(placement, imbalanced, three_nodes)

        assert result is None

    def test_works_with_five_nodes(self, five_nodes):
        placement = {
            "vol-a": {"node-0": ["r1"], "node-1": ["r2"]},
            "vol-b": {"node-0": ["r3"], "node-1": ["r4"]},
        }
        imbalanced = ["vol-a", "vol-b"]

        result = select_donor_and_volume(placement, imbalanced, five_nodes)

        assert result is not None


class TestRealisticScenario:
    @pytest.mark.parametrize("node_count", [3, 5])
    def test_all_on_two_nodes_detected_as_imbalanced(self, node_count):
        nodes = make_nodes(node_count)
        placement = {}
        for i in range(16):
            placement[f"pvc-{i}"] = {
                "node-0": [f"pvc-{i}-r1"],
                "node-1": [f"pvc-{i}-r2"],
            }

        imbalanced = find_imbalanced_volumes(placement, nodes)

        assert len(imbalanced) == 16

    @pytest.mark.parametrize("node_count", [3, 5])
    def test_empty_node_has_zero_replicas(self, node_count):
        nodes = make_nodes(node_count)
        placement = {}
        for i in range(16):
            placement[f"pvc-{i}"] = {
                "node-0": [f"pvc-{i}-r1"],
                "node-1": [f"pvc-{i}-r2"],
            }

        counts = count_replicas_per_node(placement, nodes)

        assert counts["node-0"] == 16
        assert counts["node-1"] == 16
        for n in range(2, node_count):
            assert counts[f"node-{n}"] == 0

    @pytest.mark.parametrize("node_count", [3, 5])
    def test_donor_selection_picks_from_loaded_node(self, node_count):
        nodes = make_nodes(node_count)
        placement = {}
        for i in range(16):
            placement[f"pvc-{i}"] = {
                "node-0": [f"pvc-{i}-r1"],
                "node-1": [f"pvc-{i}-r2"],
            }
        imbalanced = list(placement.keys())

        result = select_donor_and_volume(placement, imbalanced, nodes)

        assert result is not None
        _, donor_node, _ = result
        assert donor_node in ("node-0", "node-1")

    def test_after_one_rebalance_counts_improve(self, three_nodes):
        placement = {}
        for i in range(16):
            placement[f"pvc-{i}"] = {
                "node-0": [f"pvc-{i}-r1"],
                "node-1": [f"pvc-{i}-r2"],
            }

        result = select_donor_and_volume(placement, list(placement.keys()), three_nodes)
        assert result is not None
        vol_name, donor_node, replica_name = result

        placement[vol_name][donor_node].remove(replica_name)
        if not placement[vol_name][donor_node]:
            del placement[vol_name][donor_node]
        placement[vol_name]["node-2"] = [f"{vol_name}-r-new"]

        counts = count_replicas_per_node(placement, three_nodes)
        assert counts["node-2"] == 1
        assert counts[donor_node] == 15

    def test_15_1_16_skips_volume_already_on_least_loaded(self, three_nodes):
        placement = {}
        sizes = {}
        placement["pvc-already-moved"] = {
            "node-1": ["moved-r1"],
            "node-2": ["moved-r2"],
        }
        sizes["pvc-already-moved"] = 5368709120
        for i in range(15):
            placement[f"pvc-{i}"] = {
                "node-0": [f"pvc-{i}-r1"],
                "node-2": [f"pvc-{i}-r2"],
            }
            sizes[f"pvc-{i}"] = 5368709120 * (i + 2)

        imbalanced = list(placement.keys())

        result = select_donor_and_volume(placement, imbalanced, three_nodes, sizes)

        assert result is not None
        vol_name, donor_node, _ = result
        assert vol_name != "pvc-already-moved"
        assert donor_node == "node-2"

    def test_even_distribution_is_balanced(self, three_nodes):
        placement = {}
        for i in range(6):
            if i < 2:
                placement[f"pvc-{i}"] = {
                    "node-0": [f"pvc-{i}-r1"],
                    "node-2": [f"pvc-{i}-r2"],
                }
            elif i < 4:
                placement[f"pvc-{i}"] = {
                    "node-1": [f"pvc-{i}-r1"],
                    "node-2": [f"pvc-{i}-r2"],
                }
            else:
                placement[f"pvc-{i}"] = {
                    "node-0": [f"pvc-{i}-r1"],
                    "node-1": [f"pvc-{i}-r2"],
                }

        imbalanced = find_imbalanced_volumes(placement, three_nodes)

        assert len(imbalanced) == 0

    def test_heavily_skewed_all_detected_as_imbalanced(self, three_nodes):
        placement = {}
        for i in range(8):
            placement[f"pvc-{i}"] = {
                "node-0": [f"pvc-{i}-r1"],
                "node-1": [f"pvc-{i}-r2"],
            }

        imbalanced = find_imbalanced_volumes(placement, three_nodes)

        assert len(imbalanced) == 8


class TestEdgeCases:
    def test_stopped_replica_excluded_from_placement(self):
        volumes = [make_volume("vol-a")]
        replicas = [
            make_replica("r1", "vol-a", "node-0", state="running"),
            make_replica("r2", "vol-a", "node-2", state="running"),
            make_replica("r3", "vol-a", "node-1", state="stopped"),
        ]

        placement = map_replica_placement(volumes, replicas)

        assert "node-1" not in placement["vol-a"]
        assert set(placement["vol-a"].keys()) == {"node-0", "node-2"}

    def test_volume_with_3_running_replicas_on_all_nodes_not_imbalanced(self, three_nodes):
        placement = {
            "vol-3rep": {"node-0": ["r1"], "node-1": ["r2"], "node-2": ["r3"]},
            "vol-normal": {"node-0": ["r4"], "node-2": ["r5"]},
        }

        imbalanced = find_imbalanced_volumes(placement, three_nodes)

        assert "vol-3rep" not in imbalanced
        assert len(imbalanced) == 0

    def test_volume_with_3_running_replicas_skipped_by_donor_selection(self, three_nodes):
        placement = {
            "vol-3rep": {"node-0": ["r1"], "node-1": ["r2"], "node-2": ["r3"]},
            "vol-movable": {"node-0": ["r4"], "node-2": ["r5"]},
        }
        imbalanced = ["vol-3rep", "vol-movable"]

        result = select_donor_and_volume(placement, imbalanced, three_nodes)

        assert result is not None
        vol_name, _, _ = result
        assert vol_name == "vol-movable"

    def test_16_1_16_with_one_3replica_volume(self, three_nodes):
        placement = {}
        sizes = {}
        placement["pvc-3rep"] = {
            "node-0": ["3rep-r1"],
            "node-1": ["3rep-r2"],
            "node-2": ["3rep-r3"],
        }
        sizes["pvc-3rep"] = 5368709120
        for i in range(15):
            placement[f"pvc-{i}"] = {
                "node-0": [f"pvc-{i}-r1"],
                "node-2": [f"pvc-{i}-r2"],
            }
            sizes[f"pvc-{i}"] = 5368709120 * (i + 2)

        imbalanced = find_imbalanced_volumes(placement, three_nodes)

        assert "pvc-3rep" not in imbalanced
        assert len(imbalanced) == 15

    def test_failed_replica_not_counted_in_node_totals(self, three_nodes):
        volumes = [make_volume("vol-a"), make_volume("vol-b")]
        replicas = [
            make_replica("r1", "vol-a", "node-0", state="running"),
            make_replica("r2", "vol-a", "node-2", state="running"),
            make_replica("r3", "vol-a", "node-1", state="stopped"),
            make_replica("r4", "vol-b", "node-0", state="running"),
            make_replica("r5", "vol-b", "node-2", state="running"),
        ]

        placement = map_replica_placement(volumes, replicas)
        counts = count_replicas_per_node(placement, three_nodes)

        assert counts["node-0"] == 2
        assert counts["node-1"] == 0
        assert counts["node-2"] == 2


class TestGetVolumeSizes:
    def test_extracts_sizes_from_volumes(self):
        volumes = [
            make_volume("vol-small"),
            make_volume("vol-large"),
        ]
        volumes[0]["spec"]["size"] = "5368709120"
        volumes[1]["spec"]["size"] = "214748364800"

        sizes = get_volume_sizes(volumes)

        assert sizes["vol-small"] == 5368709120
        assert sizes["vol-large"] == 214748364800

    def test_defaults_to_zero_when_missing(self):
        volumes = [make_volume("vol-a")]

        sizes = get_volume_sizes(volumes)

        assert sizes["vol-a"] == 0


class TestFormatSize:
    def test_formats_gigabytes(self):
        assert _format_size(5368709120) == "5GB"
        assert _format_size(214748364800) == "200GB"

    def test_formats_megabytes(self):
        assert _format_size(104857600) == "100MB"

    def test_formats_bytes(self):
        assert _format_size(1024) == "1024B"


class TestSizeBasedSelection:
    def test_selects_smallest_volume_first(self, three_nodes):
        placement = {
            "vol-large": {"node-0": ["r1"], "node-1": ["r2"]},
            "vol-small": {"node-0": ["r3"], "node-1": ["r4"]},
            "vol-medium": {"node-0": ["r5"], "node-1": ["r6"]},
        }
        imbalanced = ["vol-large", "vol-small", "vol-medium"]
        volume_sizes = {
            "vol-large": 214748364800,
            "vol-small": 5368709120,
            "vol-medium": 53687091200,
        }

        result = select_donor_and_volume(placement, imbalanced, three_nodes, volume_sizes)

        assert result is not None
        vol_name, _, _ = result
        assert vol_name == "vol-small"

    def test_selects_smallest_among_donor_volumes(self, three_nodes):
        placement = {
            "vol-200g": {"node-0": ["r1"], "node-1": ["r2"]},
            "vol-5g": {"node-0": ["r3"], "node-1": ["r4"]},
            "vol-50g": {"node-0": ["r5"], "node-1": ["r6"]},
            "vol-100g": {"node-0": ["r7"], "node-1": ["r8"]},
        }
        imbalanced = ["vol-200g", "vol-5g", "vol-50g", "vol-100g"]
        volume_sizes = {
            "vol-200g": 214748364800,
            "vol-5g": 5368709120,
            "vol-50g": 53687091200,
            "vol-100g": 107374182400,
        }

        result = select_donor_and_volume(placement, imbalanced, three_nodes, volume_sizes)

        assert result is not None
        vol_name, _, _ = result
        assert vol_name == "vol-5g"

    def test_works_without_volume_sizes(self, three_nodes):
        placement = {
            "vol-a": {"node-0": ["r1"], "node-1": ["r2"]},
            "vol-b": {"node-0": ["r3"], "node-1": ["r4"]},
        }
        imbalanced = ["vol-a", "vol-b"]

        result = select_donor_and_volume(placement, imbalanced, three_nodes)

        assert result is not None

    def test_realistic_mixed_sizes(self, three_nodes):
        placement = {}
        sizes = {}
        vol_configs = [
            ("pvc-prom-0", 214748364800),
            ("pvc-prom-1", 214748364800),
            ("pvc-loki", 107374182400),
            ("pvc-grafana", 5368709120),
            ("pvc-authelia", 5368709120),
            ("pvc-sonarqube", 53687091200),
        ]
        for name, size in vol_configs:
            placement[name] = {"node-0": [f"{name}-r1"], "node-1": [f"{name}-r2"]}
            sizes[name] = size

        imbalanced = list(placement.keys())

        result = select_donor_and_volume(placement, imbalanced, three_nodes, sizes)

        assert result is not None
        vol_name, _, _ = result
        assert sizes[vol_name] == 5368709120
