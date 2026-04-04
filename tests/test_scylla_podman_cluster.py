"""Integration tests for ScyllaPodmanCluster with multi-DC network topology.

These tests require:
- podman installed and working (rootless)
- SCYLLA_PODMAN_IMAGE env var set to a valid ScyllaDB container image
  (defaults to SCYLLA_DOCKER_IMAGE)

Run with: pytest tests/test_scylla_podman_cluster.py -v -m network_topology
"""

import re
import os
import subprocess
import tempfile
import pytest
from collections import OrderedDict
from types import SimpleNamespace
from unittest.mock import patch

from ruamel.yaml import YAML

from ccmlib.scylla_podman_cluster import PodmanNetworkTopology
from ccmlib.scylla_podman_cluster import PodmanProcess
from ccmlib import common


# ============================================================================
# Unit tests for PodmanNetworkTopology (no podman required)
# ============================================================================


class TestPodmanNetworkTopology:
    """Unit tests for the topology/IP allocation logic."""

    @pytest.fixture
    def multi_dc_topology(self):
        """DC1: 2 racks (2+2 nodes), DC2: 1 rack (1 node)"""
        topology = OrderedDict(
            [
                ("DC1", OrderedDict([("RAC1", 2), ("RAC2", 2)])),
                ("DC2", OrderedDict([("RAC1", 1)])),
            ]
        )
        return PodmanNetworkTopology(
            cluster_name="test-cluster",
            topology=topology,
            inter_rack_delay_ms=1,
            inter_dc_delay_ms=50,
            packet_loss_percent=0.5,
        )

    def test_node_ip_assignment(self, multi_dc_topology):
        topo = multi_dc_topology
        # DC1/RAC1 => rack_idx=1
        assert topo.get_node_ip("node1") == "10.89.1.1"
        assert topo.get_node_ip("node2") == "10.89.1.2"
        # DC1/RAC2 => rack_idx=2
        assert topo.get_node_ip("node3") == "10.89.2.1"
        assert topo.get_node_ip("node4") == "10.89.2.2"
        # DC2/RAC1 => rack_idx=3
        assert topo.get_node_ip("node5") == "10.89.3.1"

    def test_network_naming(self, multi_dc_topology):
        topo = multi_dc_topology
        assert topo.get_node_network("node1") == "ccm-test-cluster-dc1-rac1"
        assert topo.get_node_network("node3") == "ccm-test-cluster-dc1-rac2"
        assert topo.get_node_network("node5") == "ccm-test-cluster-dc2-rac1"

    def test_network_naming_sanitization(self):
        """Names with special characters should be sanitized to alphanumeric + hyphens."""
        topology = OrderedDict([("DC_1.us east", OrderedDict([("RAC 1!", 1)]))])
        topo = PodmanNetworkTopology(
            cluster_name="my.cluster_v2",
            topology=topology,
        )
        net = topo.get_node_network("node1")
        assert net == "ccm-my-cluster-v2-dc-1-us-east-rac-1"

    def test_custom_subnet_prefix(self):
        topology = OrderedDict([("DC1", OrderedDict([("RAC1", 2)]))])
        topo = PodmanNetworkTopology(
            cluster_name="test-cluster",
            topology=topology,
            subnet_prefix="10.123",
        )
        assert topo.get_node_ip("node1") == "10.123.1.1"
        assert topo.get_all_rack_subnets() == ["10.123.1.0/24"]
        assert topo.get_client_ip() == "10.123.1.100"

    def test_rack_subnets(self, multi_dc_topology):
        topo = multi_dc_topology
        subnets = topo.get_all_rack_subnets()
        assert subnets == [
            "10.89.1.0/24",
            "10.89.2.0/24",
            "10.89.3.0/24",
        ]

    def test_foreign_subnets_dc1_rac1(self, multi_dc_topology):
        topo = multi_dc_topology
        foreign = topo.get_foreign_subnets("node1")
        # node1 is in DC1/RAC1 (10.89.1.0/24)
        # Inter-rack same DC: DC1/RAC2 = 10.89.2.0/24
        assert foreign["inter_rack"] == ["10.89.2.0/24"]
        # Inter-DC: DC2/RAC1 = 10.89.3.0/24
        assert foreign["inter_dc"] == ["10.89.3.0/24"]

    def test_foreign_subnets_dc2(self, multi_dc_topology):
        topo = multi_dc_topology
        foreign = topo.get_foreign_subnets("node5")
        # node5 is in DC2/RAC1 (10.89.3.0/24)
        assert foreign["inter_rack"] == []
        assert set(foreign["inter_dc"]) == {"10.89.1.0/24", "10.89.2.0/24"}

    def test_routes_for_node(self, multi_dc_topology):
        topo = multi_dc_topology
        routes = topo.get_routes_for_node("node1")
        # node1 is in DC1/RAC1, gateway = 10.89.1.254
        # Should have routes to 10.89.2.0/24 and 10.89.3.0/24
        assert len(routes) == 2
        destinations = {r[0] for r in routes}
        assert destinations == {"10.89.2.0/24", "10.89.3.0/24"}
        # All go through the same gateway
        assert all(r[1] == "10.89.1.254" for r in routes)

    def test_tc_commands_structure(self, multi_dc_topology):
        topo = multi_dc_topology
        cmds = topo.build_tc_commands("node1")
        # Should have: root qdisc, inter-rack netem, inter-rack filter,
        #              inter-dc netem, inter-dc filter
        assert any("prio bands 4" in c for c in cmds), "Missing root prio qdisc"
        assert any("netem delay 1ms" in c for c in cmds), "Missing inter-rack delay"
        assert any("netem delay 50ms" in c for c in cmds), "Missing inter-DC delay"
        assert any("loss 0.5%" in c for c in cmds), "Missing packet loss"
        # Filter for inter-rack subnet
        assert any("10.89.2.0/24" in c and "flowid 1:2" in c for c in cmds)
        # Filter for inter-DC subnet
        assert any("10.89.3.0/24" in c and "flowid 1:3" in c for c in cmds)

    def test_tc_commands_no_delay(self):
        """When delays are 0, no netem qdiscs should be created."""
        topology = OrderedDict(
            [
                ("DC1", OrderedDict([("RAC1", 1), ("RAC2", 1)])),
            ]
        )
        topo = PodmanNetworkTopology(
            cluster_name="test",
            topology=topology,
            inter_rack_delay_ms=0,
            inter_dc_delay_ms=0,
        )
        cmds = topo.build_tc_commands("node1")
        # Only the root qdisc should be present
        assert len(cmds) == 1
        assert "prio bands 4" in cmds[0]

    def test_client_ip(self, multi_dc_topology):
        topo = multi_dc_topology
        assert topo.get_client_ip() == "10.89.1.100"

    def test_client_network(self, multi_dc_topology):
        topo = multi_dc_topology
        assert topo.get_client_network() == "ccm-test-cluster-dc1-rac1"

    def test_serialization_roundtrip(self, multi_dc_topology):
        topo = multi_dc_topology
        data = topo.to_dict()
        restored = PodmanNetworkTopology.from_dict("test-cluster", data)
        assert restored.get_node_ip("node1") == topo.get_node_ip("node1")
        assert restored.get_node_ip("node5") == topo.get_node_ip("node5")
        assert restored.inter_rack_delay_ms == topo.inter_rack_delay_ms
        assert restored.inter_dc_delay_ms == topo.inter_dc_delay_ms
        assert restored.packet_loss_percent == topo.packet_loss_percent
        assert restored.get_all_rack_subnets() == topo.get_all_rack_subnets()
        assert restored.subnet_prefix == topo.subnet_prefix

    def test_single_dc_topology(self):
        """Single DC with 1 rack collapses correctly."""
        topology = OrderedDict(
            [
                ("dc1", OrderedDict([("RAC1", 3)])),
            ]
        )
        topo = PodmanNetworkTopology(
            cluster_name="single",
            topology=topology,
        )
        assert topo.get_node_ip("node1") == "10.89.1.1"
        assert topo.get_node_ip("node3") == "10.89.1.3"
        assert len(topo.get_all_rack_subnets()) == 1
        # No foreign subnets
        foreign = topo.get_foreign_subnets("node1")
        assert foreign["inter_rack"] == []
        assert foreign["inter_dc"] == []

    def test_parse_topology_int(self):
        """Integer nodes argument."""
        from ccmlib.scylla_podman_cluster import ScyllaPodmanCluster

        result = ScyllaPodmanCluster._parse_topology(None, 3)
        assert result == OrderedDict([("dc1", OrderedDict([("RAC1", 3)]))])

    def test_parse_topology_list(self):
        """List nodes argument (multi-DC, 1 rack each)."""
        from ccmlib.scylla_podman_cluster import ScyllaPodmanCluster

        result = ScyllaPodmanCluster._parse_topology(None, [2, 3])
        assert result == OrderedDict(
            [
                ("dc1", OrderedDict([("RAC1", 2)])),
                ("dc2", OrderedDict([("RAC1", 3)])),
            ]
        )

    def test_parse_topology_dict_with_list(self):
        """Dict with list values (multi-rack)."""
        from ccmlib.scylla_podman_cluster import ScyllaPodmanCluster

        result = ScyllaPodmanCluster._parse_topology(None, {"DC1": [2, 2], "DC2": [1]})
        assert result == OrderedDict(
            [
                ("DC1", OrderedDict([("RAC1", 2), ("RAC2", 2)])),
                ("DC2", OrderedDict([("RAC1", 1)])),
            ]
        )

    def test_validation_too_many_nodes_in_first_rack(self):
        """First rack with >= 100 nodes should fail (client container IP collision)."""
        topology = OrderedDict([("dc1", OrderedDict([("RAC1", 100)]))])
        with pytest.raises(ValueError, match="client container"):
            PodmanNetworkTopology(cluster_name="test", topology=topology)

    def test_validation_too_many_nodes_gateway_collision(self):
        """Rack with >= 254 nodes should fail (gateway IP collision)."""
        topology = OrderedDict(
            [
                ("dc1", OrderedDict([("RAC1", 1)])),
                ("dc2", OrderedDict([("RAC1", 254)])),
            ]
        )
        with pytest.raises(ValueError, match="gateway"):
            PodmanNetworkTopology(cluster_name="test", topology=topology)

    def test_validation_too_many_racks(self):
        """More than 255 racks should fail (subnet exhaustion)."""
        racks = OrderedDict([(f"RAC{i}", 1) for i in range(1, 257)])
        topology = OrderedDict([("dc1", racks)])
        with pytest.raises(ValueError, match="Too many racks"):
            PodmanNetworkTopology(cluster_name="test", topology=topology)

    def test_2dc_3az_topology(self):
        """2 DCs x 3 AZs, 1 node per AZ, 40ms inter-DC, 1ms inter-AZ — matches docs example."""
        topology = OrderedDict(
            [
                (
                    "DC1",
                    OrderedDict([("AZ1", 1), ("AZ2", 1), ("AZ3", 1)]),
                ),
                (
                    "DC2",
                    OrderedDict([("AZ1", 1), ("AZ2", 1), ("AZ3", 1)]),
                ),
            ]
        )
        topo = PodmanNetworkTopology(
            cluster_name="mycluster",
            topology=topology,
            inter_rack_delay_ms=1,
            inter_dc_delay_ms=40,
            packet_loss_percent=0.0,
        )

        # 6 nodes, 6 rack subnets
        assert len(topo.node_assignments) == 6
        assert len(topo.get_all_rack_subnets()) == 6

        # IP assignments match docs
        assert topo.get_node_ip("node1") == "10.89.1.1"  # DC1/AZ1
        assert topo.get_node_ip("node2") == "10.89.2.1"  # DC1/AZ2
        assert topo.get_node_ip("node3") == "10.89.3.1"  # DC1/AZ3
        assert topo.get_node_ip("node4") == "10.89.4.1"  # DC2/AZ1
        assert topo.get_node_ip("node5") == "10.89.5.1"  # DC2/AZ2
        assert topo.get_node_ip("node6") == "10.89.6.1"  # DC2/AZ3

        # Network names match docs
        assert topo.get_node_network("node1") == "ccm-mycluster-dc1-az1"
        assert topo.get_node_network("node4") == "ccm-mycluster-dc2-az1"

        # Client on first rack
        assert topo.get_client_ip() == "10.89.1.100"
        assert topo.get_client_network() == "ccm-mycluster-dc1-az1"

        # Foreign subnets for node1 (DC1/AZ1)
        foreign = topo.get_foreign_subnets("node1")
        assert set(foreign["inter_rack"]) == {"10.89.2.0/24", "10.89.3.0/24"}
        assert set(foreign["inter_dc"]) == {
            "10.89.4.0/24",
            "10.89.5.0/24",
            "10.89.6.0/24",
        }

        # tc commands for node1 — verify structure matches docs
        cmds = topo.build_tc_commands("node1")
        assert any("prio bands 4" in c for c in cmds)
        assert any("netem delay 1ms" in c for c in cmds)
        assert any("netem delay 40ms" in c for c in cmds)
        # Inter-AZ filters (band 2)
        assert any("10.89.2.0/24" in c and "flowid 1:2" in c for c in cmds)
        assert any("10.89.3.0/24" in c and "flowid 1:2" in c for c in cmds)
        # Inter-DC filters (band 3)
        assert any("10.89.4.0/24" in c and "flowid 1:3" in c for c in cmds)
        assert any("10.89.5.0/24" in c and "flowid 1:3" in c for c in cmds)
        assert any("10.89.6.0/24" in c and "flowid 1:3" in c for c in cmds)
        # No packet loss since 0.0%
        assert not any("loss" in c for c in cmds)

    def test_validation_negative_inter_rack_delay(self):
        """Negative inter-rack delay should fail."""
        topology = OrderedDict([("dc1", OrderedDict([("RAC1", 1)]))])
        with pytest.raises(ValueError, match="inter_rack_delay_ms must be >= 0"):
            PodmanNetworkTopology(
                cluster_name="test", topology=topology, inter_rack_delay_ms=-1
            )

    def test_validation_negative_inter_dc_delay(self):
        """Negative inter-DC delay should fail."""
        topology = OrderedDict([("dc1", OrderedDict([("RAC1", 1)]))])
        with pytest.raises(ValueError, match="inter_dc_delay_ms must be >= 0"):
            PodmanNetworkTopology(
                cluster_name="test", topology=topology, inter_dc_delay_ms=-5
            )

    def test_validation_packet_loss_out_of_range(self):
        """Packet loss outside 0-100 should fail."""
        topology = OrderedDict([("dc1", OrderedDict([("RAC1", 1)]))])
        with pytest.raises(ValueError, match="packet_loss_percent must be between"):
            PodmanNetworkTopology(
                cluster_name="test", topology=topology, packet_loss_percent=101.0
            )
        with pytest.raises(ValueError, match="packet_loss_percent must be between"):
            PodmanNetworkTopology(
                cluster_name="test", topology=topology, packet_loss_percent=-0.1
            )


class TestPodmanProcess:
    """Unit tests for PodmanProcess."""

    def test_poll_returns_none_initially(self):
        """A new PodmanProcess should have returncode=None."""
        p = PodmanProcess("nonexistent-container-id-12345")
        assert p.returncode is None

    def test_poll_detects_missing_container(self):
        """poll() should set returncode=-1 for a non-existent container."""
        p = PodmanProcess("nonexistent-container-id-12345")
        result = p.poll()
        # Container doesn't exist, so returncode should be set to -1
        assert result == -1
        assert p.returncode == -1

    def test_poll_caches_returncode(self):
        """Once returncode is set, poll() should return it without re-checking."""
        p = PodmanProcess("nonexistent-container-id-12345")
        p.returncode = 42
        assert p.poll() == 42


class TestFilterArgs:
    """Unit tests for ScyllaPodmanNode.filter_args."""

    def test_filter_args_does_not_mutate_input(self):
        """filter_args should not modify the original list."""
        from ccmlib.scylla_podman_cluster import ScyllaPodmanNode

        original = [
            "scylla",
            "--opt1",
            "val1",
            "--overprovisioned",
            "--seeds",
            "10.0.0.1",
        ]
        original_copy = list(original)
        ScyllaPodmanNode.filter_args(original)
        assert original == original_copy, "filter_args mutated the input list"

    def test_filter_args_skips_preamble_and_options_file(self):
        """filter_args should skip the launch binary, --options-file, and
        --log-to-stdout while keeping whitelisted flags."""
        from ccmlib.scylla_podman_cluster import ScyllaPodmanNode

        args = [
            "/path/to/scylla",
            "--options-file",
            "/path/to/scylla.yaml",
            "--log-to-stdout",
            "1",
            "--smp",
            "2",
            "--memory",
            "512M",
            "--seeds",
            "10.0.0.1",
            "--developer-mode",
            "true",
        ]
        result = ScyllaPodmanNode.filter_args(args)
        assert "--options-file" not in result
        assert "--log-to-stdout" not in result
        assert "/path/to/scylla" not in result
        assert "--seeds" in result
        assert "10.0.0.1" in result
        assert "--smp" in result
        assert "2" in result
        # --developer-mode true should become --developer-mode 1
        idx = result.index("--developer-mode")
        assert result[idx + 1] == "1"

    def test_filter_args_preserves_boolean_flags(self):
        """Boolean flags should not consume the following flag as their value."""
        from ccmlib.scylla_podman_cluster import ScyllaPodmanNode

        args = [
            "/path/to/scylla",
            "--options-file",
            "/path/to/scylla.yaml",
            "--experimental",
            "--smp",
            "2",
            "--disable-version-check",
            "--memory",
            "512M",
        ]

        result = ScyllaPodmanNode.filter_args(args)

        assert result == [
            "--experimental",
            "--smp",
            "2",
            "--disable-version-check",
            "--memory",
            "512M",
        ]


class TestPodmanNodeBehavior:
    def test_wait_for_binary_interface_checks_inside_container(self, monkeypatch):
        from ccmlib.scylla_podman_cluster import ScyllaPodmanNode

        node = object.__new__(ScyllaPodmanNode)
        node.cluster = SimpleNamespace(version=lambda: "2026.1")
        node.network_interfaces = {"binary": ("10.90.1.1", 9042)}
        node.pid = "container-id"

        seen = {"watch_log_for": False, "wait_for": False, "run": []}

        monkeypatch.setattr(
            node,
            "watch_log_for",
            lambda *args, **kwargs: seen.__setitem__("watch_log_for", True),
        )
        monkeypatch.setattr(
            "ccmlib.scylla_podman_cluster.common.check_socket_listening",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                AssertionError("host-side socket check should not be used")
            ),
        )

        def fake_run(cmd, stdout=None, stderr=None):
            seen["run"].append(cmd)
            return SimpleNamespace(returncode=0)

        def fake_wait_for(func, timeout, first=0.0, step=1.0):
            seen["wait_for"] = True
            return func()

        monkeypatch.setattr("ccmlib.scylla_podman_cluster.run", fake_run)
        monkeypatch.setattr(
            "ccmlib.scylla_podman_cluster.common.wait_for", fake_wait_for
        )

        node.wait_for_binary_interface(timeout=5)

        assert seen["watch_log_for"]
        assert seen["wait_for"]
        assert seen["run"] == [
            [
                "podman",
                "exec",
                "container-id",
                "python3",
                "-c",
                "import socket; sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM); sock.settimeout(1.0); sock.connect(('10.90.1.1', 9042)); sock.close()",
            ]
        ]

    def test_wait_for_binary_interface_times_out_when_container_probe_fails(
        self, monkeypatch
    ):
        from ccmlib.scylla_podman_cluster import ScyllaPodmanNode

        node = object.__new__(ScyllaPodmanNode)
        node.cluster = SimpleNamespace(version=lambda: "2026.1")
        node.network_interfaces = {"binary": ("10.90.1.1", 9042)}
        node.pid = "container-id"

        monkeypatch.setattr(node, "watch_log_for", lambda *args, **kwargs: True)
        monkeypatch.setattr(
            "ccmlib.scylla_podman_cluster.common.check_socket_listening",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                AssertionError("host-side socket check should not be used")
            ),
        )
        monkeypatch.setattr(
            "ccmlib.scylla_podman_cluster.run",
            lambda *args, **kwargs: SimpleNamespace(returncode=1),
        )
        monkeypatch.setattr(
            "ccmlib.scylla_podman_cluster.common.wait_for",
            lambda func, timeout, first=0.0, step=1.0: False,
        )

        with pytest.raises(TimeoutError, match="10.90.1.1:9042"):
            node.wait_for_binary_interface(timeout=5)


class TestPodmanClusterBehavior:
    def test_populate_retries_with_another_subnet_prefix_on_collision(
        self, monkeypatch
    ):
        from ccmlib.scylla_podman_cluster import ScyllaPodmanCluster

        cluster = object.__new__(ScyllaPodmanCluster)
        cluster.name = "testcluster"
        cluster.inter_rack_delay_ms = 1
        cluster.inter_dc_delay_ms = 50
        cluster.packet_loss_percent = 0.0
        cluster.network_topology = None
        cluster.nodes = OrderedDict()
        cluster.snitch = "org.apache.cassandra.locator.GossipingPropertyFileSnitch"
        cluster.seeds = []
        cluster.use_vnodes = False
        cluster._config_options = {}
        cluster._scylla_manager = None
        cluster.pinning = False
        cluster._cpu_assignments = {}

        monkeypatch.setattr(
            cluster,
            "_parse_topology",
            lambda nodes: OrderedDict([("DC1", OrderedDict([("RAC1", 1)]))]),
        )
        monkeypatch.setattr(cluster, "set_configuration_options", lambda values: None)
        monkeypatch.setattr(
            cluster, "balanced_tokens", lambda node_count: [None] * node_count
        )
        monkeypatch.setattr(
            cluster,
            "balanced_tokens_across_dcs",
            lambda node_locations: [None] * len(node_locations),
        )
        monkeypatch.setattr(cluster, "new_node", lambda *args, **kwargs: None)
        monkeypatch.setattr(cluster, "_update_config", lambda *args, **kwargs: None)
        monkeypatch.setattr(cluster, "cluster_cleanup", lambda: None)

        attempted_prefixes = []
        prefixes = iter(["10.89", "10.90"])

        def fake_find_available_subnet_prefix(exclude_prefixes=None):
            prefix = next(prefixes)
            attempted_prefixes.append(prefix)
            return prefix

        def fake_create_networks(self):
            if self.subnet_prefix == "10.89":
                raise RuntimeError(
                    "Failed to create podman network test: Error: subnet 10.89.1.0/24 is already used on the host or by another config"
                )

        monkeypatch.setattr(
            "ccmlib.scylla_podman_cluster._find_available_subnet_prefix",
            fake_find_available_subnet_prefix,
        )
        monkeypatch.setattr(
            "ccmlib.scylla_podman_cluster.PodmanNetworkTopology.create_networks",
            fake_create_networks,
        )

        cluster.populate(1)

        assert attempted_prefixes == ["10.89", "10.90"]
        assert cluster.network_topology is not None
        assert cluster.network_topology.subnet_prefix == "10.90"
        assert cluster.network_topology.get_node_ip("node1") == "10.90.1.1"

    def test_populate_honors_env_subnet_prefix_without_retry(self, monkeypatch):
        from ccmlib.scylla_podman_cluster import ScyllaPodmanCluster

        cluster = object.__new__(ScyllaPodmanCluster)
        cluster.name = "testcluster"
        cluster.inter_rack_delay_ms = 1
        cluster.inter_dc_delay_ms = 50
        cluster.packet_loss_percent = 0.0
        cluster.network_topology = None
        cluster.nodes = OrderedDict()
        cluster.snitch = "org.apache.cassandra.locator.GossipingPropertyFileSnitch"
        cluster.seeds = []
        cluster.use_vnodes = False
        cluster._config_options = {}
        cluster._scylla_manager = None
        cluster.pinning = False
        cluster._cpu_assignments = {}

        monkeypatch.setattr(
            cluster,
            "_parse_topology",
            lambda nodes: OrderedDict([("DC1", OrderedDict([("RAC1", 1)]))]),
        )
        monkeypatch.setattr(cluster, "set_configuration_options", lambda values: None)
        monkeypatch.setattr(
            cluster, "balanced_tokens", lambda node_count: [None] * node_count
        )
        monkeypatch.setattr(
            cluster,
            "balanced_tokens_across_dcs",
            lambda node_locations: [None] * len(node_locations),
        )
        monkeypatch.setattr(cluster, "new_node", lambda *args, **kwargs: None)
        monkeypatch.setattr(cluster, "_update_config", lambda *args, **kwargs: None)
        monkeypatch.setattr(cluster, "cluster_cleanup", lambda: None)
        monkeypatch.setenv("CCM_PODMAN_SUBNET_PREFIX", "10.123")

        def fake_create_networks(self):
            raise RuntimeError(
                "Failed to create podman network test: Error: subnet 10.123.1.0/24 is already used on the host or by another config"
            )

        monkeypatch.setattr(
            "ccmlib.scylla_podman_cluster.PodmanNetworkTopology.create_networks",
            fake_create_networks,
        )

        with pytest.raises(RuntimeError, match="already used"):
            cluster.populate(1)

    def test_cluster_add_rejected_for_podman(self):
        from ccmlib.cmds.cluster_cmds import ClusterAddCmd

        cmd = object.__new__(ClusterAddCmd)
        cmd.options = SimpleNamespace(
            scylla_node=True,
            bootstrap=False,
            is_seed=False,
            data_center=None,
            rack=None,
        )
        cmd.cluster = SimpleNamespace(is_podman=lambda: True, is_docker=lambda: False)
        cmd.name = "node4"
        cmd.storage = ("10.0.0.4", 7000)
        cmd.binary = ("10.0.0.4", 9042)
        cmd.jmx_port = "7199"
        cmd.remote_debug_port = "0"
        cmd.initial_token = None

        with patch("sys.exit", side_effect=SystemExit(1)):
            with pytest.raises(SystemExit):
                cmd.run()

    def test_start_client_container_uses_documented_ip_flags(self, monkeypatch):
        from ccmlib.scylla_podman_cluster import PODMAN_RESOURCE_OWNER_LABEL
        from ccmlib.scylla_podman_cluster import ScyllaPodmanCluster

        cluster = object.__new__(ScyllaPodmanCluster)
        cluster.name = "testcluster"
        cluster.podman_image = "docker.io/scylladb/scylla:2026.1"
        cluster._client_container_id = None
        cluster.network_topology = SimpleNamespace(
            get_client_ip=lambda: "10.89.1.100",
            get_client_network=lambda: "ccm-testcluster-dc1-rack1",
            build_tc_commands=lambda node_name: [],
        )

        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            if cmd[:3] == ["podman", "run", "-d"]:
                return SimpleNamespace(returncode=0, stdout="client123\n", stderr="")
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        monkeypatch.setattr("ccmlib.scylla_podman_cluster.run", fake_run)
        monkeypatch.setattr(
            cluster,
            "_setup_container_routes",
            lambda client_name, node_name: None,
        )

        cluster.start_client_container()

        run_cmd = next(cmd for cmd in calls if cmd[:3] == ["podman", "run", "-d"])
        assert "--network" in run_cmd
        network_idx = run_cmd.index("--network")
        assert run_cmd[network_idx + 1] == "ccm-testcluster-dc1-rack1"
        assert "--ip" in run_cmd
        ip_idx = run_cmd.index("--ip")
        assert run_cmd[ip_idx + 1] == "10.89.1.100"
        assert f"{PODMAN_RESOURCE_OWNER_LABEL}={os.getpid()}" in run_cmd
        assert not any(":ip=" in part for part in run_cmd)

    def test_setup_container_routes_logs_failures(self, monkeypatch):
        from ccmlib.scylla_podman_cluster import ScyllaPodmanCluster

        cluster = object.__new__(ScyllaPodmanCluster)
        cluster.network_topology = SimpleNamespace(
            get_routes_for_node=lambda node_name: [("10.89.2.0/24", "10.89.1.254")]
        )

        warnings = []

        monkeypatch.setattr(
            "ccmlib.scylla_podman_cluster._nsenter_net_run",
            lambda container_id, command: SimpleNamespace(
                returncode=1, stderr="route failed"
            ),
        )
        monkeypatch.setattr(
            "ccmlib.scylla_podman_cluster.LOGGER.warning",
            lambda *args: warnings.append(args[0] if len(args) == 1 else args[0] % args[1:]),
        )

        cluster._setup_container_routes("client123", "node1")

        assert warnings == [
            "Failed to add route 10.89.2.0/24 via 10.89.1.254 in client123: route failed"
        ]


class TestPodmanContainerCreate:
    def test_create_container_uses_documented_ip_flags(self, monkeypatch):
        from ccmlib.scylla_podman_cluster import PODMAN_RESOURCE_OWNER_LABEL
        from ccmlib.scylla_podman_cluster import ScyllaPodmanNode

        node = object.__new__(ScyllaPodmanNode)
        node.name = "node1"
        node.pid = None
        node.podman_name = "ccm-test-node1"
        node.base_data_path = "/var/lib/scylla"
        node.local_yaml_path = "/tmp/node1-conf"
        node.share_directories = []
        node.log_thread = None
        node.network_interfaces = {
            "storage": ("127.0.0.1", 7000),
            "binary": ("127.0.0.1", 9042),
        }
        node.cluster = SimpleNamespace(
            podman_image="docker.io/scylladb/scylla:2026.1",
            network_topology=SimpleNamespace(
                get_node_network=lambda node_name: "ccm-testcluster-dc1-rack1"
            ),
            nodelist=lambda: [
                SimpleNamespace(
                    name="node1", network_interfaces={"storage": ("10.89.1.1", 7000)}
                )
            ],
        )

        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            if cmd[:2] == ["podman", "run"]:
                return SimpleNamespace(returncode=0, stdout="container123\n", stderr="")
            if cmd[:3] == ["podman", "exec", "container123"]:
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        monkeypatch.setattr("ccmlib.scylla_podman_cluster.run", fake_run)
        monkeypatch.setattr(
            "ccmlib.scylla_podman_cluster.common.wait_for",
            lambda func, timeout, step: True,
        )
        monkeypatch.setattr(node, "_prepare_bind_mounts", lambda: None)
        monkeypatch.setattr(node, "_get_rack_ip", lambda: "10.89.1.1")
        monkeypatch.setattr(node, "read_scylla_yaml", lambda: {})
        monkeypatch.setattr(node, "get_path", lambda: "/tmp/node1")
        monkeypatch.setattr(node, "_scylla_service_name", lambda: "scylla")
        monkeypatch.setattr(node, "_jmx_service_name", lambda: None)
        monkeypatch.setattr(node, "service_stop", lambda service_name: None)
        monkeypatch.setattr(node, "_setup_routes", lambda: None)
        monkeypatch.setattr(
            "ccmlib.scylla_podman_cluster.PodmanLogger",
            lambda node_obj, log_path: SimpleNamespace(
                start=lambda: None, stop=lambda: None
            ),
        )

        node.create_container([])

        run_cmd = next(cmd for cmd in calls if cmd[:2] == ["podman", "run"])
        assert "--network" in run_cmd
        network_idx = run_cmd.index("--network")
        assert run_cmd[network_idx + 1] == "ccm-testcluster-dc1-rack1"
        assert "--ip" in run_cmd
        ip_idx = run_cmd.index("--ip")
        assert run_cmd[ip_idx + 1] == "10.89.1.1"
        assert f"{PODMAN_RESOURCE_OWNER_LABEL}={os.getpid()}" in run_cmd
        assert not any(":ip=" in part for part in run_cmd)


class TestPodmanConftestCleanup:
    def test_prune_stale_resources_keeps_live_owner_resources(self, monkeypatch):
        from tests import conftest

        run_calls = []

        def fake_run(cmd, stdout=None, stderr=None, text=False):
            run_calls.append(cmd)
            if cmd[:4] == ["podman", "ps", "-a", "--format"]:
                return SimpleNamespace(
                    returncode=0,
                    stdout='[{"Names":["ccm-live"],"State":"running","Labels":{"org.scylladb.ccm-owner-pid":"111"}},{"Names":["ccm-dead"],"State":"running","Labels":{"org.scylladb.ccm-owner-pid":"222"}}]',
                    stderr="",
                )
            if cmd[:4] == ["podman", "network", "ls", "--format"]:
                return SimpleNamespace(
                    returncode=0,
                    stdout='[{"name":"ccm-live-net","labels":{"org.scylladb.ccm-owner-pid":"111"}},{"name":"ccm-dead-net","labels":{"org.scylladb.ccm-owner-pid":"222"}}]',
                    stderr="",
                )
            if cmd[:3] == ["podman", "network", "inspect"]:
                if cmd[-1] == "ccm-dead-net":
                    return SimpleNamespace(
                        returncode=0,
                        stdout='[{"containers":{"abc":{"name":"ccm-dead"}}}]',
                        stderr="",
                    )
                return SimpleNamespace(
                    returncode=0,
                    stdout='[{"containers":{"xyz":{"name":"ccm-live"}}}]',
                    stderr="",
                )
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        monkeypatch.setattr(conftest, "run", fake_run)
        monkeypatch.setattr(conftest, "_pid_is_alive", lambda pid: pid == 111)

        conftest._prune_stale_ccm_podman_resources()

        assert ["podman", "rm", "-f", "ccm-dead"] in run_calls
        assert ["podman", "network", "rm", "-f", "ccm-dead-net"] in run_calls
        assert ["podman", "rm", "-f", "ccm-live"] not in run_calls
        assert ["podman", "network", "rm", "-f", "ccm-live-net"] not in run_calls

    def test_prune_stale_resources_ignores_unlabeled_resources(self, monkeypatch):
        from tests import conftest

        run_calls = []

        def fake_run(cmd, stdout=None, stderr=None, text=False):
            run_calls.append(cmd)
            if cmd[:4] == ["podman", "ps", "-a", "--format"]:
                return SimpleNamespace(
                    returncode=0,
                    stdout='[{"Names":["ccm-unlabeled"],"State":"exited","Labels":{}}]',
                    stderr="",
                )
            if cmd[:4] == ["podman", "network", "ls", "--format"]:
                return SimpleNamespace(
                    returncode=0,
                    stdout='[{"name":"ccm-unlabeled-net","labels":{}}]',
                    stderr="",
                )
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        monkeypatch.setattr(conftest, "run", fake_run)
        monkeypatch.setattr(conftest, "_pid_is_alive", lambda pid: False)

        conftest._prune_stale_ccm_podman_resources()

        assert ["podman", "rm", "-f", "ccm-unlabeled"] not in run_calls
        assert ["podman", "network", "rm", "-f", "ccm-unlabeled-net"] not in run_calls

    def test_podman_cluster_fixture_removes_cluster_on_start_failure(self, monkeypatch):
        from tests import conftest

        removed = {"called": False}

        class FakeCluster:
            def set_configuration_options(self, values):
                pass

            def populate(self, nodes):
                pass

            def start(self, wait_for_binary_proto=True):
                raise RuntimeError("boom")

            def remove(self):
                removed["called"] = True

        monkeypatch.setattr(
            conftest, "ScyllaPodmanCluster", lambda *args, **kwargs: FakeCluster()
        )

        fixture = conftest.podman_cluster.__wrapped__("/tmp/testdir", "tid")
        with pytest.raises(RuntimeError, match="boom"):
            next(fixture)
        assert removed["called"]


class TestPodmanNodeRemoveAndStatus:
    """Unit tests for remove(), service_status(), kill(), and __update_status() interaction."""

    def _make_node(self):
        from ccmlib.scylla_podman_cluster import ScyllaPodmanNode

        class TestableScyllaPodmanNode(ScyllaPodmanNode):
            @property
            def api_port(self):
                return 10000

        node = object.__new__(TestableScyllaPodmanNode)
        node.name = "node1"
        node.pid = "abc123"
        node.log_thread = None
        node.status = SimpleNamespace()  # placeholder
        node._cached_nodetool_support = {}
        return node

    def test_remove_clears_pid_before_podman_rm(self, monkeypatch):
        """Verify that remove() sets self.pid = None before calling podman rm.

        This ordering prevents __update_status() from exec-ing into a removed container.
        """
        from ccmlib.scylla_podman_cluster import ScyllaPodmanNode

        node = self._make_node()

        call_order = []
        original_pid = node.pid

        def fake_run(cmd, **kwargs):
            call_order.append(("run", cmd, node.pid))
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        monkeypatch.setattr("ccmlib.scylla_podman_cluster.run", fake_run)

        node.remove()

        # pid should be None after remove
        assert node.pid is None
        # podman rm should have been called (first try succeeds, no fallback)
        assert len(call_order) == 1
        run_entry = call_order[0]
        assert "podman" in run_entry[1]
        assert original_pid in run_entry[1]
        # pid was already None when podman rm was called
        assert run_entry[2] is None

    def test_remove_with_none_pid_is_noop(self, monkeypatch):
        """Verify remove() doesn't crash when pid is already None."""
        node = self._make_node()
        node.pid = None
        calls = []
        monkeypatch.setattr(
            "ccmlib.scylla_podman_cluster.run",
            lambda *a, **kw: calls.append(1) or SimpleNamespace(returncode=0),
        )
        node.remove()
        assert calls == []  # no podman rm should have been called

    def test_remove_stops_log_thread(self, monkeypatch):
        """Verify remove() stops the log thread."""
        node = self._make_node()
        stopped = {"called": False}
        node.log_thread = SimpleNamespace(
            stop=lambda: stopped.__setitem__("called", True)
        )
        monkeypatch.setattr(
            "ccmlib.scylla_podman_cluster.run",
            lambda *a, **kw: SimpleNamespace(returncode=0),
        )
        node.remove()
        assert stopped["called"]
        assert node.log_thread is None

    def test_service_status_returns_down_when_pid_is_none(self):
        """Verify service_status() returns 'DOWN' when container is gone."""
        node = self._make_node()
        node.pid = None
        assert node.service_status("scylla") == "DOWN"

    def test_service_status_returns_status_on_success(self, monkeypatch):
        """Verify service_status() parses supervisorctl output correctly."""
        node = self._make_node()
        monkeypatch.setattr(
            "ccmlib.scylla_podman_cluster.run",
            lambda *a, **kw: SimpleNamespace(
                returncode=0, stdout="scylla RUNNING pid 123, uptime 0:01:00", stderr=""
            ),
        )
        assert node.service_status("scylla") == "RUNNING"

    def test_service_status_returns_down_on_failure(self, monkeypatch):
        """Verify service_status() returns 'DOWN' on exec failure."""
        node = self._make_node()
        monkeypatch.setattr(
            "ccmlib.scylla_podman_cluster.run",
            lambda *a, **kw: SimpleNamespace(
                returncode=1, stdout="", stderr="no such container"
            ),
        )
        assert node.service_status("scylla") == "DOWN"

    def test_service_status_handles_unexpected_output(self, monkeypatch):
        """Verify service_status() doesn't crash on short/unexpected output."""
        node = self._make_node()
        monkeypatch.setattr(
            "ccmlib.scylla_podman_cluster.run",
            lambda *a, **kw: SimpleNamespace(returncode=0, stdout="scylla", stderr=""),
        )
        # Only one word in output — can't parse status, should return "DOWN", not crash
        assert node.service_status("scylla") == "DOWN"

    def test_kill_with_none_pid_is_noop(self, monkeypatch):
        """Verify kill() doesn't crash when pid is None."""
        node = self._make_node()
        node.pid = None
        calls = []
        monkeypatch.setattr(
            "ccmlib.scylla_podman_cluster.run",
            lambda *a, **kw: (
                calls.append(1) or SimpleNamespace(returncode=0, stdout="", stderr="")
            ),
        )
        node.kill(9)
        assert calls == []

    def test_kill_sends_signal_to_correct_pid(self, monkeypatch):
        """Verify kill() gets service PID then sends signal without shell."""
        node = self._make_node()
        calls = []

        def fake_run(cmd, stdout=None, stderr=None, text=False):
            calls.append(cmd)
            if "supervisorctl" in cmd and "pid" in cmd:
                return SimpleNamespace(returncode=0, stdout="42\n", stderr="")
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        monkeypatch.setattr("ccmlib.scylla_podman_cluster.run", fake_run)
        # Need _scylla_service_name to work
        monkeypatch.setattr(node, "_scylla_service_name", lambda: "scylla")

        node.kill(15)

        # First call: supervisorctl pid scylla
        assert "supervisorctl" in calls[0]
        assert "pid" in calls[0]
        # Second call: bash -c "kill -15 42" (kill is a shell builtin, not a binary)
        assert "bash" in calls[1]
        assert "-c" in calls[1]
        kill_arg = calls[1][-1]
        assert "kill" in kill_arg
        assert "-15" in kill_arg
        assert "42" in kill_arg

    def test_nodetool_raises_when_pid_is_none(self):
        """Verify nodetool() raises RuntimeError when container is gone."""
        node = self._make_node()
        node.pid = None
        with pytest.raises(RuntimeError, match="no running container"):
            node.nodetool("status")

    def test_nodetool_caches_supported_command_probe(self, monkeypatch):
        """Supported native nodetool commands should only be probed once per node."""
        node = self._make_node()
        check_calls = []
        run_calls = []

        def fake_check_call(cmd, **kwargs):
            check_calls.append(cmd)
            return 0

        def fake_do_run_nodetool(cmd, capture_output, wait, timeout, verbose):
            run_calls.append(cmd)
            return "ok", ""

        monkeypatch.setattr(
            "ccmlib.scylla_podman_cluster.subprocess.check_call", fake_check_call
        )
        monkeypatch.setattr(node, "_do_run_nodetool", fake_do_run_nodetool)

        assert node.nodetool("status") == ("ok", "")
        assert node.nodetool("status") == ("ok", "")

        assert len(check_calls) == 1
        assert len(run_calls) == 2
        assert run_calls[0] == [
            "podman",
            "exec",
            "abc123",
            "scylla",
            "nodetool",
            "-h",
            "localhost",
            "-p",
            "10000",
            "status",
        ]

    def test_nodetool_caches_unsupported_command_probe(self, monkeypatch):
        """Unsupported native commands should only probe once before JMX fallback."""
        node = self._make_node()
        node._cached_supervisor_programs = {"scylla", "scylla-jmx"}
        check_calls = []
        run_calls = []

        def fake_check_call(cmd, **kwargs):
            check_calls.append(cmd)
            raise subprocess.CalledProcessError(1, cmd)

        def fake_do_run_nodetool(cmd, capture_output, wait, timeout, verbose):
            run_calls.append(cmd)
            return "fallback", ""

        monkeypatch.setattr(
            "ccmlib.scylla_podman_cluster.subprocess.check_call", fake_check_call
        )
        monkeypatch.setattr(node, "_do_run_nodetool", fake_do_run_nodetool)

        assert node.nodetool("cleanup") == ("fallback", "")
        assert node.nodetool("cleanup") == ("fallback", "")

        assert len(check_calls) == 1
        assert run_calls == [
            [
                "podman",
                "exec",
                "abc123",
                "nodetool",
                "-Dcom.scylladb.apiPort=10000",
                "cleanup",
            ],
            [
                "podman",
                "exec",
                "abc123",
                "nodetool",
                "-Dcom.scylladb.apiPort=10000",
                "cleanup",
            ],
        ]

    def test_service_start_raises_on_failure(self, monkeypatch):
        """Verify failed supervisor start does not silently continue."""
        node = self._make_node()
        calls = []
        run_kwargs = []

        def fake_run(cmd, **kw):
            calls.append(cmd)
            run_kwargs.append(kw)
            # First call is service_status() — return STOPPED so no clear needed
            if len(calls) == 1:
                return SimpleNamespace(returncode=0, stdout="scylla STOPPED", stderr="")
            # Second call is supervisorctl start — fail
            return SimpleNamespace(returncode=1, stdout="", stderr="boom")

        monkeypatch.setattr("ccmlib.scylla_podman_cluster.run", fake_run)
        with pytest.raises(RuntimeError, match="failed to start.*boom"):
            node.service_start("scylla")

        assert run_kwargs[1]["text"] is True

    def test_service_start_clears_fatal_before_start(self, monkeypatch):
        """Verify service_start() clears FATAL state before starting."""
        node = self._make_node()
        calls = []

        def fake_run(cmd, **kw):
            calls.append(cmd)
            if len(calls) == 1:
                # service_status returns FATAL
                return SimpleNamespace(returncode=0, stdout="scylla FATAL", stderr="")
            # clear and start both succeed
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        monkeypatch.setattr("ccmlib.scylla_podman_cluster.run", fake_run)
        node.service_start("scylla")

        # Should have 3 calls: status, clear, start
        assert len(calls) == 3
        assert "status" in calls[0][-1] or calls[0] == [
            "podman",
            "exec",
            "abc123",
            "supervisorctl",
            "status",
            "scylla",
        ]
        assert calls[1] == [
            "podman",
            "exec",
            "abc123",
            "supervisorctl",
            "clear",
            "scylla",
        ]
        assert calls[2] == [
            "podman",
            "exec",
            "abc123",
            "supervisorctl",
            "start",
            "scylla",
        ]

    def test_wait_until_stopped_does_not_call_os_kill(self, monkeypatch):
        """Verify wait_until_stopped doesn't try os.kill on a container ID string."""
        from ccmlib.scylla_podman_cluster import ScyllaPodmanNode
        from ccmlib.node import Status

        node = self._make_node()
        node.status = Status.UP
        call_count = {"status": 0}

        def fake_run(cmd, **kw):
            call_count["status"] += 1
            # After first check, report as stopped
            if call_count["status"] >= 2:
                return SimpleNamespace(returncode=0, stdout="scylla STOPPED", stderr="")
            return SimpleNamespace(
                returncode=0, stdout="scylla RUNNING pid 42, uptime 0:01:00", stderr=""
            )

        monkeypatch.setattr("ccmlib.scylla_podman_cluster.run", fake_run)
        # Stub out _update_config since it needs a full cluster/filesystem
        monkeypatch.setattr(ScyllaPodmanNode, "_update_config", lambda self: None)
        # Should not raise, and should not try os.kill on a string pid
        node.wait_until_stopped(wait_seconds=5)
        assert not node.is_running()


class TestPodmanClusterFactoryLoad:
    def test_load_restores_log_level_and_topology_without_rewriting_config(
        self, monkeypatch
    ):
        from ccmlib.cluster_factory import ClusterFactory

        with tempfile.TemporaryDirectory() as tmpdir:
            cluster_dir = os.path.join(tmpdir, "podman-cluster")
            os.mkdir(cluster_dir)
            cluster_conf = os.path.join(cluster_dir, "cluster.conf")
            original_data = {
                "name": "podman-cluster",
                "nodes": [],
                "seeds": [],
                "partitioner": None,
                "config_options": {},
                "dse_config_options": {},
                "log_level": "DEBUG",
                "use_vnodes": False,
                "id": 0,
                "ipprefix": None,
                "docker_image": "docker.io/scylladb/scylla:2026.1",
                "network_topology": {
                    "cluster_name": "podman-cluster",
                    "topology": {"DC1": {"RAC1": 1}},
                    "inter_rack_delay_ms": 1,
                    "inter_dc_delay_ms": 40,
                    "packet_loss_percent": 0.0,
                    "subnet_prefix": "10.89",
                },
            }
            with open(cluster_conf, "w") as f:
                YAML().dump(original_data, f)

            monkeypatch.setattr(
                "ccmlib.cluster_factory.Node.load",
                lambda cluster_path, node_name, cluster: None,
            )

            cluster = ClusterFactory.load(tmpdir, "podman-cluster")

            assert getattr(cluster, "_Cluster__log_level") == "DEBUG"
            assert cluster.network_topology is not None
            assert cluster.network_topology.inter_dc_delay_ms == 40

            with open(cluster_conf, "r") as f:
                loaded_data = YAML().load(f)

            assert loaded_data == original_data


# ============================================================================
# Integration tests (require podman and a ScyllaDB image)
# ============================================================================


@pytest.mark.network_topology
class TestScyllaPodmanCluster:
    """Integration tests for a multi-DC podman cluster.

    These tests use the `podman_cluster` fixture from conftest.py which creates:
    - DC1: RAC1 (1 node), RAC2 (1 node)
    - DC2: RAC1 (1 node)

    IMPORTANT: Tests are numbered (test_01 through test_09) and MUST run in
    order.  Later tests depend on state created by earlier ones (e.g. test_05
    reads data inserted by test_04; test_07 restarts the node stopped by
    test_06).  Do not use pytest-randomly or reorder these tests.
    """

    @staticmethod
    def parse_nodetool_status(lines):
        """Parse output of nodetool status into a list of node info dicts."""
        keys = ["status", "address"]
        nodes_statuses = []
        line_re = re.compile(
            r"(?P<status>[UNDJ]{2}?)\s+(?P<address>[\d]{1,3}\.[\d]{1,3}\.[\d]{1,3}\.[\d]{1,3}?)\s"
        )
        for line in lines:
            node_status = {}
            res = line_re.search(line)
            if res:
                for key in keys:
                    node_status[key] = res[key]
                nodes_statuses.append(node_status)
        return nodes_statuses

    def test_01_cluster_topology(self, podman_cluster):
        """Verify nodes are correctly assigned to DCs and racks."""
        nodes = podman_cluster.nodelist()
        assert len(nodes) == 3

        topo = podman_cluster.network_topology
        assert topo is not None
        prefix = topo.subnet_prefix

        # Verify IP assignments
        assert topo.get_node_ip("node1") == f"{prefix}.1.1"  # DC1/RAC1
        assert topo.get_node_ip("node2") == f"{prefix}.2.1"  # DC1/RAC2
        assert topo.get_node_ip("node3") == f"{prefix}.3.1"  # DC2/RAC1

    def test_02_all_nodes_up(self, podman_cluster):
        """Verify all nodes are running."""
        for node in podman_cluster.nodelist():
            assert node.is_running(), f"Node {node.name} is not running"

    def test_03_nodetool_status(self, podman_cluster):
        """Verify nodetool status shows all nodes as UN."""
        node1 = podman_cluster.nodelist()[0]
        status, error = node1.nodetool("status")
        assert error == ""
        nodes_statuses = self.parse_nodetool_status(status.splitlines())
        assert len(nodes_statuses) == 3
        assert all(node["status"] == "UN" for node in nodes_statuses), (
            f"Not all nodes are UN: {nodes_statuses}"
        )

    def test_04_cqlsh(self, podman_cluster):
        """Verify CQL operations work across the cluster."""
        node1 = podman_cluster.nodelist()[0]

        node1.run_cqlsh(
            """
            CREATE KEYSPACE IF NOT EXISTS test_ks
            WITH replication = {
                'class': 'NetworkTopologyStrategy',
                'dc1': 2,
                'dc2': 1
            };
            USE test_ks;
            CREATE TABLE IF NOT EXISTS test (key int PRIMARY KEY, value text);
            INSERT INTO test (key, value) VALUES (1, 'hello');
            """
        )
        rv = node1.run_cqlsh("SELECT * FROM test_ks.test", return_output=True)
        assert "hello" in rv[0], f"Expected 'hello' in output, got: {rv[0]}"
        assert rv[1] == ""

    def test_05_cross_dc_query(self, podman_cluster):
        """Verify queries work from a node in DC2 after data was written in DC1."""
        node3 = podman_cluster.nodelist()[2]  # DC2/RAC1
        rv = node3.run_cqlsh(
            "SELECT * FROM test_ks.test WHERE key = 1",
            return_output=True,
        )
        assert "hello" in rv[0], f"Cross-DC read failed: {rv[0]}"

    def test_06_stop_and_start_node(self, podman_cluster):
        """Verify a node can be stopped and restarted."""
        node2 = podman_cluster.nodelist()[1]  # DC1/RAC2

        node2.stop(gently=True)
        assert not node2.is_running()

        node2.start(wait_for_binary_proto=True)
        assert node2.is_running()

    def test_07_stop_ungently(self, podman_cluster):
        """Verify ungraceful stop (kill -9) works."""
        node2 = podman_cluster.nodelist()[1]  # DC1/RAC2

        node2.stop(gently=False)
        node2.start(wait_for_binary_proto=True)
        assert node2.is_running()

    def test_08_network_isolation(self, podman_cluster):
        """Verify that nodes are on different podman networks per rack."""
        topo = podman_cluster.network_topology
        # Each rack should have its own network
        networks = set()
        for node in podman_cluster.nodelist():
            networks.add(topo.get_node_network(node.name))
        assert len(networks) == 3, f"Expected 3 distinct networks, got {networks}"

    def test_09_is_podman(self, podman_cluster):
        """Verify cluster and nodes report as podman."""
        assert podman_cluster.is_podman()
        assert podman_cluster.is_docker()  # is_docker() returns True for compat
        for node in podman_cluster.nodelist():
            assert node.is_podman()


# ============================================================================
# Unit tests for CPU pinning feature (no podman required)
# ============================================================================


class TestCpuPinning:
    """Unit tests for CPU pinning assignment, Scylla arg injection,
    podman arg injection, and io_properties generation."""

    def _make_cluster_with_nodes(
        self, monkeypatch, num_nodes=3, smp=2, pinning=True, host_cpus=16
    ):
        """Helper: build a ScyllaPodmanCluster with mocked nodes for pinning tests."""
        from ccmlib.scylla_podman_cluster import ScyllaPodmanCluster

        cluster = object.__new__(ScyllaPodmanCluster)
        cluster.name = "test-pinning"
        cluster.podman_image = "scylladb/scylla:latest"
        cluster.pinning = pinning
        cluster._cpu_assignments = {}
        cluster.network_topology = None
        cluster._client_container_id = None
        cluster.inter_rack_delay_ms = 1
        cluster.inter_dc_delay_ms = 50
        cluster.packet_loss_percent = 0.0

        # Build fake nodes
        nodes = OrderedDict()
        for i in range(1, num_nodes + 1):
            node = SimpleNamespace(
                name=f"node{i}",
                smp=lambda _smp=smp: _smp,
            )
            nodes[f"node{i}"] = node
        cluster.nodes = nodes

        monkeypatch.setattr(
            "ccmlib.scylla_podman_cluster.host_cpu_count",
            lambda: host_cpus,
        )
        return cluster

    def test_compute_assignments_basic(self, monkeypatch):
        """3 nodes x 2 smp on a 16-core host -> 3 non-overlapping pairs."""
        cluster = self._make_cluster_with_nodes(
            monkeypatch, num_nodes=3, smp=2, host_cpus=16
        )
        cluster._compute_cpu_assignments()
        assert cluster.pinning is True
        assert cluster._cpu_assignments == {
            "node1": [0, 1],
            "node2": [2, 3],
            "node3": [4, 5],
        }

    def test_compute_assignments_exact_fit(self, monkeypatch):
        """4 nodes x 4 smp on a 16-core host -> exact fit, still valid."""
        cluster = self._make_cluster_with_nodes(
            monkeypatch, num_nodes=4, smp=4, host_cpus=16
        )
        cluster._compute_cpu_assignments()
        assert cluster.pinning is True
        assert len(cluster._cpu_assignments) == 4
        # Verify no overlap
        all_cpus = []
        for cpus in cluster._cpu_assignments.values():
            all_cpus.extend(cpus)
        assert len(all_cpus) == len(set(all_cpus)) == 16

    def test_compute_assignments_insufficient_cpus(self, monkeypatch):
        """5 nodes x 4 smp on a 16-core host -> pinning disabled."""
        cluster = self._make_cluster_with_nodes(
            monkeypatch, num_nodes=5, smp=4, host_cpus=16
        )
        cluster._compute_cpu_assignments()
        assert cluster.pinning is False
        assert cluster._cpu_assignments == {}

    def test_compute_assignments_not_implemented(self, monkeypatch):
        """If host_cpu_count() raises NotImplementedError, pinning disabled."""
        from ccmlib.scylla_podman_cluster import ScyllaPodmanCluster

        cluster = self._make_cluster_with_nodes(
            monkeypatch, num_nodes=2, smp=2, host_cpus=8
        )
        monkeypatch.setattr(
            "ccmlib.scylla_podman_cluster.host_cpu_count",
            lambda: (_ for _ in ()).throw(NotImplementedError),
        )
        cluster._compute_cpu_assignments()
        assert cluster.pinning is False
        assert cluster._cpu_assignments == {}

    def test_compute_assignments_empty_cluster(self, monkeypatch):
        """No nodes -> empty assignments, pinning remains True."""
        cluster = self._make_cluster_with_nodes(
            monkeypatch, num_nodes=0, smp=2, host_cpus=16
        )
        cluster._compute_cpu_assignments()
        assert cluster._cpu_assignments == {}

    def test_pinning_container_args_when_pinned(self, monkeypatch):
        """Node returns --cpuset-cpus flag when it has a CPU assignment."""
        from ccmlib.scylla_podman_cluster import ScyllaPodmanNode

        node = object.__new__(ScyllaPodmanNode)
        node.name = "node1"
        node.cluster = SimpleNamespace(_cpu_assignments={"node1": [0, 1, 2, 3]})

        result = node._pinning_container_args()
        assert result == ["--cpuset-cpus", "0,1,2,3"]

    def test_pinning_container_args_when_not_pinned(self, monkeypatch):
        """Node returns empty list when no CPU assignment."""
        from ccmlib.scylla_podman_cluster import ScyllaPodmanNode

        node = object.__new__(ScyllaPodmanNode)
        node.name = "node1"
        node.cluster = SimpleNamespace(_cpu_assignments={})

        result = node._pinning_container_args()
        assert result == []

    def test_pinning_scylla_args_injects_cpuset_removes_overprovisioned(
        self, monkeypatch, tmp_path
    ):
        """When pinned, --cpuset is added, --overprovisioned is removed."""
        from ccmlib.scylla_podman_cluster import ScyllaPodmanNode

        node = object.__new__(ScyllaPodmanNode)
        node.name = "node1"
        node.cluster = SimpleNamespace(_cpu_assignments={"node1": [4, 5]})
        node.local_yaml_path = str(tmp_path)

        input_args = [
            "--smp",
            "2",
            "--memory",
            "1G",
            "--overprovisioned",
            "1",
            "--developer-mode",
            "1",
        ]
        result = node._pinning_scylla_args(input_args)

        assert "--cpuset" in result
        cpuset_idx = result.index("--cpuset")
        assert result[cpuset_idx + 1] == "4,5"
        assert "--overprovisioned" not in result
        assert "--io-setup" in result
        io_idx = result.index("--io-setup")
        assert result[io_idx + 1] == "0"
        assert "--io-properties-file" in result

    def test_pinning_scylla_args_does_not_mutate_input(self, monkeypatch, tmp_path):
        """The original args list is not modified."""
        from ccmlib.scylla_podman_cluster import ScyllaPodmanNode

        node = object.__new__(ScyllaPodmanNode)
        node.name = "node1"
        node.cluster = SimpleNamespace(_cpu_assignments={"node1": [0, 1]})
        node.local_yaml_path = str(tmp_path)

        original = ["--smp", "2", "--overprovisioned", "1"]
        original_copy = list(original)
        node._pinning_scylla_args(original)
        assert original == original_copy

    def test_pinning_scylla_args_noop_when_not_pinned(self, monkeypatch):
        """When not pinned, args pass through unchanged."""
        from ccmlib.scylla_podman_cluster import ScyllaPodmanNode

        node = object.__new__(ScyllaPodmanNode)
        node.name = "node1"
        node.cluster = SimpleNamespace(_cpu_assignments={})

        input_args = ["--smp", "2", "--overprovisioned", "1"]
        result = node._pinning_scylla_args(input_args)
        assert result is input_args  # same object, not a copy

    def test_pinning_scylla_args_replaces_existing_io_setup(
        self, monkeypatch, tmp_path
    ):
        """If --io-setup is already in args, its value is replaced with 0."""
        from ccmlib.scylla_podman_cluster import ScyllaPodmanNode

        node = object.__new__(ScyllaPodmanNode)
        node.name = "node1"
        node.cluster = SimpleNamespace(_cpu_assignments={"node1": [0]})
        node.local_yaml_path = str(tmp_path)

        input_args = ["--io-setup", "1", "--overprovisioned", "1"]
        result = node._pinning_scylla_args(input_args)
        io_idx = result.index("--io-setup")
        assert result[io_idx + 1] == "0"

    def test_write_io_properties(self, tmp_path):
        """io_properties.yaml is written with correct structure and values."""
        from ccmlib.scylla_podman_cluster import ScyllaPodmanNode

        node = object.__new__(ScyllaPodmanNode)
        node.local_yaml_path = str(tmp_path)

        path = node._write_io_properties(4)

        assert os.path.exists(path)
        yaml = YAML()
        with open(path) as f:
            data = yaml.load(f)
        assert "disks" in data
        assert len(data["disks"]) == 1
        disk = data["disks"][0]
        assert disk["mountpoint"] == "/var/lib/scylla"
        assert disk["read_iops"] == 400000  # 100k * 4
        assert disk["write_iops"] == 400000
        assert disk["read_bandwidth"] == 1073741824 * 4
        assert disk["write_bandwidth"] == 1073741824 * 4

    def test_write_io_properties_single_cpu(self, tmp_path):
        """io_properties.yaml for a single CPU has base values."""
        from ccmlib.scylla_podman_cluster import ScyllaPodmanNode

        node = object.__new__(ScyllaPodmanNode)
        node.local_yaml_path = str(tmp_path)

        path = node._write_io_properties(1)
        yaml = YAML()
        with open(path) as f:
            data = yaml.load(f)
        disk = data["disks"][0]
        assert disk["read_iops"] == 100000
        assert disk["write_iops"] == 100000

    def test_pinning_persisted_in_config(self, monkeypatch, tmp_path):
        """The pinning flag is saved in cluster.conf."""
        from ccmlib.scylla_podman_cluster import ScyllaPodmanCluster

        cluster = object.__new__(ScyllaPodmanCluster)
        cluster.name = "test-pinning"
        cluster.podman_image = "scylladb/scylla:latest"
        cluster.pinning = True
        cluster._cpu_assignments = {}
        cluster.network_topology = None
        cluster._client_container_id = None
        cluster.partitioner = None
        cluster._config_options = {}
        cluster._dse_config_options = {}
        cluster.use_vnodes = False
        cluster.id = 0
        cluster.ipprefix = "127.0.0."
        cluster.nodes = OrderedDict()
        cluster.seeds = []

        monkeypatch.setattr(cluster, "get_path", lambda: str(tmp_path))

        cluster._update_config()

        yaml = YAML()
        with open(tmp_path / "cluster.conf") as f:
            data = yaml.load(f)
        assert data["pinning"] is True

    def test_pinning_false_persisted_in_config(self, monkeypatch, tmp_path):
        """Pinning=False is also saved (not omitted)."""
        from ccmlib.scylla_podman_cluster import ScyllaPodmanCluster

        cluster = object.__new__(ScyllaPodmanCluster)
        cluster.name = "test-no-pinning"
        cluster.podman_image = "scylladb/scylla:latest"
        cluster.pinning = False
        cluster._cpu_assignments = {}
        cluster.network_topology = None
        cluster._client_container_id = None
        cluster.partitioner = None
        cluster._config_options = {}
        cluster._dse_config_options = {}
        cluster.use_vnodes = False
        cluster.id = 0
        cluster.ipprefix = "127.0.0."
        cluster.nodes = OrderedDict()
        cluster.seeds = []

        monkeypatch.setattr(cluster, "get_path", lambda: str(tmp_path))

        cluster._update_config()

        yaml = YAML()
        with open(tmp_path / "cluster.conf") as f:
            data = yaml.load(f)
        assert data["pinning"] is False

    def test_filter_args_passes_io_properties_file(self):
        """--io-properties-file is in the whitelist and passes through filter_args."""
        from ccmlib.scylla_podman_cluster import ScyllaPodmanNode

        args = ["--smp", "2", "--io-properties-file", "/etc/scylla/io_properties.yaml"]
        result = ScyllaPodmanNode.filter_args(args)
        assert "--io-properties-file" in result
        idx = result.index("--io-properties-file")
        assert result[idx + 1] == "/etc/scylla/io_properties.yaml"
