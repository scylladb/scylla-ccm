"""Tests for the loaders integration (Loader, LoaderSet) -- pure logic only,
no live podman: IP/name allocation, schema/workload knobs, persistence round-trip."""
import subprocess
from collections import OrderedDict

import pytest

from ccmlib.cmds.cluster_cmds import ClusterLoadersCmd
from ccmlib.common import ArgumentError
from ccmlib.scylla_loaders import (
    Loader, LoaderSet, LB_POLICIES, _pid1_script, _workload_script, WORKLOAD_MODES, DEFAULT_SCHEMA_CONFIG, RUNE_SCRIPT_CONTAINER_PATH,
)
from ccmlib.scylla_podman_cluster import PodmanNetworkTopology


class TestParseRacks:
    def test_malformed_count_raises_argument_error(self):
        with pytest.raises(ArgumentError):
            ClusterLoadersCmd._parse_racks("dc1:rack1=abc")

    def test_zero_count_rejected(self):
        with pytest.raises(ArgumentError):
            ClusterLoadersCmd._parse_racks("dc1:rack1=0")

    def test_negative_count_rejected(self):
        with pytest.raises(ArgumentError):
            ClusterLoadersCmd._parse_racks("dc1:rack1=-1")

    def test_valid_spec_parses(self):
        assert ClusterLoadersCmd._parse_racks("dc1:rack1=5") == [("dc1", "rack1", 5)]


class FakeCluster:
    """Just enough of Cluster/ScyllaPodmanCluster for LoaderSet's pure logic."""

    def __init__(self, topology):
        self.name = "test"
        self.network_topology = topology
        self.config_writes = 0

    def _update_config(self):
        self.config_writes += 1


def make_cluster(racks=(("dc1", "rack1", 2), ("dc1", "rack2", 2))):
    topo_spec = OrderedDict()
    for dc, rack, count in racks:
        topo_spec.setdefault(dc, OrderedDict())[rack] = count
    topology = PodmanNetworkTopology("test", topo_spec)
    return FakeCluster(topology)


class TestLoaderValidation:
    def test_rejects_bad_lb_policy(self):
        cluster = make_cluster()
        with pytest.raises(ValueError):
            Loader(cluster, "l1", "dc1", "rack1", "10.0.1.150", "net", "img", lb_policy="bogus")

    def test_rejects_bad_workload_mode(self):
        cluster = make_cluster()
        with pytest.raises(ValueError):
            Loader(cluster, "l1", "dc1", "rack1", "10.0.1.150", "net", "img", workload_mode="bogus")


class TestLoaderSetAdd:
    def test_add_allocates_sequential_ips_in_loader_band(self, monkeypatch):
        monkeypatch.setattr(Loader, "start", lambda self: None)
        cluster = make_cluster()
        loader_set = LoaderSet(cluster)
        created = loader_set.add("dc1", "rack1", count=2)
        rack_idx = cluster.network_topology.rack_networks[("dc1", "rack1")]["rack_idx"]
        assert [loader.ip for loader in created] == [
            f"10.89.{rack_idx}.150", f"10.89.{rack_idx}.151",
        ]
        # Persisted once per loader (not once per batch), so partial progress
        # in a batch that later fails is never lost.
        assert cluster.config_writes == 2

    def test_rejects_non_positive_count(self):
        cluster = make_cluster()
        loader_set = LoaderSet(cluster)
        with pytest.raises(ValueError):
            loader_set.add("dc1", "rack1", count=0)

    def test_add_persists_loaders_started_before_a_later_failure(self, monkeypatch):
        """A later loader in a batch failing must not strand earlier ones unpersisted."""
        calls = {"n": 0}
        real_init = Loader.__init__

        def flaky_start(self):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("boom")

        monkeypatch.setattr(Loader, "start", flaky_start)
        cluster = make_cluster()
        loader_set = LoaderSet(cluster)
        with pytest.raises(RuntimeError, match="boom"):
            loader_set.add("dc1", "rack1", count=3)
        assert len(loader_set.loaders) == 1
        assert cluster.config_writes == 1

    def test_unknown_rack_rejected(self, monkeypatch):
        cluster = make_cluster()
        loader_set = LoaderSet(cluster)
        with pytest.raises(RuntimeError, match="Unknown rack"):
            loader_set.add("dc1", "no-such-rack", count=1)

    def test_keyspace_index_out_of_range_rejected(self):
        cluster = make_cluster()
        loader_set = LoaderSet(cluster)
        with pytest.raises(ValueError, match="keyspace_index"):
            loader_set.add("dc1", "rack1", count=1, keyspace_index=5)

    def test_table_index_out_of_range_rejected(self):
        cluster = make_cluster()
        loader_set = LoaderSet(cluster)
        with pytest.raises(ValueError, match="table_index"):
            loader_set.add("dc1", "rack1", count=1, table_index=5)

    def test_bad_lb_policy_rejected_before_container_start(self):
        cluster = make_cluster()
        loader_set = LoaderSet(cluster)
        with pytest.raises(ValueError):
            loader_set.add("dc1", "rack1", count=1, lb_policy="bogus")


class TestSchemaConfig:
    def test_defaults(self):
        cluster = make_cluster()
        loader_set = LoaderSet(cluster)
        assert loader_set.schema_config == DEFAULT_SCHEMA_CONFIG

    def test_configure_schema_updates_fields(self):
        cluster = make_cluster()
        loader_set = LoaderSet(cluster)
        loader_set.configure_schema(keyspaces=3, tables_per_keyspace=2)
        assert loader_set.schema_config == {
            "keyspaces": 3, "tables_per_keyspace": 2, "replication_factor": 3,
        }
        assert cluster.config_writes == 1

    def test_configure_schema_rejects_non_positive(self):
        cluster = make_cluster()
        loader_set = LoaderSet(cluster)
        with pytest.raises(ValueError):
            loader_set.configure_schema(keyspaces=0)


class TestLatteCommand:
    def test_target_ips_scoped_by_lb_policy(self):
        cluster = make_cluster(racks=(("dc1", "rack1", 1), ("dc1", "rack2", 1), ("dc2", "rack1", 1)))
        loader = Loader(cluster, "l1", "dc1", "rack1", "10.89.1.150", "net", "img")
        assert loader.lb_policy == "local-rack"
        rack_ips = loader._target_ips()
        loader.lb_policy = "local-dc"
        dc_ips = loader._target_ips()
        loader.lb_policy = "whole-cluster"
        cluster_ips = loader._target_ips()
        assert len(rack_ips) == 1 < len(dc_ips) == 2 < len(cluster_ips) == 3

    def test_run_cmd_uses_workload_and_schema_knobs(self):
        cluster = make_cluster()
        loader = Loader(cluster, "l1", "dc1", "rack1", "10.89.1.150", "net", "img",
                         workload_mode="write", keyspace_index=0, table_index=0)
        run_cmd = loader._run_cmd()
        assert run_cmd[:3] == ["latte", "run", RUNE_SCRIPT_CONTAINER_PATH]
        assert run_cmd[3] == ",".join(loader._target_ips())
        assert "-f" in run_cmd and "write" in run_cmd and "read" not in run_cmd

    def test_schema_cmd_uses_replication_factor(self):
        cluster = make_cluster()
        loader = Loader(cluster, "l1", "dc1", "rack1", "10.89.1.150", "net", "img",
                         schema_config={"keyspaces": 1, "tables_per_keyspace": 1, "replication_factor": 5})
        assert "replication_factor=5" in loader._schema_cmd()


class TestPersistence:
    def test_to_dict_from_dict_round_trip(self):
        cluster = make_cluster()
        loader_set = LoaderSet(cluster)
        loader_set.configure_schema(keyspaces=2)
        loader = Loader(cluster, "loader-dc1-rack1-150", "dc1", "rack1", "10.0.1.150",
                         "net", "img", lb_policy="local-dc", workload_mode="write",
                         keyspace_index=1, table_index=0)
        loader_set.loaders[loader.name] = loader

        data = loader_set.to_dict()
        restored = LoaderSet.from_dict(cluster, data)

        assert restored.schema_config == loader_set.schema_config
        assert set(restored.loaders) == {"loader-dc1-rack1-150"}
        restored_loader = restored.loaders["loader-dc1-rack1-150"]
        assert restored_loader.lb_policy == "local-dc"
        assert restored_loader.workload_mode == "write"
        assert restored_loader.keyspace_index == 1

    def test_from_dict_empty(self):
        cluster = make_cluster()
        restored = LoaderSet.from_dict(cluster, {})
        assert restored.loaders == {}
        assert restored.schema_config == DEFAULT_SCHEMA_CONFIG


class TestWorkloadFailureSurfaces:
    @pytest.mark.parametrize("schema, run, expected_rc", [("false", "true", 1), ("true", "exit 3", 3)])
    def test_workload_failure_makes_pid1_exit_with_its_rc(self, tmp_path, schema, run, expected_rc):
        exit_file = str(tmp_path / "exit")
        pid1 = subprocess.Popen(["sh", "-c", _pid1_script(exit_file)])
        subprocess.run(["sh", "-c", _workload_script(schema, f"sh -c '{run}'", exit_file)], check=True, timeout=10)
        assert pid1.wait(timeout=10) == expected_rc


if __name__ == "__main__":
    # ponytail smoke check: run without pytest if invoked directly.
    cluster = make_cluster()
    loader_set = LoaderSet(cluster)
    loader_set.configure_schema(keyspaces=2, tables_per_keyspace=2)
    loader = Loader(cluster, "l1", "dc1", "rack1", "10.0.1.150", "net", "img")
    loader_set.loaders[loader.name] = loader
    restored = LoaderSet.from_dict(cluster, loader_set.to_dict())
    assert restored.schema_config == loader_set.schema_config
    assert "l1" in restored.loaders
    print("scylla_loaders smoke check OK")
