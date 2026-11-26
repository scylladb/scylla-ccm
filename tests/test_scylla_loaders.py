"""Tests for the loaders integration (Loader, LoaderSet) -- pure logic only,
no live podman: IP/name allocation, schema/workload knobs, persistence round-trip."""
import os
import subprocess
from collections import OrderedDict

import pytest

from ccmlib.common import ArgumentError
from ccmlib.scylla_loaders import (
    Loader, LoaderSet, LB_POLICIES, _pid1_script, _workload_script, WORKLOAD_MODES, DEFAULT_SCHEMA_CONFIG, RUNE_SCRIPT_CONTAINER_PATH,
)
from ccmlib.scylla_podman_cluster import PodmanNetworkTopology


@pytest.fixture
def ClusterLoadersCmd():
    # Importing cluster_cmds sets SCYLLA_CCM_STANDALONE, which disables node reaping; undo it.
    saved = os.environ.get('SCYLLA_CCM_STANDALONE')
    from ccmlib.cmds.cluster_cmds import ClusterLoadersCmd
    if saved is None:
        os.environ.pop('SCYLLA_CCM_STANDALONE', None)
    yield ClusterLoadersCmd


class TestParseRacks:
    def test_malformed_count_raises_argument_error(self, ClusterLoadersCmd):
        with pytest.raises(ArgumentError):
            ClusterLoadersCmd._parse_racks("dc1:rack1=abc")

    def test_zero_count_rejected(self, ClusterLoadersCmd):
        with pytest.raises(ArgumentError):
            ClusterLoadersCmd._parse_racks("dc1:rack1=0")

    def test_negative_count_rejected(self, ClusterLoadersCmd):
        with pytest.raises(ArgumentError):
            ClusterLoadersCmd._parse_racks("dc1:rack1=-1")

    def test_valid_spec_parses(self, ClusterLoadersCmd):
        assert ClusterLoadersCmd._parse_racks("dc1:rack1=5") == [("dc1", "rack1", 5)]


class FakeCluster:
    """Just enough of Cluster/ScyllaPodmanCluster for LoaderSet's pure logic."""

    def __init__(self, topology):
        self.name = "test"
        self.network_topology = topology
        self.nodes = dict.fromkeys(topology.node_assignments)
        self.config_writes = 0

    def _update_config(self):
        self.config_writes += 1

    def network_cluster_name(self):
        return f"ccmdir-{self.name}"


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


    def test_container_name_includes_ccm_dir(self):
        loader = Loader(make_cluster(), "loader-dc1-rack1-150", "dc1", "rack1", "10.0.1.150", "net", "img")
        assert loader.container_name() == "ccmdir-test-loader-dc1-rack1-150"


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

    def test_remove_keeps_and_reports_loaders_that_fail_to_stop(self, monkeypatch):
        monkeypatch.setattr(Loader, "start", lambda self: None)
        cluster = make_cluster()
        loader_set = LoaderSet(cluster)
        bad, good = loader_set.add("dc1", "rack1", count=2)

        def stop(self):
            if self is bad:
                raise RuntimeError("rm failed")

        monkeypatch.setattr(Loader, "stop", stop)
        assert loader_set.remove() == [bad.name]
        assert list(loader_set.loaders) == [bad.name]

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
            "keyspaces": 3, "tables_per_keyspace": 2, "replication_factor": None,
        }
        assert cluster.config_writes == 1

    def test_configure_schema_rejects_non_positive(self):
        cluster = make_cluster()
        loader_set = LoaderSet(cluster)
        with pytest.raises(ValueError):
            loader_set.configure_schema(keyspaces=0)

    def test_configure_schema_refuses_to_orphan_existing_loaders(self, monkeypatch):
        monkeypatch.setattr(Loader, "start", lambda self: None)
        cluster = make_cluster()
        loader_set = LoaderSet(cluster)
        loader_set.configure_schema(keyspaces=2)
        loader_set.add("dc1", "rack1", count=1, keyspace_index=1)
        with pytest.raises(ValueError, match="keyspaces=1"):
            loader_set.configure_schema(keyspaces=1)
        assert loader_set.schema_config["keyspaces"] == 2


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
        assert run_cmd[run_cmd.index("--") + 1:] == loader._target_ips()
        assert "-f" in run_cmd and "write" in run_cmd and "read" not in run_cmd
        # Unquoted, latte parses ks0/tbl0 as rune variable names and fails to load.
        assert 'keyspace="ks0"' in run_cmd and 'table="tbl0"' in run_cmd

    def test_schema_cmd_uses_replication_factor(self):
        cluster = make_cluster()
        loader = Loader(cluster, "l1", "dc1", "rack1", "10.89.1.150", "net", "img",
                         schema_config={"keyspaces": 1, "tables_per_keyspace": 1, "replication_factor": 5})
        assert "replication_factor=5" in loader._schema_cmd()

    @pytest.mark.parametrize("racks, expected", [
        ((("dc1", "rack1", 1),), 1),
        ((("dc1", "rack1", 2), ("dc1", "rack2", 3)), 3),
        ((("dc1", "rack1", 4), ("dc2", "rack1", 2)), 2),
    ])
    def test_default_replication_factor_fits_smallest_dc(self, racks, expected):
        loader = Loader(make_cluster(racks=racks), "l1", "dc1", "rack1", "10.89.1.150", "net", "img")
        assert f"replication_factor={expected}" in loader._schema_cmd()

    def test_removed_node_not_counted(self):
        """The topology keeps removed nodes; RF and contact points must not."""
        cluster = make_cluster(racks=(("dc1", "rack1", 3),))
        removed_ip = cluster.network_topology.node_assignments["node3"]["ip"]
        del cluster.nodes["node3"]
        loader = Loader(cluster, "l1", "dc1", "rack1", "10.89.1.150", "net", "img")
        assert "replication_factor=2" in loader._schema_cmd()
        assert removed_ip not in loader._target_ips()

    @pytest.mark.parametrize("policy, expected", [
        ("local-rack", ["--datacenter", "dc1", "--rack", "rack1"]),
        ("local-dc", ["--datacenter", "dc1"]),
        ("whole-cluster", []),
    ])
    def test_lb_policy_sets_driver_locality(self, policy, expected):
        loader = Loader(make_cluster(), "l1", "dc1", "rack1", "10.89.1.150", "net", "img", lb_policy=policy)
        for cmd in (loader._run_cmd(), loader._schema_cmd()):
            opts = cmd[:cmd.index("--")]
            assert [a for i, a in enumerate(opts) if a in ("--datacenter", "--rack") or
                    (i and opts[i - 1] in ("--datacenter", "--rack"))] == expected


class TestLoaderStart:
    def test_start_sets_up_rack_routes_in_one_nsenter_call(self, monkeypatch):
        """Regression: start() called a cluster method that no longer exists."""
        import ccmlib.scylla_podman_cluster as pc
        from unittest.mock import MagicMock
        cluster = make_cluster()
        client = MagicMock()
        client.get_container_status.return_value = "running"
        monkeypatch.setattr(pc, "_get_podman_client", lambda: client)
        monkeypatch.setattr("ccmlib.scylla_loaders._force_remove_loader_container", lambda name: None)
        monkeypatch.setattr(Loader, "_rune_script_copy", lambda self: "/tmp/x.rn")
        for step in ("_apply_tc_rules", "_start_workload", "_start_log_stream"):
            monkeypatch.setattr(Loader, step, lambda self, *a: None)
        nsenter = MagicMock(return_value=MagicMock(returncode=0, stderr=""))
        monkeypatch.setattr(pc, "_nsenter_net_run", nsenter)

        loader = Loader(cluster, "l1", "dc1", "rack1", "10.89.1.150", "net", "img")
        loader.start()

        routes = cluster.network_topology.get_routes_for_node(loader._rack_node_name())
        assert routes
        nsenter.assert_called_once()
        script = nsenter.call_args[0][1][2]
        assert script == " && ".join(f"ip route add {d} via {g}" for d, g in routes)


class TestForceRemove:
    def _patch(self, monkeypatch, inspect, ps):
        import ccmlib.scylla_podman_cluster as pc
        from unittest.mock import MagicMock
        client = MagicMock()
        client.runtime_name = "podman"
        client._run_command.return_value = ps
        monkeypatch.setattr(pc, "_get_podman_client", lambda: client)
        monkeypatch.setattr(pc, "_inspect_container", lambda name: inspect)
        return client

    def test_confirmed_absent_is_a_noop(self, monkeypatch):
        from ccmlib.scylla_loaders import _force_remove_loader_container
        client = self._patch(monkeypatch, None, (0, "", ""))
        _force_remove_loader_container("l1")
        client.remove_container.assert_not_called()

    @pytest.mark.parametrize("ps", [(125, "", "cannot connect"), (0, "l1\n", "")])
    def test_unconfirmed_absence_raises(self, monkeypatch, ps):
        """A failed inspect must not let LoaderSet.remove() forget a running loader."""
        from ccmlib.scylla_loaders import _force_remove_loader_container
        self._patch(monkeypatch, None, ps)
        with pytest.raises(RuntimeError, match="Cannot confirm"):
            _force_remove_loader_container("l1")

    def test_save_full_log_skips_missing_container(self, monkeypatch, tmp_path):
        """`podman logs` on a gone container returns its error text, which must not replace the saved log."""
        client = self._patch(monkeypatch, None, (0, "", ""))
        client.stream_logs.return_value = "Error: no such container l1"
        cluster = make_cluster()
        cluster.get_path = lambda: str(tmp_path)
        (tmp_path / "loaders").mkdir()
        (tmp_path / "loaders" / "l1.log").write_text("why it died")
        Loader(cluster, "l1", "dc1", "rack1", "10.89.1.150", "net", "img")._save_full_log("l1")
        assert (tmp_path / "loaders" / "l1.log").read_text() == "why it died"


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


def test_remove_keep_loaders_preserves_loaders_dir(tmp_path, capsys):
    """Kept loaders bind-mount <cluster>/loaders, which must outlive the cluster dir."""
    from types import SimpleNamespace
    from unittest.mock import patch
    from ccmlib.cluster import Cluster

    with patch.object(Cluster, '__init__', lambda *a, **k: None):
        cluster = Cluster(None, None)
    cluster.path, cluster.name = str(tmp_path), 'test'
    cluster.nodes, cluster.seeds = {}, []
    cluster.monitoring_enabled = False
    cluster.monitoring_stack = None
    cluster.loader_set = SimpleNamespace(
        loaders={'l1': SimpleNamespace(container_name=lambda: 'ccm-test-l1')})
    loaders = tmp_path / 'test' / 'loaders'
    loaders.mkdir(parents=True)
    (loaders / 'w.rn').write_text('x')

    with patch.object(Cluster, 'stop'):
        cluster.remove(keep_loaders=True)

    kept = list((tmp_path / '.kept-loaders').iterdir())
    assert len(kept) == 1 and (kept[0] / 'w.rn').read_text() == 'x'
    assert not (tmp_path / 'test').exists()
    assert 'podman rm -f --depend ccm-test-l1' in capsys.readouterr().err


def test_cluster_remove_keeps_everything_when_a_loader_survives():
    """A loader that failed to stop still uses <cluster>/loaders and the rack networks."""
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from ccmlib.scylla_podman_cluster import ScyllaPodmanCluster

    cluster = object.__new__(ScyllaPodmanCluster)
    cluster.loader_set = SimpleNamespace(remove=lambda: ["l1"], loaders={"l1": None})
    cluster._batch_remove_nodes = MagicMock()
    cluster.network_topology = MagicMock()
    with pytest.raises(RuntimeError, match="loaders"):
        cluster.remove()
    cluster._batch_remove_nodes.assert_not_called()
    cluster.network_topology.destroy_networks.assert_not_called()
