import os
import signal

import pytest
from unittest import mock

from ruamel.yaml import YAML

from ccmlib import cluster_factory  # noqa: F401  (import before ScyllaCluster gets patched)
from ccmlib import common
from ccmlib.node import NodeError
from ccmlib.scylla_cluster import ScyllaManager


def _manager(tmp_path):
    (tmp_path / common.SCYLLAMANAGER_CONF).write_text("database:\n  hosts: [x]\n")
    cluster = mock.Mock()
    cluster.get_node_ip.return_value = "127.0.1.1"
    with mock.patch.object(ScyllaManager, "_update_pid"):
        manager = ScyllaManager(cluster)
    manager._get_path = lambda: str(tmp_path)
    return manager


def _database_section(tmp_path):
    with open(tmp_path / common.SCYLLAMANAGER_CONF) as f:
        return YAML().load(f)["database"]


def _update_config(manager):
    with mock.patch.object(ScyllaManager, "version", new_callable=mock.PropertyMock, return_value=mock.MagicMock(__lt__=lambda *_: False)):
        manager._update_config()


def test_manager_db_defaults_to_node1(tmp_path):
    manager = _manager(tmp_path)
    _update_config(manager)
    assert _database_section(tmp_path) == {"hosts": ["127.0.1.1"], "replication_factor": 3}


def test_manager_db_can_be_a_separate_cluster(tmp_path):
    manager = _manager(tmp_path)
    manager.db_hosts = ["127.0.9.1"]
    _update_config(manager)
    assert _database_section(tmp_path) == {"hosts": ["127.0.9.1"], "replication_factor": 1}


def test_manager_db_replication_factor_follows_host_count(tmp_path):
    manager = _manager(tmp_path)
    manager.db_hosts = ["127.0.9.1", "127.0.9.2", "127.0.9.3", "127.0.9.4"]
    _update_config(manager)
    assert _database_section(tmp_path)["replication_factor"] == 3



# --- manager_own_db lifecycle, with a mocked helper cluster ---

def _own_db_manager(tmp_path, own_db=True):
    manager = _manager(tmp_path)
    manager.scylla_cluster.manager_own_db = own_db
    manager.scylla_cluster.manager_db_install_dir = None
    manager.scylla_cluster.is_scylla_reloc.return_value = False
    # Source-tree layout: get_install_dir() is the root, the mode lives in build/<mode>.
    manager.scylla_cluster.get_install_dir.return_value = "/x/scylla"
    manager.scylla_cluster.scylla_mode = "dev"
    manager.scylla_cluster.get_path.return_value = str(tmp_path)
    manager.scylla_cluster.get_ipprefix.return_value = "127.0.5."
    return manager


class _Env:
    """Patches the helper cluster class and everything that spawns a process; records call order."""

    def __init__(self, manager):
        self.events = []
        self.node = mock.Mock()
        self.node.address.return_value = "127.0.5.201"
        self.node.is_running.return_value = False
        self.db = mock.Mock()
        self.db.nodelist.return_value = [self.node]
        self.db.start.side_effect = lambda **kw: (self.events.append("db_start"), setattr(self.node.is_running, "return_value", True))
        self.db.stop.side_effect = lambda **kw: (self.events.append("db_stop"), setattr(self.node.is_running, "return_value", False))
        self.cluster_cls = mock.Mock(return_value=self.db)
        self.popen = mock.Mock(side_effect=lambda *a, **kw: self.events.append("manager_spawn") or mock.Mock(pid=1234))
        self.manager = manager
        manager._update_config = lambda *a: self.events.append(("update_config", list(manager.db_hosts or [])))

    def __enter__(self):
        self.patches = [
            mock.patch("ccmlib.scylla_cluster.ScyllaCluster", self.cluster_cls),
            mock.patch("ccmlib.scylla_cluster.subprocess.Popen", self.popen),
            mock.patch("ccmlib.scylla_cluster.common.check_socket_listening", return_value=True),
            mock.patch.object(ScyllaManager, "_get_api_address", return_value="127.0.5.1"),
        ]
        for p in self.patches:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in self.patches:
            p.stop()


def test_own_db_starts_helper_before_manager(tmp_path):
    manager = _own_db_manager(tmp_path)
    with _Env(manager) as env:
        manager.start()
    assert env.events == ["db_start", ("update_config", ["127.0.5.201"]), "manager_spawn"]
    assert manager.db_hosts == ["127.0.5.201"]
    args, kwargs = env.cluster_cls.call_args
    assert args[1] == "manager-db" and kwargs["install_dir"] == "/x/scylla/build/dev"
    env.db.populate.assert_called_once_with(1, ipprefix="127.0.5.", ipformat="127.0.5.20%d")
    env.db.set_configuration_options.assert_called_once_with({"auto_snapshot": False})
    env.node.set_smp.assert_called_once_with(1)
    env.db.start.assert_called_once_with(wait_for_binary_proto=True)


def test_db_cluster_accessor(tmp_path):
    manager = _own_db_manager(tmp_path)
    assert manager.db_cluster is None
    with _Env(manager) as env:
        manager.start()
        assert manager.db_cluster is env.db


def test_own_db_mode_is_right_before_populate(tmp_path):
    # populate() hard-links build/<mode>/scylla, so the install_dir must already imply the mode.
    manager = _own_db_manager(tmp_path)
    manager.scylla_cluster.scylla_mode = "debug"
    with _Env(manager) as env:
        manager.start()
    install_dir = env.cluster_cls.call_args.kwargs["install_dir"]
    root, mode = common.scylla_extract_install_dir_and_mode(install_dir)
    assert (os.path.abspath(root), mode) == ("/x/scylla", "debug")


def test_own_db_reloc_reuses_resolved_dir_without_version_setup(tmp_path):
    manager = _own_db_manager(tmp_path)
    manager.scylla_cluster.is_scylla_reloc.return_value = True
    manager.scylla_cluster.get_install_dir.return_value = "/repo/unstable/master/1"
    manager.scylla_cluster.scylla_version = "unstable/master:latest"
    with _Env(manager) as env:
        manager.start()
    # A cassandra_version kwarg would re-run scylla_repository.setup() (network).
    assert env.cluster_cls.call_args.kwargs == {"install_dir": "/repo/unstable/master/1"}
    assert (env.db.scylla_reloc, env.db.scylla_version) == (True, "unstable/master:latest")


def test_own_db_install_dir_override(tmp_path):
    manager = _own_db_manager(tmp_path)
    manager.scylla_cluster.manager_db_install_dir = "/other/scylla"
    with _Env(manager) as env:
        manager.start()
    assert env.cluster_cls.call_args.kwargs["install_dir"] == "/other/scylla"


def test_own_db_is_idempotent(tmp_path):
    manager = _own_db_manager(tmp_path)
    with _Env(manager) as env:
        manager.start()
        manager.stop(gently=True)
        manager._pid = None
        manager.start()
    assert env.cluster_cls.call_count == 1
    assert env.db.populate.call_count == 1
    assert env.db.start.call_count == 1


def test_own_db_restarted_on_same_dir_after_cluster_stop(tmp_path):
    manager = _own_db_manager(tmp_path)
    with _Env(manager) as env:
        manager.start()
        manager.stop_db()
        manager.start()
    assert env.cluster_cls.call_count == 1
    assert env.db.populate.call_count == 1
    assert env.db.start.call_count == 2


def test_cluster_stop_stops_helper_but_stop_scylla_manager_does_not(tmp_path):
    from ccmlib.scylla_cluster import ScyllaCluster
    manager = _own_db_manager(tmp_path)
    cluster = mock.Mock(_scylla_manager=manager, skip_manager_server=False)
    with _Env(manager) as env:
        manager.start()
        ScyllaCluster.stop_scylla_manager(cluster)
        assert env.db.stop.call_count == 0
        with mock.patch.object(ScyllaCluster, "stop_nodes", return_value=[]):
            ScyllaCluster.stop(cluster)
    assert env.db.stop.call_count == 1


def test_cluster_stop_without_helper_is_fine(tmp_path):
    from ccmlib.scylla_cluster import ScyllaCluster
    manager = _own_db_manager(tmp_path)
    cluster = mock.Mock(_scylla_manager=manager, skip_manager_server=False)
    with _Env(manager) as env, mock.patch.object(ScyllaCluster, "stop_nodes", return_value=[]):
        ScyllaCluster.stop(cluster)
    assert env.cluster_cls.call_count == 0


def test_failed_manager_start_stops_helper(tmp_path):
    manager = _own_db_manager(tmp_path)
    with _Env(manager) as env:
        env.popen.side_effect = OSError("boom")
        with pytest.raises(OSError):
            manager.start()
    assert env.events == ["db_start", ("update_config", ["127.0.5.201"]), "db_stop"]


def test_failed_helper_start_stops_helper_and_skips_manager(tmp_path):
    manager = _own_db_manager(tmp_path)
    with _Env(manager) as env:
        env.db.start.side_effect = RuntimeError("db down")
        with pytest.raises(RuntimeError):
            manager.start()
    assert env.popen.call_count == 0
    assert env.db.stop.call_count == 1


def test_install_dir_override_keeps_helper_mode_and_timeouts(tmp_path):
    manager = _own_db_manager(tmp_path)
    manager.scylla_cluster.scylla_mode = "debug"
    manager.scylla_cluster.default_wait_for_binary_proto = 999
    manager.scylla_cluster.manager_db_install_dir = "/other/scylla"
    with _Env(manager) as env:
        env.db.scylla_mode = "release"
        env.db.default_wait_for_binary_proto = 60
        manager.start()
    assert (env.db.scylla_mode, env.db.default_wait_for_binary_proto) == ("release", 60)


def test_no_override_copies_managed_mode_and_timeouts(tmp_path):
    manager = _own_db_manager(tmp_path)
    manager.scylla_cluster.scylla_mode = "debug"
    manager.scylla_cluster.default_wait_for_binary_proto = 999
    with _Env(manager) as env:
        manager.start()
    assert (env.db.scylla_mode, env.db.default_wait_for_binary_proto) == ("debug", 999)


def _scylla_cluster(tmp_path, name="c1", **kw):
    from ccmlib.scylla_cluster import ScyllaCluster
    with mock.patch("ccmlib.cluster.Cluster.__init__", return_value=None), \
         mock.patch.object(ScyllaCluster, "get_path", return_value=str(tmp_path)), \
         mock.patch.object(ScyllaCluster, "debug"), \
         mock.patch.object(ScyllaCluster, "_ScyllaCluster__set_default_timeouts"), \
         mock.patch.object(ScyllaCluster, "default_wait_other_notice_timeout", 0, create=True), \
         mock.patch.object(ScyllaCluster, "default_wait_for_binary_proto", 0, create=True):
        return ScyllaCluster(str(tmp_path), name, install_dir=str(tmp_path), cassandra_version="x", **kw)


def test_own_db_options_default_off(tmp_path):
    cluster = _scylla_cluster(tmp_path)
    assert (cluster.manager_own_db, cluster.manager_db_install_dir) == (False, None)


def _written_conf(tmp_path, **kw):
    from ccmlib.scylla_cluster import ScyllaCluster
    cluster = _scylla_cluster(tmp_path, **kw)

    def fake_super_update(self, install_dir=None):
        with open(tmp_path / "cluster.conf", "w") as f:
            YAML().dump({"name": "c1"}, f)

    with mock.patch("ccmlib.cluster.Cluster._update_config", fake_super_update), \
         mock.patch.object(ScyllaCluster, "get_path", return_value=str(tmp_path)):
        cluster._update_config()
    with open(tmp_path / "cluster.conf") as f:
        return YAML().load(f)


def test_own_db_options_are_persisted(tmp_path):
    data = _written_conf(tmp_path, manager_own_db=True, manager_db_install_dir="/other")
    assert data["manager_own_db"] is True and data["manager_db_install_dir"] == "/other"


def test_own_db_options_not_persisted_when_off(tmp_path):
    data = _written_conf(tmp_path)
    assert "manager_own_db" not in data and "manager_db_install_dir" not in data


def test_cluster_factory_restores_own_db_options(tmp_path):
    from ccmlib.cluster_factory import ClusterFactory
    (tmp_path / "c1").mkdir()
    conf = {"name": "c1", "install_dir": "/x", "nodes": [], "seeds": [],
            "manager_own_db": True, "manager_db_install_dir": "/other"}
    with open(tmp_path / "c1" / "cluster.conf", "w") as f:
        YAML().dump(conf, f)
    with mock.patch("ccmlib.cluster_factory.repository.validate"), \
         mock.patch("ccmlib.cluster_factory.common.isScylla", return_value=True), \
         mock.patch("ccmlib.cluster_factory.ScyllaCluster") as cls:
        ClusterFactory.load(str(tmp_path), "c1")
    assert cls.call_args.kwargs["manager_own_db"] is True
    assert cls.call_args.kwargs["manager_db_install_dir"] == "/other"


def test_helper_named_cluster_never_gets_a_manager(tmp_path, monkeypatch):
    monkeypatch.setenv("SCYLLA_EXT_OPTS", "--scylla-manager=/mgr")
    with mock.patch("ccmlib.scylla_cluster.ScyllaManager") as mgr:
        assert _scylla_cluster(tmp_path, name="manager-db")._scylla_manager is None
        assert mgr.call_count == 0
        assert _scylla_cluster(tmp_path, name="c1")._scylla_manager is mgr.return_value


def test_load_from_disk_reuses_helper(tmp_path):
    manager = _own_db_manager(tmp_path)
    (tmp_path / "manager-db").mkdir()
    (tmp_path / "manager-db" / "cluster.conf").write_text("x")
    with _Env(manager) as env, mock.patch("ccmlib.cluster_factory.ClusterFactory.load", return_value=env.db) as load:
        manager.start()
    load.assert_called_once_with(str(tmp_path), "manager-db")
    assert env.cluster_cls.call_count == 0
    assert env.db.populate.call_count == 0
    env.node.set_smp.assert_called_once_with(1)
    assert manager.db_hosts == ["127.0.5.201"]


def test_failed_creation_removes_half_created_dir(tmp_path):
    manager = _own_db_manager(tmp_path)
    with _Env(manager) as env:
        env.cluster_cls.side_effect = lambda *a, **kw: (tmp_path / "manager-db").mkdir() or env.db
        env.db.populate.side_effect = RuntimeError("populate failed")
        with pytest.raises(RuntimeError):
            manager.start()
    assert not (tmp_path / "manager-db").exists()


def test_helper_without_nodes_gives_clear_error(tmp_path):
    manager = _own_db_manager(tmp_path)
    (tmp_path / "manager-db").mkdir()
    (tmp_path / "manager-db" / "cluster.conf").write_text("x")
    with _Env(manager) as env, mock.patch("ccmlib.cluster_factory.ClusterFactory.load", return_value=env.db):
        env.db.nodelist.return_value = []
        with pytest.raises(NodeError, match="no nodes"):
            manager.start()


def test_docker_cluster_is_rejected(tmp_path):
    manager = _own_db_manager(tmp_path)
    manager.scylla_cluster.get_install_dir.return_value = None
    with _Env(manager), pytest.raises(NodeError, match="docker"):
        manager.start()


def test_failed_manager_start_stops_manager_and_cleanup_errors_do_not_mask(tmp_path):
    manager = _own_db_manager(tmp_path)
    with _Env(manager) as env, mock.patch.object(ScyllaManager, "stop") as stop, \
            mock.patch("ccmlib.scylla_cluster.common.check_socket_listening", return_value=False):
        env.db.stop.side_effect = RuntimeError("stop failed")
        with pytest.raises(Exception, match="not listening"):
            manager.start()
    stop.assert_called_once_with(gently=False)


def test_failed_start_stops_db_even_if_manager_stop_raises(tmp_path):
    manager = _own_db_manager(tmp_path)
    with _Env(manager) as env, mock.patch.object(ScyllaManager, "stop", side_effect=RuntimeError("kill failed")), \
            mock.patch("ccmlib.scylla_cluster.common.check_socket_listening", return_value=False):
        with pytest.raises(Exception, match="not listening"):
            manager.start()
    assert env.db.stop.call_count == 1


def test_failed_start_does_not_signal_a_stale_pid(tmp_path):
    # After a reload _pid comes from an old pidfile; it may belong to an unrelated process.
    manager = _own_db_manager(tmp_path)
    manager._pid = 99999
    with _Env(manager) as env, mock.patch("ccmlib.scylla_cluster.os.kill") as kill:
        env.db.start.side_effect = RuntimeError("db down")
        with pytest.raises(RuntimeError):
            manager.start()
    assert kill.call_count == 0
    assert env.db.stop.call_count == 1


def test_cluster_stop_order_and_errors_never_skip_nodes(tmp_path):
    from ccmlib.scylla_cluster import ScyllaCluster
    manager = mock.Mock()
    cluster = mock.Mock(_scylla_manager=manager, skip_manager_server=False)
    order = mock.Mock()
    order.attach_mock(manager.stop, "manager_stop")
    order.attach_mock(manager.stop_db, "db_stop")
    ScyllaCluster.stop(cluster)
    assert [c[0] for c in order.mock_calls] == ["manager_stop", "db_stop"]
    cluster.stop_nodes.reset_mock()
    manager.stop_db.side_effect = RuntimeError("db stop failed")
    manager.stop.side_effect = RuntimeError("mgr stop failed")
    with pytest.raises(RuntimeError, match="mgr stop failed"):
        ScyllaCluster.stop(cluster)
    assert cluster.stop_nodes.call_count == 1 and manager.stop_db.call_count == 2


def test_clear_also_clears_helper_data(tmp_path):
    cluster = _scylla_cluster(tmp_path)
    cluster._scylla_manager = mock.Mock()
    with mock.patch("ccmlib.cluster.Cluster.clear") as base_clear:
        cluster.clear()
    base_clear.assert_called_once()
    cluster._scylla_manager.db_cluster.clear.assert_called_once_with()


def test_clear_without_helper_is_fine(tmp_path):
    cluster = _scylla_cluster(tmp_path)
    cluster._scylla_manager = mock.Mock(db_cluster=None)
    with mock.patch("ccmlib.cluster.Cluster.clear"):
        cluster.clear()


def test_flag_off_creates_no_helper(tmp_path):
    manager = _own_db_manager(tmp_path, own_db=False)
    with _Env(manager) as env:
        manager.start()
    assert env.cluster_cls.call_count == 0
    assert manager.db_hosts is None
    assert env.events == [("update_config", []), "manager_spawn"]


def test_leftover_helper_dir_without_conf_does_not_break_creation(tmp_path):
    manager = _own_db_manager(tmp_path)
    leftover = tmp_path / "manager-db"
    (leftover / "node1").mkdir(parents=True)

    def create(*a, **kw):
        leftover.mkdir()  # the real ScyllaCluster ctor fails if the dir exists
        return env.db
    with _Env(manager) as env:
        env.cluster_cls.side_effect = create
        manager.start()
    assert env.db.start.call_count == 1 and not (leftover / "node1").exists()


def test_failure_before_creation_keeps_existing_dir(tmp_path):
    manager = _own_db_manager(tmp_path)
    manager.scylla_cluster.get_install_dir.return_value = None
    (tmp_path / "manager-db").mkdir()
    with _Env(manager), pytest.raises(NodeError, match="docker"):
        manager.start()
    assert (tmp_path / "manager-db").exists()


def test_docker_cluster_is_rejected_even_with_install_dir_override(tmp_path):
    manager = _own_db_manager(tmp_path)
    manager.scylla_cluster.get_install_dir.return_value = None
    manager.scylla_cluster.manager_db_install_dir = "/other/scylla"
    with _Env(manager) as env, pytest.raises(NodeError, match="docker"):
        manager.start()
    assert env.cluster_cls.call_count == 0


def test_stop_db_with_unloadable_helper_kills_by_pidfile(tmp_path):
    manager = _own_db_manager(tmp_path)
    node_dir = tmp_path / "manager-db" / "node1"
    node_dir.mkdir(parents=True)
    (tmp_path / "manager-db" / "cluster.conf").write_text("garbage")
    (node_dir / "cassandra.pid").write_text("4242")
    proc = mock.Mock()
    proc.cmdline.return_value = ["scylla", "--workdir", str(node_dir)]
    with mock.patch("ccmlib.cluster_factory.ClusterFactory.load", side_effect=KeyError("nodes")), \
            mock.patch("ccmlib.scylla_cluster.psutil.Process", return_value=proc), \
            mock.patch("ccmlib.scylla_cluster.os.kill") as kill:
        manager.stop_db(gently=False)
    kill.assert_called_once_with(4242, signal.SIGKILL)


def test_stop_db_with_unloadable_helper_ignores_stale_pidfile(tmp_path):
    manager = _own_db_manager(tmp_path)
    node_dir = tmp_path / "manager-db" / "node1"
    node_dir.mkdir(parents=True)
    (tmp_path / "manager-db" / "cluster.conf").write_text("garbage")
    (node_dir / "cassandra.pid").write_text("4242")
    proc = mock.Mock()
    proc.cmdline.return_value = ["firefox"]
    with mock.patch("ccmlib.cluster_factory.ClusterFactory.load", side_effect=KeyError("nodes")), \
            mock.patch("ccmlib.scylla_cluster.psutil.Process", return_value=proc), \
            mock.patch("ccmlib.scylla_cluster.os.kill") as kill:
        manager.stop_db()
    assert kill.call_count == 0


def test_install_dir_override_is_made_absolute(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cluster = _scylla_cluster(tmp_path, manager_own_db=True, manager_db_install_dir="rel/scylla")
    assert cluster.manager_db_install_dir == str(tmp_path / "rel" / "scylla")


# --- real ScyllaCluster/populate/ClusterFactory round trip, no processes ---

def _fake_source_tree(root):
    (root / "build" / "dev").mkdir(parents=True)
    (root / "conf").mkdir()
    (root / "build" / "dev" / "scylla").write_text("x")
    (root / "SCYLLA-VERSION-FILE").write_text("2026.1.0\n")
    (root / "conf" / "scylla.yaml").write_text(
        "cluster_name: x\nseed_provider:\n"
        "  - class_name: org.apache.cassandra.locator.SimpleSeedProvider\n"
        "    parameters:\n      - seeds: 127.0.0.1\n")
    return root


def test_helper_round_trip_through_real_cluster_and_factory(tmp_path):
    src = _fake_source_tree(tmp_path / "src")
    path = tmp_path / "clusters" / "c1"
    path.mkdir(parents=True)
    cluster = mock.Mock()
    cluster.get_path.return_value = str(path)
    cluster.get_install_dir.return_value = str(src)
    cluster.is_scylla_reloc.return_value = False
    cluster.scylla_mode = "dev"
    cluster.manager_db_install_dir = None
    cluster.get_ipprefix.return_value = "127.0.5."
    cluster.default_wait_for_binary_proto = 420
    cluster.manager_own_db = True
    with mock.patch.object(ScyllaManager, "_update_pid"):
        ScyllaManager(cluster)._get_db_cluster(create=True)
        loaded = ScyllaManager(cluster)._get_db_cluster(create=False)
    assert loaded.name == "manager-db"
    assert loaded.scylla_mode == "dev" and loaded.scylla_reloc is False
    assert os.path.abspath(loaded.get_install_dir()) == str(src)
    assert loaded._scylla_manager is None
    assert loaded._config_options["auto_snapshot"] is False
    (node,) = loaded.nodelist()
    assert node.smp() == 1 and node.address() == "127.0.5.201"


def test_old_cluster_conf_without_own_db_keys_loads_with_defaults(tmp_path):
    from ccmlib.cluster_factory import ClusterFactory
    from ccmlib.scylla_cluster import ScyllaCluster
    src = _fake_source_tree(tmp_path / "src")
    ScyllaCluster(str(tmp_path), "old", install_dir=str(src / "build" / "dev")).populate(1)
    conf = tmp_path / "old" / "cluster.conf"
    with open(conf) as f:
        data = YAML().load(f)
    data.pop("manager_own_db", None)
    data.pop("manager_db_install_dir", None)
    with open(conf, "w") as f:
        YAML().dump(data, f)
    loaded = ClusterFactory.load(str(tmp_path), "old")
    assert (loaded.manager_own_db, loaded.manager_db_install_dir) == (False, None)
