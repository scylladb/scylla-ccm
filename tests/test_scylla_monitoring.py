"""Tests for the scylla-monitoring integration (MonitoringStack, targets, hooks, CLI)."""
import json
import os
import pytest
from unittest.mock import Mock, patch, MagicMock

from ccmlib.scylla_monitoring import MonitoringStack, _resolve_monitoring_dir
from ccmlib.cluster import Cluster
from ccmlib.node import Status


class TestResolveMonitoringDir:
    """Tests for the _resolve_monitoring_dir helper."""

    def test_explicit_dir_exists(self, tmp_path):
        d = str(tmp_path)
        assert _resolve_monitoring_dir(d) == d

    def test_explicit_dir_not_exists(self):
        assert _resolve_monitoring_dir('/nonexistent/path') is None

    def test_env_var(self, tmp_path, monkeypatch):
        d = str(tmp_path)
        monkeypatch.setenv('SCYLLA_MONITORING_DIR', d)
        assert _resolve_monitoring_dir() == d

    def test_env_var_not_exists(self, monkeypatch):
        monkeypatch.setenv('SCYLLA_MONITORING_DIR', '/nonexistent/path')
        monkeypatch.delenv('SCYLLA_MONITORING_DIR', raising=False)
        assert _resolve_monitoring_dir() is None

    def test_default_ccm_dir(self, tmp_path, monkeypatch):
        default_dir = tmp_path / '.ccm' / 'scylla-monitoring'
        default_dir.mkdir(parents=True)
        monkeypatch.delenv('SCYLLA_MONITORING_DIR', raising=False)
        with patch('os.path.expanduser', return_value=str(default_dir)):
            assert _resolve_monitoring_dir() == str(default_dir)

    def test_explicit_takes_priority_over_env(self, tmp_path, monkeypatch):
        explicit = tmp_path / 'explicit'
        explicit.mkdir()
        env_dir = tmp_path / 'env'
        env_dir.mkdir()
        monkeypatch.setenv('SCYLLA_MONITORING_DIR', str(env_dir))
        assert _resolve_monitoring_dir(str(explicit)) == str(explicit)

    def test_none_when_nothing_available(self, monkeypatch):
        monkeypatch.delenv('SCYLLA_MONITORING_DIR', raising=False)
        with patch('os.path.expanduser', return_value='/nonexistent'):
            assert _resolve_monitoring_dir() is None


class TestGenerateTargetsYaml:
    """Tests for MonitoringStack._generate_targets_yaml."""

    def _make_mock_cluster(self, nodes_config):
        """Create a mock cluster with the given node configuration.

        nodes_config: list of (address, status, data_center) tuples
        """
        cluster = Mock()
        cluster.name = 'test_cluster'
        cluster.get_path.return_value = '/tmp/test_cluster'

        mock_nodes = []
        for addr, status, dc in nodes_config:
            node = Mock()
            node.address.return_value = addr
            node.status = status
            node.data_center = dc
            mock_nodes.append(node)

        cluster.nodelist.return_value = mock_nodes
        return cluster

    def test_single_dc_multiple_nodes(self):
        cluster = self._make_mock_cluster([
            ('127.0.0.1', Status.UP, 'dc1'),
            ('127.0.0.2', Status.UP, 'dc1'),
            ('127.0.0.3', Status.UP, 'dc1'),
        ])
        stack = MonitoringStack(cluster, '/fake/monitoring')
        targets = stack._generate_targets_yaml()

        assert len(targets) == 1
        assert targets[0]['labels']['cluster'] == 'test_cluster'
        assert targets[0]['labels']['dc'] == 'dc1'
        assert set(targets[0]['targets']) == {'127.0.0.1', '127.0.0.2', '127.0.0.3'}

    def test_multi_dc(self):
        cluster = self._make_mock_cluster([
            ('127.0.0.1', Status.UP, 'dc1'),
            ('127.0.0.2', Status.UP, 'dc1'),
            ('127.0.0.3', Status.UP, 'dc2'),
        ])
        stack = MonitoringStack(cluster, '/fake/monitoring')
        targets = stack._generate_targets_yaml()

        assert len(targets) == 2
        dc_map = {t['labels']['dc']: t for t in targets}
        assert set(dc_map['dc1']['targets']) == {'127.0.0.1', '127.0.0.2'}
        assert dc_map['dc2']['targets'] == ['127.0.0.3']

    def test_excludes_down_nodes(self):
        cluster = self._make_mock_cluster([
            ('127.0.0.1', Status.UP, 'dc1'),
            ('127.0.0.2', Status.DOWN, 'dc1'),
            ('127.0.0.3', Status.UP, 'dc1'),
        ])
        stack = MonitoringStack(cluster, '/fake/monitoring')
        targets = stack._generate_targets_yaml()

        assert len(targets) == 1
        assert set(targets[0]['targets']) == {'127.0.0.1', '127.0.0.3'}

    def test_excludes_uninitialized_nodes(self):
        cluster = self._make_mock_cluster([
            ('127.0.0.1', Status.UP, 'dc1'),
            ('127.0.0.2', Status.UNINITIALIZED, 'dc1'),
        ])
        stack = MonitoringStack(cluster, '/fake/monitoring')
        targets = stack._generate_targets_yaml()

        assert len(targets) == 1
        assert targets[0]['targets'] == ['127.0.0.1']

    def test_includes_decommissioned_nodes(self):
        cluster = self._make_mock_cluster([
            ('127.0.0.1', Status.UP, 'dc1'),
            ('127.0.0.2', Status.DECOMMISSIONED, 'dc1'),
        ])
        stack = MonitoringStack(cluster, '/fake/monitoring')
        targets = stack._generate_targets_yaml()

        assert len(targets) == 1
        assert set(targets[0]['targets']) == {'127.0.0.1', '127.0.0.2'}

    def test_empty_cluster(self):
        cluster = self._make_mock_cluster([])
        stack = MonitoringStack(cluster, '/fake/monitoring')
        targets = stack._generate_targets_yaml()

        assert targets == []

    def test_all_nodes_down(self):
        cluster = self._make_mock_cluster([
            ('127.0.0.1', Status.DOWN, 'dc1'),
            ('127.0.0.2', Status.DOWN, 'dc1'),
        ])
        stack = MonitoringStack(cluster, '/fake/monitoring')
        targets = stack._generate_targets_yaml()

        assert targets == []

    def test_default_datacenter(self):
        """Nodes with data_center=None should use 'datacenter1'."""
        cluster = self._make_mock_cluster([
            ('127.0.0.1', Status.UP, None),
            ('127.0.0.2', Status.UP, None),
        ])
        stack = MonitoringStack(cluster, '/fake/monitoring')
        targets = stack._generate_targets_yaml()

        assert len(targets) == 1
        assert targets[0]['labels']['dc'] == 'datacenter1'

    def test_mixed_statuses_and_dcs(self):
        cluster = self._make_mock_cluster([
            ('127.0.0.1', Status.UP, 'dc1'),
            ('127.0.0.2', Status.DOWN, 'dc1'),
            ('127.0.0.3', Status.UP, 'dc2'),
            ('127.0.0.4', Status.DECOMMISSIONED, 'dc2'),
            ('127.0.0.5', Status.UNINITIALIZED, 'dc1'),
        ])
        stack = MonitoringStack(cluster, '/fake/monitoring')
        targets = stack._generate_targets_yaml()

        assert len(targets) == 2
        dc_map = {t['labels']['dc']: t for t in targets}
        assert dc_map['dc1']['targets'] == ['127.0.0.1']
        assert set(dc_map['dc2']['targets']) == {'127.0.0.3', '127.0.0.4'}


class TestWriteTargetsFile:
    """Tests for MonitoringStack._write_targets_file."""

    def test_atomic_write(self, tmp_path):
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = str(tmp_path)

        stack = MonitoringStack(cluster, '/fake/monitoring')
        stack._working_dir = str(tmp_path / 'monitoring')

        targets = [
            {"targets": ["127.0.0.1"], "labels": {"cluster": "test", "dc": "dc1"}}
        ]
        stack._write_targets_file(targets)

        targets_file = tmp_path / 'monitoring' / 'prometheus' / 'targets' / 'scylla_servers.yml'
        assert targets_file.exists()

        with open(targets_file) as f:
            data = json.load(f)
        assert data == targets

    def test_overwrite_existing(self, tmp_path):
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = str(tmp_path)

        stack = MonitoringStack(cluster, '/fake/monitoring')
        stack._working_dir = str(tmp_path / 'monitoring')

        # Write initial targets
        initial = [{"targets": ["127.0.0.1"], "labels": {"cluster": "test", "dc": "dc1"}}]
        stack._write_targets_file(initial)

        # Overwrite with updated targets
        updated = [
            {"targets": ["127.0.0.1", "127.0.0.2"], "labels": {"cluster": "test", "dc": "dc1"}}
        ]
        stack._write_targets_file(updated)

        targets_file = tmp_path / 'monitoring' / 'prometheus' / 'targets' / 'scylla_servers.yml'
        with open(targets_file) as f:
            data = json.load(f)
        assert data == updated


class TestUpdateTargets:
    """Tests for MonitoringStack.update_targets (end-to-end generate + write)."""

    def test_update_targets_writes_correct_file(self, tmp_path):
        cluster = Mock()
        cluster.name = 'mytest'
        cluster.get_path.return_value = str(tmp_path)

        node1 = Mock()
        node1.address.return_value = '127.0.0.1'
        node1.status = Status.UP
        node1.data_center = 'dc1'

        node2 = Mock()
        node2.address.return_value = '127.0.0.2'
        node2.status = Status.UP
        node2.data_center = 'dc1'

        cluster.nodelist.return_value = [node1, node2]

        stack = MonitoringStack(cluster, '/fake/monitoring')
        stack._working_dir = str(tmp_path / 'monitoring')

        stack.update_targets()

        targets_file = tmp_path / 'monitoring' / 'prometheus' / 'targets' / 'scylla_servers.yml'
        with open(targets_file) as f:
            data = json.load(f)

        assert len(data) == 1
        assert data[0]['labels']['cluster'] == 'mytest'
        assert data[0]['labels']['dc'] == 'dc1'
        assert set(data[0]['targets']) == {'127.0.0.1', '127.0.0.2'}


class TestNotifyTopologyChange:
    """Tests for the _notify_topology_change hook on Cluster."""

    def test_hook_updates_targets_when_monitoring_active(self):
        with patch.object(Cluster, '__init__', lambda x, *args, **kwargs: None):
            cluster = Cluster(None, None)
            cluster.monitoring_enabled = True
            cluster.monitoring_stack = Mock()
            cluster.monitoring_stack.is_running.return_value = True

            cluster._notify_topology_change()

            cluster.monitoring_stack.update_targets.assert_called_once()

    def test_hook_noop_when_monitoring_disabled(self):
        with patch.object(Cluster, '__init__', lambda x, *args, **kwargs: None):
            cluster = Cluster(None, None)
            cluster.monitoring_enabled = False
            cluster.monitoring_stack = Mock()

            cluster._notify_topology_change()

            cluster.monitoring_stack.update_targets.assert_not_called()

    def test_hook_noop_when_no_stack(self):
        with patch.object(Cluster, '__init__', lambda x, *args, **kwargs: None):
            cluster = Cluster(None, None)
            cluster.monitoring_enabled = True
            cluster.monitoring_stack = None

            # Should not raise
            cluster._notify_topology_change()

    def test_hook_noop_when_stack_not_running(self):
        with patch.object(Cluster, '__init__', lambda x, *args, **kwargs: None):
            cluster = Cluster(None, None)
            cluster.monitoring_enabled = True
            cluster.monitoring_stack = Mock()
            cluster.monitoring_stack.is_running.return_value = False

            cluster._notify_topology_change()

            cluster.monitoring_stack.update_targets.assert_not_called()

    def test_hook_handles_update_failure(self):
        with patch.object(Cluster, '__init__', lambda x, *args, **kwargs: None):
            cluster = Cluster(None, None)
            cluster.monitoring_enabled = True
            cluster.monitoring_stack = Mock()
            cluster.monitoring_stack.is_running.return_value = True
            cluster.monitoring_stack.update_targets.side_effect = RuntimeError("test error")

            # Should not raise, just warn
            cluster._notify_topology_change()


class TestClusterAddHook:
    """Test that Cluster.add() calls _notify_topology_change."""

    def test_add_calls_notify(self):
        with patch.object(Cluster, '__init__', lambda x, *args, **kwargs: None):
            cluster = Cluster(None, None)
            cluster.nodes = {}
            cluster.seeds = []
            cluster._config_options = {}
            cluster._dse_config_options = {}
            cluster._Cluster__log_level = "INFO"
            cluster.path = '/tmp'
            cluster.name = 'test'
            cluster._Cluster__install_dir = '/fake'
            cluster.partitioner = None
            cluster.use_vnodes = False
            cluster.id = 0
            cluster.ipprefix = None
            cluster.snitch = 'org.apache.cassandra.locator.PropertyFileSnitch'
            cluster._debug = []
            cluster._trace = []
            cluster.monitoring_enabled = False
            cluster.monitoring_stack = None
            cluster.monitoring_dir = None
            cluster.grafana_port = 3000
            cluster.prometheus_port = 9090
            cluster.alertmanager_port = 9093

            node = Mock()
            node.name = 'node1'
            node.data_center = None
            node.rack = None

            with patch.object(cluster, '_update_config'):
                with patch.object(cluster, '_notify_topology_change') as mock_notify:
                    cluster.add(node, is_seed=False)
                    mock_notify.assert_called_once()


class TestClusterRemoveHook:
    """Test that Cluster.remove() calls _notify_topology_change."""

    def test_remove_node_calls_notify(self):
        with patch.object(Cluster, '__init__', lambda x, *args, **kwargs: None):
            cluster = Cluster(None, None)
            cluster.seeds = []
            cluster.path = '/tmp'
            cluster.name = 'test'
            cluster._Cluster__install_dir = '/fake'
            cluster.partitioner = None
            cluster._config_options = {}
            cluster._dse_config_options = {}
            cluster._Cluster__log_level = "INFO"
            cluster.use_vnodes = False
            cluster.id = 0
            cluster.ipprefix = None
            cluster.monitoring_enabled = False
            cluster.monitoring_stack = None
            cluster.monitoring_dir = None
            cluster.grafana_port = 3000
            cluster.prometheus_port = 9090
            cluster.alertmanager_port = 9093

            node = Mock()
            node.name = 'node1'
            cluster.nodes = {'node1': node}

            with patch.object(cluster, '_update_config'):
                with patch.object(cluster, '_notify_topology_change') as mock_notify:
                    with patch.object(cluster, 'remove_dir_with_retry'):
                        cluster.remove(node)
                        mock_notify.assert_called_once()


class TestConfigPersistence:
    """Test that monitoring fields are persisted in cluster.conf."""

    def test_update_config_includes_monitoring_fields(self, tmp_path):
        cluster_dir = tmp_path / 'test'
        cluster_dir.mkdir()

        with patch.object(Cluster, '__init__', lambda x, *args, **kwargs: None):
            cluster = Cluster(None, None)
            cluster.name = 'test'
            cluster.path = str(tmp_path)
            cluster.nodes = {}
            cluster.seeds = []
            cluster.partitioner = None
            cluster._Cluster__install_dir = '/fake'
            cluster._config_options = {}
            cluster._dse_config_options = {}
            cluster._Cluster__log_level = "INFO"
            cluster.use_vnodes = False
            cluster.id = 0
            cluster.ipprefix = None
            cluster.monitoring_enabled = True
            cluster.monitoring_dir = '/some/monitoring/dir'
            cluster.grafana_port = 3001
            cluster.prometheus_port = 9091
            cluster.alertmanager_port = 9094

            cluster._update_config()

            from ruamel.yaml import YAML
            conf_file = cluster_dir / 'cluster.conf'
            with open(conf_file) as f:
                data = YAML().load(f)

            assert data['monitoring_enabled'] is True
            assert data['monitoring_dir'] == '/some/monitoring/dir'
            assert data['grafana_port'] == 3001
            assert data['prometheus_port'] == 9091
            assert data['alertmanager_port'] == 9094

    def test_update_config_default_monitoring_fields(self, tmp_path):
        cluster_dir = tmp_path / 'test'
        cluster_dir.mkdir()

        with patch.object(Cluster, '__init__', lambda x, *args, **kwargs: None):
            cluster = Cluster(None, None)
            cluster.name = 'test'
            cluster.path = str(tmp_path)
            cluster.nodes = {}
            cluster.seeds = []
            cluster.partitioner = None
            cluster._Cluster__install_dir = '/fake'
            cluster._config_options = {}
            cluster._dse_config_options = {}
            cluster._Cluster__log_level = "INFO"
            cluster.use_vnodes = False
            cluster.id = 0
            cluster.ipprefix = None
            cluster.monitoring_enabled = False
            cluster.monitoring_dir = None
            cluster.grafana_port = 3000
            cluster.prometheus_port = 9090
            cluster.alertmanager_port = 9093

            cluster._update_config()

            from ruamel.yaml import YAML
            conf_file = cluster_dir / 'cluster.conf'
            with open(conf_file) as f:
                data = YAML().load(f)

            assert data['monitoring_enabled'] is False
            assert data['monitoring_dir'] is None
            assert data['grafana_port'] == 3000


class TestMonitoringStackIsRunning:
    """Tests for MonitoringStack.is_running."""

    def test_not_running_when_no_connection(self):
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = '/tmp/test'
        stack = MonitoringStack(cluster, '/fake/monitoring')
        # Prometheus is not running, so is_running should return False
        assert stack.is_running() is False


class TestMonitoringStackStartValidation:
    """Tests for MonitoringStack.start validation."""

    def test_start_raises_when_no_monitoring_dir(self):
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = '/tmp/test'
        stack = MonitoringStack(cluster, '/nonexistent')
        stack.monitoring_dir = None  # Explicitly unset

        with pytest.raises(EnvironmentError, match="scylla-monitoring directory not found"):
            stack.start()

    def test_start_raises_when_no_start_all_sh(self, tmp_path):
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = str(tmp_path)

        monitoring_dir = tmp_path / 'monitoring_src'
        monitoring_dir.mkdir()
        # No start-all.sh present

        stack = MonitoringStack(cluster, str(monitoring_dir))
        with pytest.raises(EnvironmentError, match="start-all.sh not found"):
            stack.start()


class TestMonitoringStackPorts:
    """Tests for MonitoringStack port configuration."""

    def test_default_ports(self):
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = '/tmp/test'
        stack = MonitoringStack(cluster, '/fake/monitoring')
        assert stack.grafana_port == 3000
        assert stack.prometheus_port == 9090
        assert stack.alertmanager_port == 9093

    def test_custom_ports(self):
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = '/tmp/test'
        stack = MonitoringStack(
            cluster, '/fake/monitoring',
            grafana_port=3001, prometheus_port=9091, alertmanager_port=9094
        )
        assert stack.grafana_port == 3001
        assert stack.prometheus_port == 9091
        assert stack.alertmanager_port == 9094

    def test_urls_use_configured_ports(self):
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = '/tmp/test'
        stack = MonitoringStack(
            cluster, '/fake/monitoring',
            grafana_port=4000, prometheus_port=9100
        )
        assert stack.grafana_url() == 'http://localhost:4000'
        assert stack.prometheus_url() == 'http://localhost:9100'


class TestCCMMonitoringEnvVar:
    """Tests for CCM_MONITORING environment variable."""

    def _create_cluster(self, tmp_path):
        """Create a real Cluster with minimal setup to test __init__ env var handling."""
        install_dir = tmp_path / 'install'
        install_dir.mkdir(exist_ok=True)
        # Cluster.__init__ calls validate_install_dir, __get_version_from_build,
        # and _update_config, so we patch those out and just test monitoring_enabled.
        with patch('ccmlib.common.validate_install_dir'):
            with patch.object(Cluster, '_Cluster__get_version_from_build', return_value='4.0'):
                with patch.object(Cluster, '_update_config'):
                    cluster = Cluster(str(tmp_path), 'test', install_dir=str(install_dir))
        return cluster

    def test_env_var_unset(self, tmp_path, monkeypatch):
        monkeypatch.delenv('CCM_MONITORING', raising=False)
        cluster = self._create_cluster(tmp_path)
        assert cluster.monitoring_enabled is False

    def test_env_var_empty(self, tmp_path, monkeypatch):
        monkeypatch.setenv('CCM_MONITORING', '')
        cluster = self._create_cluster(tmp_path)
        assert cluster.monitoring_enabled is False

    def test_env_var_zero(self, tmp_path, monkeypatch):
        monkeypatch.setenv('CCM_MONITORING', '0')
        cluster = self._create_cluster(tmp_path)
        assert cluster.monitoring_enabled is False

    def test_env_var_one(self, tmp_path, monkeypatch):
        monkeypatch.setenv('CCM_MONITORING', '1')
        cluster = self._create_cluster(tmp_path)
        assert cluster.monitoring_enabled is True

    def test_env_var_true(self, tmp_path, monkeypatch):
        monkeypatch.setenv('CCM_MONITORING', 'true')
        cluster = self._create_cluster(tmp_path)
        assert cluster.monitoring_enabled is True

    def test_env_var_any_nonempty_nonzero(self, tmp_path, monkeypatch):
        monkeypatch.setenv('CCM_MONITORING', 'yes')
        cluster = self._create_cluster(tmp_path)
        assert cluster.monitoring_enabled is True
