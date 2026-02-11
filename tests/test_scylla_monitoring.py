"""Tests for the scylla-monitoring integration (MonitoringStack, targets, hooks, CLI)."""
import json
import os
import socket
import subprocess
import pytest
from unittest.mock import Mock, patch

from ccmlib.scylla_monitoring import (
    MonitoringStack, _resolve_monitoring_dir, _BUILTIN_DASHBOARDS,
    _DATASOURCE_UID, _clone_monitoring_repo, _generate_dashboards,
    _fix_dashboard_datasources, _postprocess_dashboards,
    _is_port_available, _find_available_port,
)
from ccmlib.cluster import Cluster
from ccmlib.node import Status


class TestResolveMonitoringDir:
    """Tests for the _resolve_monitoring_dir helper."""

    def test_explicit_dir_exists(self, tmp_path):
        d = str(tmp_path)
        assert _resolve_monitoring_dir(d) == d

    def test_explicit_dir_not_exists(self, monkeypatch):
        monkeypatch.delenv('SCYLLA_MONITORING_DIR', raising=False)
        with patch('os.path.expanduser', return_value='/nonexistent'):
            assert _resolve_monitoring_dir('/nonexistent/path') is None

    def test_env_var(self, tmp_path, monkeypatch):
        d = str(tmp_path)
        monkeypatch.setenv('SCYLLA_MONITORING_DIR', d)
        assert _resolve_monitoring_dir() == d

    def test_env_var_not_exists(self, monkeypatch):
        monkeypatch.delenv('SCYLLA_MONITORING_DIR', raising=False)
        with patch('os.path.expanduser', return_value='/nonexistent'):
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

    def test_cluster_dir(self, tmp_path, monkeypatch):
        cluster_local = tmp_path / 'cluster' / 'scylla-monitoring'
        cluster_local.mkdir(parents=True)
        monkeypatch.delenv('SCYLLA_MONITORING_DIR', raising=False)
        with patch('os.path.expanduser', return_value='/nonexistent'):
            assert _resolve_monitoring_dir(cluster_dir=str(tmp_path / 'cluster')) == str(cluster_local)

    def test_cluster_dir_takes_priority_over_default(self, tmp_path, monkeypatch):
        cluster_local = tmp_path / 'cluster' / 'scylla-monitoring'
        cluster_local.mkdir(parents=True)
        default_dir = tmp_path / '.ccm' / 'scylla-monitoring'
        default_dir.mkdir(parents=True)
        monkeypatch.delenv('SCYLLA_MONITORING_DIR', raising=False)
        with patch('os.path.expanduser', return_value=str(default_dir)):
            assert _resolve_monitoring_dir(cluster_dir=str(tmp_path / 'cluster')) == str(cluster_local)

    def test_none_when_nothing_available(self, monkeypatch):
        monkeypatch.delenv('SCYLLA_MONITORING_DIR', raising=False)
        with patch('os.path.expanduser', return_value='/nonexistent'):
            assert _resolve_monitoring_dir() is None


class TestDefaultPorts:
    """Tests for MonitoringStack.default_ports."""

    def test_default_cluster_id_0(self):
        ports = MonitoringStack.default_ports(0)
        assert ports == {
            'grafana_port': 3000,
            'prometheus_port': 9090,
            'alertmanager_port': 9093,
        }

    def test_cluster_id_1(self):
        ports = MonitoringStack.default_ports(1)
        assert ports == {
            'grafana_port': 3010,
            'prometheus_port': 9100,
            'alertmanager_port': 9103,
        }

    def test_cluster_id_5(self):
        ports = MonitoringStack.default_ports(5)
        assert ports == {
            'grafana_port': 3050,
            'prometheus_port': 9140,
            'alertmanager_port': 9143,
        }

    def test_two_clusters_no_overlap(self):
        ports_0 = MonitoringStack.default_ports(0)
        ports_1 = MonitoringStack.default_ports(1)
        all_ports_0 = set(ports_0.values())
        all_ports_1 = set(ports_1.values())
        assert not all_ports_0.intersection(all_ports_1)


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
        stack = MonitoringStack(cluster)
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
        stack = MonitoringStack(cluster)
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
        stack = MonitoringStack(cluster)
        targets = stack._generate_targets_yaml()

        assert len(targets) == 1
        assert set(targets[0]['targets']) == {'127.0.0.1', '127.0.0.3'}

    def test_excludes_uninitialized_nodes(self):
        cluster = self._make_mock_cluster([
            ('127.0.0.1', Status.UP, 'dc1'),
            ('127.0.0.2', Status.UNINITIALIZED, 'dc1'),
        ])
        stack = MonitoringStack(cluster)
        targets = stack._generate_targets_yaml()

        assert len(targets) == 1
        assert targets[0]['targets'] == ['127.0.0.1']

    def test_includes_decommissioned_nodes(self):
        cluster = self._make_mock_cluster([
            ('127.0.0.1', Status.UP, 'dc1'),
            ('127.0.0.2', Status.DECOMMISSIONED, 'dc1'),
        ])
        stack = MonitoringStack(cluster)
        targets = stack._generate_targets_yaml()

        assert len(targets) == 1
        assert set(targets[0]['targets']) == {'127.0.0.1', '127.0.0.2'}

    def test_empty_cluster(self):
        cluster = self._make_mock_cluster([])
        stack = MonitoringStack(cluster)
        targets = stack._generate_targets_yaml()

        assert targets == []

    def test_all_nodes_down(self):
        cluster = self._make_mock_cluster([
            ('127.0.0.1', Status.DOWN, 'dc1'),
            ('127.0.0.2', Status.DOWN, 'dc1'),
        ])
        stack = MonitoringStack(cluster)
        targets = stack._generate_targets_yaml()

        assert targets == []

    def test_default_datacenter(self):
        """Nodes with data_center=None should use 'datacenter1'."""
        cluster = self._make_mock_cluster([
            ('127.0.0.1', Status.UP, None),
            ('127.0.0.2', Status.UP, None),
        ])
        stack = MonitoringStack(cluster)
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
        stack = MonitoringStack(cluster)
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

        stack = MonitoringStack(cluster)
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

        stack = MonitoringStack(cluster)
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

        stack = MonitoringStack(cluster)
        stack._working_dir = str(tmp_path / 'monitoring')

        stack.update_targets()

        targets_file = tmp_path / 'monitoring' / 'prometheus' / 'targets' / 'scylla_servers.yml'
        with open(targets_file) as f:
            data = json.load(f)

        assert len(data) == 1
        assert data[0]['labels']['cluster'] == 'mytest'
        assert data[0]['labels']['dc'] == 'dc1'
        assert set(data[0]['targets']) == {'127.0.0.1', '127.0.0.2'}


class TestWritePrometheusConfig:
    """Tests for MonitoringStack._write_prometheus_config."""

    def test_writes_config_file(self, tmp_path):
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = str(tmp_path)

        stack = MonitoringStack(cluster)
        stack._working_dir = str(tmp_path / 'monitoring')

        stack._write_prometheus_config()

        config_file = tmp_path / 'monitoring' / 'prometheus' / 'prometheus.yml'
        assert config_file.exists()

        content = config_file.read_text()
        assert 'scrape_interval: 20s' in content
        assert 'scylla_servers.yml' in content
        assert '9180' in content

    def test_creates_directory_if_needed(self, tmp_path):
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = str(tmp_path)

        stack = MonitoringStack(cluster)
        stack._working_dir = str(tmp_path / 'monitoring')

        stack._write_prometheus_config()

        assert (tmp_path / 'monitoring' / 'prometheus').is_dir()


class TestWriteGrafanaProvisioning:
    """Tests for MonitoringStack._write_grafana_provisioning."""

    def test_writes_datasource_config(self, tmp_path):
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = str(tmp_path)

        stack = MonitoringStack(cluster, prometheus_port=9100)
        stack._working_dir = str(tmp_path / 'monitoring')

        stack._write_grafana_provisioning()

        datasource_file = (tmp_path / 'monitoring' / 'grafana' /
                          'provisioning' / 'datasources' / 'prometheus.yml')
        assert datasource_file.exists()

        content = datasource_file.read_text()
        assert 'http://localhost:9100' in content
        assert 'isDefault: true' in content
        assert _DATASOURCE_UID in content

    def test_always_writes_dashboard_provisioning(self, tmp_path):
        """Dashboard provisioning is always created (built-in dashboards)."""
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = str(tmp_path)

        stack = MonitoringStack(cluster)
        stack._working_dir = str(tmp_path / 'monitoring')

        stack._write_grafana_provisioning()

        dashboard_config = (tmp_path / 'monitoring' / 'grafana' /
                           'provisioning' / 'dashboards' / 'dashboards.yml')
        assert dashboard_config.exists()

    def test_writes_builtin_dashboard_files(self, tmp_path):
        """Built-in Scylla dashboards should be written when generation is unavailable."""
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = str(tmp_path)

        stack = MonitoringStack(cluster)
        stack._working_dir = str(tmp_path / 'monitoring')
        stack.monitoring_dir = None  # Force no monitoring dir

        stack._write_grafana_provisioning()

        dashboards_dir = tmp_path / 'monitoring' / 'grafana' / 'dashboards'
        assert dashboards_dir.is_dir()

        # Should have at least the overview dashboard
        overview = dashboards_dir / 'scylla-overview.json'
        assert overview.exists()

        data = json.loads(overview.read_text())
        assert data['title'] == 'Scylla Overview'
        assert len(data['panels']) >= 4

    def test_generation_called_when_monitoring_dir_set(self, tmp_path):
        """When monitoring_dir is set, _generate_dashboards should be called."""
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = str(tmp_path)

        monitoring_dir = tmp_path / 'scylla-monitoring'
        monitoring_dir.mkdir()

        stack = MonitoringStack(cluster, monitoring_dir=str(monitoring_dir))
        stack._working_dir = str(tmp_path / 'monitoring')

        with patch('ccmlib.scylla_monitoring._generate_dashboards',
                   return_value=3) as mock_gen:
            stack._write_grafana_provisioning()

        mock_gen.assert_called_once_with(
            str(monitoring_dir),
            str(tmp_path / 'monitoring' / 'grafana' / 'dashboards'))


class TestBuiltinDashboards:
    """Tests for the built-in Scylla dashboard definitions."""

    def test_builtin_dashboards_are_valid_json(self):
        """Each built-in dashboard should produce valid JSON with required fields."""
        for filename, dashboard_fn in _BUILTIN_DASHBOARDS.items():
            assert filename.endswith('.json')
            dashboard = dashboard_fn()
            assert isinstance(dashboard, dict)
            assert 'title' in dashboard
            assert 'panels' in dashboard
            assert 'uid' in dashboard
            # Round-trip through JSON to verify serialisability
            json.loads(json.dumps(dashboard))

    def test_overview_dashboard_has_key_panels(self):
        """The overview dashboard should cover the most important metrics."""
        dashboard = _BUILTIN_DASHBOARDS['scylla-overview.json']()
        panel_titles = {p['title'] for p in dashboard['panels']}
        assert 'CPU Utilization' in panel_titles
        assert 'CQL Requests / sec' in panel_titles

    def test_overview_dashboard_references_correct_datasource(self):
        """All panels should reference the provisioned datasource UID."""
        dashboard = _BUILTIN_DASHBOARDS['scylla-overview.json']()
        for panel in dashboard['panels']:
            for target in panel.get('targets', []):
                ds = target.get('datasource', {})
                assert ds.get('uid') == _DATASOURCE_UID, \
                    f"Panel '{panel['title']}' should reference {_DATASOURCE_UID}"

    def test_write_builtin_dashboards(self, tmp_path):
        """_write_builtin_dashboards should create JSON files on disk."""
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = str(tmp_path)

        stack = MonitoringStack(cluster)
        dest = str(tmp_path / 'dashboards')
        os.makedirs(dest)
        stack._write_builtin_dashboards(dest)

        for filename in _BUILTIN_DASHBOARDS:
            filepath = os.path.join(dest, filename)
            assert os.path.exists(filepath)
            with open(filepath) as f:
                data = json.load(f)
            assert 'panels' in data


class TestCloneMonitoringRepo:
    """Tests for _clone_monitoring_repo."""

    def test_successful_clone(self, tmp_path):
        target = str(tmp_path / 'scylla-monitoring')
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout='', stderr='')
            assert _clone_monitoring_repo(target) is True
            cmd = mock_run.call_args[0][0]
            assert cmd[0:2] == ['git', 'clone']
            assert '--depth' in cmd
            assert target == cmd[-1]

    def test_clone_failure(self, tmp_path):
        target = str(tmp_path / 'scylla-monitoring')
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=128, stdout='', stderr='fatal')
            assert _clone_monitoring_repo(target) is False

    def test_git_not_installed(self, tmp_path):
        target = str(tmp_path / 'scylla-monitoring')
        with patch('subprocess.run', side_effect=FileNotFoundError):
            assert _clone_monitoring_repo(target) is False

    def test_clone_timeout(self, tmp_path):
        target = str(tmp_path / 'scylla-monitoring')
        with patch('subprocess.run', side_effect=subprocess.TimeoutExpired('git', 300)):
            assert _clone_monitoring_repo(target) is False


class TestGenerateDashboards:
    """Tests for _generate_dashboards."""

    def test_generates_from_templates(self, tmp_path):
        """Should run make_dashboards.py for each .template.json file."""
        monitoring_dir = tmp_path / 'scylla-monitoring'
        monitoring_dir.mkdir()
        (monitoring_dir / 'make_dashboards.py').write_text('# script')
        grafana_dir = monitoring_dir / 'grafana'
        grafana_dir.mkdir()
        (grafana_dir / 'types.json').write_text('{}')
        (grafana_dir / 'scylla-overview.template.json').write_text('{}')
        (grafana_dir / 'scylla-cql.template.json').write_text('{}')
        (grafana_dir / 'not-a-template.json').write_text('{}')

        output = str(tmp_path / 'output')

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout='', stderr='')
            count = _generate_dashboards(str(monitoring_dir), output)

        # Should have called for 2 template files, not the non-template
        assert count == 2
        assert mock_run.call_count == 2

    def test_returns_zero_on_missing_script(self, tmp_path):
        """Should return 0 if make_dashboards.py is missing."""
        monitoring_dir = tmp_path / 'scylla-monitoring'
        monitoring_dir.mkdir()
        (monitoring_dir / 'grafana').mkdir()
        (monitoring_dir / 'grafana' / 'types.json').write_text('{}')
        # No make_dashboards.py

        count = _generate_dashboards(str(monitoring_dir), str(tmp_path / 'out'))
        assert count == 0

    def test_partial_failure(self, tmp_path):
        """Should count only successful generations."""
        monitoring_dir = tmp_path / 'scylla-monitoring'
        monitoring_dir.mkdir()
        (monitoring_dir / 'make_dashboards.py').write_text('# script')
        grafana_dir = monitoring_dir / 'grafana'
        grafana_dir.mkdir()
        (grafana_dir / 'types.json').write_text('{}')
        (grafana_dir / 'a.template.json').write_text('{}')
        (grafana_dir / 'b.template.json').write_text('{}')

        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return Mock(returncode=0, stdout='', stderr='')
            return Mock(returncode=1, stdout='', stderr='error')

        with patch('subprocess.run', side_effect=side_effect):
            count = _generate_dashboards(str(monitoring_dir), str(tmp_path / 'out'))

        assert count == 1


class TestFixDashboardDatasources:
    """Tests for _fix_dashboard_datasources and _postprocess_dashboards."""

    def test_replaces_hardcoded_prometheus_uid(self):
        dashboard = {
            "panels": [{
                "datasource": {"type": "prometheus", "uid": "P1809F7CD0C75ACF3"},
                "targets": [{
                    "datasource": {"type": "prometheus", "uid": "OLD_UID"},
                    "expr": "up",
                }],
            }]
        }
        _fix_dashboard_datasources(dashboard)
        assert dashboard["panels"][0]["datasource"] == {
            "type": "prometheus", "uid": _DATASOURCE_UID}
        assert dashboard["panels"][0]["targets"][0]["datasource"] == {
            "type": "prometheus", "uid": _DATASOURCE_UID}

    def test_converts_string_prometheus_to_uid_object(self):
        """String 'prometheus' refs must become UID objects for Grafana."""
        dashboard = {
            "panels": [{"datasource": "prometheus"}],
            "templating": {"list": [{"datasource": "prometheus", "name": "cluster"}]},
        }
        _fix_dashboard_datasources(dashboard)
        expected = {"type": "prometheus", "uid": _DATASOURCE_UID}
        assert dashboard["panels"][0]["datasource"] == expected
        assert dashboard["templating"]["list"][0]["datasource"] == expected

    def test_adds_uid_to_prometheus_without_uid(self):
        dashboard = {
            "panels": [{"datasource": {"type": "prometheus"}}]
        }
        _fix_dashboard_datasources(dashboard)
        assert dashboard["panels"][0]["datasource"] == {
            "type": "prometheus", "uid": _DATASOURCE_UID}

    def test_leaves_non_prometheus_datasource_alone(self):
        dashboard = {
            "panels": [{"datasource": {"type": "datasource", "uid": "grafana"}}]
        }
        _fix_dashboard_datasources(dashboard)
        assert dashboard["panels"][0]["datasource"]["uid"] == "grafana"

    def test_leaves_non_prometheus_string_datasource_alone(self):
        dashboard = {"panels": [{"datasource": "loki"}]}
        _fix_dashboard_datasources(dashboard)
        assert dashboard["panels"][0]["datasource"] == "loki"

    def test_leaves_null_datasource_alone(self):
        dashboard = {"panels": [{"datasource": None}]}
        _fix_dashboard_datasources(dashboard)
        assert dashboard["panels"][0]["datasource"] is None

    def test_postprocess_rewrites_files(self, tmp_path):
        dash = {
            "panels": [{
                "datasource": "prometheus",
                "targets": [{"datasource": {"type": "prometheus", "uid": "OLD"}}],
            }],
            "templating": {"list": [{"datasource": "prometheus"}]},
        }
        fpath = tmp_path / "test.json"
        fpath.write_text(json.dumps(dash))

        _postprocess_dashboards(str(tmp_path))

        result = json.loads(fpath.read_text())
        expected = {"type": "prometheus", "uid": _DATASOURCE_UID}
        assert result["panels"][0]["datasource"] == expected
        assert result["panels"][0]["targets"][0]["datasource"] == expected
        assert result["templating"]["list"][0]["datasource"] == expected


class TestEnsureMonitoringRepo:
    """Tests for MonitoringStack._ensure_monitoring_repo."""

    def test_uses_existing_repo(self, tmp_path):
        """If the default dir already has grafana/, use it without cloning."""
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = str(tmp_path)
        stack = MonitoringStack(cluster)
        stack.monitoring_dir = None

        fake_dir = tmp_path / 'default_monitoring'
        (fake_dir / 'grafana').mkdir(parents=True)

        with patch('ccmlib.scylla_monitoring._DEFAULT_MONITORING_DIR',
                   str(fake_dir)):
            stack._ensure_monitoring_repo()

        assert stack.monitoring_dir == str(fake_dir)

    def test_clones_when_no_repo(self, tmp_path):
        """Should clone the repo when the default dir doesn't exist."""
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = str(tmp_path)
        stack = MonitoringStack(cluster)
        stack.monitoring_dir = None

        fake_dir = str(tmp_path / 'nonexistent')

        with patch('ccmlib.scylla_monitoring._DEFAULT_MONITORING_DIR', fake_dir):
            with patch('ccmlib.scylla_monitoring._clone_monitoring_repo',
                       return_value=True) as mock_clone:
                stack._ensure_monitoring_repo()

        mock_clone.assert_called_once_with(fake_dir)
        assert stack.monitoring_dir == fake_dir

    def test_fallback_when_clone_fails(self, tmp_path):
        """monitoring_dir stays None when clone fails."""
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = str(tmp_path)
        stack = MonitoringStack(cluster)
        stack.monitoring_dir = None

        fake_dir = str(tmp_path / 'nonexistent')

        with patch('ccmlib.scylla_monitoring._DEFAULT_MONITORING_DIR', fake_dir):
            with patch('ccmlib.scylla_monitoring._clone_monitoring_repo',
                       return_value=False):
                stack._ensure_monitoring_repo()

        assert stack.monitoring_dir is None


class TestGrafanaProvisioningWithGeneration:
    """Tests for dashboard generation in _write_grafana_provisioning."""

    def test_uses_generated_dashboards_when_available(self, tmp_path):
        """Should prefer generated dashboards over built-in."""
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = str(tmp_path)

        stack = MonitoringStack(cluster)
        stack._working_dir = str(tmp_path / 'monitoring')
        stack.monitoring_dir = str(tmp_path / 'fake-monitoring')

        with patch('ccmlib.scylla_monitoring._generate_dashboards',
                   return_value=5) as mock_gen:
            stack._write_grafana_provisioning()

        mock_gen.assert_called_once()
        # Verify dashboard provisioning config was created
        config = (tmp_path / 'monitoring' / 'grafana' /
                  'provisioning' / 'dashboards' / 'dashboards.yml')
        assert config.exists()

    def test_falls_back_to_builtin_when_generation_fails(self, tmp_path):
        """Should write built-in dashboards when generation returns 0."""
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = str(tmp_path)

        stack = MonitoringStack(cluster)
        stack._working_dir = str(tmp_path / 'monitoring')
        stack.monitoring_dir = str(tmp_path / 'fake-monitoring')

        with patch('ccmlib.scylla_monitoring._generate_dashboards',
                   return_value=0):
            stack._write_grafana_provisioning()

        # Built-in dashboards should exist
        overview = (tmp_path / 'monitoring' / 'grafana' /
                    'dashboards' / 'scylla-overview.json')
        assert overview.exists()
        data = json.loads(overview.read_text())
        assert data['title'] == 'Scylla Overview'

    def test_falls_back_to_builtin_when_no_monitoring_dir(self, tmp_path):
        """Should write built-in dashboards when monitoring_dir is None."""
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = str(tmp_path)

        stack = MonitoringStack(cluster)
        stack._working_dir = str(tmp_path / 'monitoring')
        stack.monitoring_dir = None

        stack._write_grafana_provisioning()

        overview = (tmp_path / 'monitoring' / 'grafana' /
                    'dashboards' / 'scylla-overview.json')
        assert overview.exists()


class TestContainerManagement:
    """Tests for Docker container start/stop operations."""

    def test_container_names(self):
        cluster = Mock()
        cluster.name = 'mycluster'
        cluster.get_path.return_value = '/tmp/test'
        stack = MonitoringStack(cluster)

        assert stack._container_name('prometheus') == 'ccm-mycluster-prometheus'
        assert stack._container_name('grafana') == 'ccm-mycluster-grafana'
        assert stack._container_name('alertmanager') == 'ccm-mycluster-alertmanager'

    def test_stop_calls_docker_stop_and_rm(self):
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = '/tmp/test'
        stack = MonitoringStack(cluster)

        with patch('subprocess.run') as mock_run:
            stack.stop()

            # Should call docker stop + docker rm for each of 3 services
            assert mock_run.call_count == 6
            calls = mock_run.call_args_list

            # Check stop calls
            stop_calls = [c for c in calls if c[0][0][1] == 'stop']
            assert len(stop_calls) == 3
            stopped_names = {c[0][0][2] for c in stop_calls}
            assert stopped_names == {
                'ccm-test-prometheus',
                'ccm-test-grafana',
                'ccm-test-alertmanager',
            }

            # Check rm calls
            rm_calls = [c for c in calls if c[0][0][1] == 'rm']
            assert len(rm_calls) == 3
            removed_names = {c[0][0][3] for c in rm_calls}
            assert removed_names == {
                'ccm-test-prometheus',
                'ccm-test-grafana',
                'ccm-test-alertmanager',
            }

    def test_start_prometheus_calls_docker_run(self, tmp_path):
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = str(tmp_path)
        stack = MonitoringStack(cluster, prometheus_port=9100)
        stack._working_dir = str(tmp_path / 'monitoring')

        # Create required files
        config_dir = tmp_path / 'monitoring' / 'prometheus'
        config_dir.mkdir(parents=True)
        (config_dir / 'prometheus.yml').write_text('global: {}')

        targets_dir = str(config_dir / 'targets')
        data_dir = str(tmp_path / 'monitoring' / 'data' / 'prometheus_data')
        os.makedirs(targets_dir, exist_ok=True)
        os.makedirs(data_dir, exist_ok=True)

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout='', stderr='')
            stack._start_prometheus(targets_dir, data_dir)

            mock_run.assert_called_once()
            cmd = mock_run.call_args[0][0]
            assert cmd[0:3] == ['docker', 'run', '-d']
            assert '--net=host' in cmd
            assert '--web.listen-address=:9100' in cmd
            assert 'prom/prometheus' in cmd

    def test_start_grafana_calls_docker_run(self, tmp_path):
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = str(tmp_path)
        stack = MonitoringStack(cluster, grafana_port=3010)
        stack._working_dir = str(tmp_path / 'monitoring')

        provisioning_dir = tmp_path / 'monitoring' / 'grafana' / 'provisioning'
        provisioning_dir.mkdir(parents=True)
        dashboards_dir = tmp_path / 'monitoring' / 'grafana' / 'dashboards'
        dashboards_dir.mkdir(parents=True)

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout='', stderr='')
            stack._start_grafana()

            mock_run.assert_called_once()
            cmd = mock_run.call_args[0][0]
            assert cmd[0:3] == ['docker', 'run', '-d']
            assert '--net=host' in cmd
            assert 'GF_SERVER_HTTP_PORT=3010' in cmd[cmd.index('-e') + 1]
            assert 'grafana/grafana' in cmd

    def test_start_grafana_always_mounts_dashboards(self, tmp_path):
        """Grafana container should always mount the dashboards directory."""
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = str(tmp_path)
        stack = MonitoringStack(cluster)
        stack._working_dir = str(tmp_path / 'monitoring')

        dashboards_dir = tmp_path / 'monitoring' / 'grafana' / 'dashboards'
        dashboards_dir.mkdir(parents=True)
        provisioning_dir = tmp_path / 'monitoring' / 'grafana' / 'provisioning'
        provisioning_dir.mkdir(parents=True)

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout='', stderr='')
            stack._start_grafana()

            cmd = mock_run.call_args[0][0]
            volume_args = [cmd[i + 1] for i, v in enumerate(cmd) if v == '-v']
            dashboards_mount = [v for v in volume_args if '/var/lib/grafana/dashboards' in v]
            assert len(dashboards_mount) == 1, \
                f"Should mount dashboards dir, volumes: {volume_args}"

    def test_start_alertmanager_calls_docker_run(self, tmp_path):
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = str(tmp_path)
        stack = MonitoringStack(cluster, alertmanager_port=9103)

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout='', stderr='')
            stack._start_alertmanager()

            mock_run.assert_called_once()
            cmd = mock_run.call_args[0][0]
            assert cmd[0:3] == ['docker', 'run', '-d']
            assert '--net=host' in cmd
            assert '--web.listen-address=:9103' in cmd
            assert 'prom/alertmanager' in cmd

    def test_start_raises_on_docker_failure(self, tmp_path):
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = str(tmp_path)
        stack = MonitoringStack(cluster)

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=1, stdout='', stderr='error')
            with pytest.raises(RuntimeError, match="Failed to start Alertmanager"):
                stack._start_alertmanager()


class TestIsRunning:
    """Tests for MonitoringStack.is_running."""

    def test_not_running_when_no_connection(self):
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = '/tmp/test'
        stack = MonitoringStack(cluster)

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(stdout='', returncode=0)
            assert stack.is_running() is False

    def test_running_via_docker_ps_fallback(self):
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = '/tmp/test'
        stack = MonitoringStack(cluster)

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(stdout='abc123\n', returncode=0)
            # HTTP probe fails, but docker ps finds container
            assert stack.is_running() is True


class TestStartIntegration:
    """Tests for the full MonitoringStack.start() flow."""

    def test_start_creates_configs_and_runs_containers(self, tmp_path):
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = str(tmp_path)
        cluster.nodelist.return_value = []

        stack = MonitoringStack(cluster, prometheus_port=9090, grafana_port=3000)
        stack._working_dir = str(tmp_path / 'monitoring')

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout='', stderr='')
            with patch.object(stack, '_wait_until_ready'):
                stack.start()

                # Should have called docker run 3 times (stop+rm calls too)
                docker_run_calls = [
                    c for c in mock_run.call_args_list
                    if c[0][0][:2] == ['docker', 'run']
                ]
                assert len(docker_run_calls) == 3

        # Verify config files were created
        prometheus_config = tmp_path / 'monitoring' / 'prometheus' / 'prometheus.yml'
        assert prometheus_config.exists()

        grafana_datasource = (tmp_path / 'monitoring' / 'grafana' /
                             'provisioning' / 'datasources' / 'prometheus.yml')
        assert grafana_datasource.exists()


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


class TestReconnectMonitoringStack:
    """Tests for ScyllaCluster._reconnect_monitoring_stack."""

    def test_reconnects_when_stack_is_none_and_containers_running(self, tmp_path):
        """After loading from disk, monitoring_stack is None but containers
        may still be running. _reconnect_monitoring_stack should find them."""
        from ccmlib.scylla_cluster import ScyllaCluster
        with patch.object(ScyllaCluster, '__init__', lambda x, *args, **kwargs: None):
            cluster = ScyllaCluster(None, None)
            cluster.monitoring_stack = None
            cluster.monitoring_dir = str(tmp_path)
            cluster.grafana_port = 3000
            cluster.prometheus_port = 9090
            cluster.alertmanager_port = 9093
            cluster.get_path = lambda: str(tmp_path)

            with patch.object(MonitoringStack, '__init__', lambda *a, **kw: None):
                with patch.object(MonitoringStack, 'is_running', return_value=True):
                    cluster._reconnect_monitoring_stack()

            assert cluster.monitoring_stack is not None

    def test_no_reconnect_when_containers_not_running(self, tmp_path):
        """If containers aren't running, stays None."""
        from ccmlib.scylla_cluster import ScyllaCluster
        with patch.object(ScyllaCluster, '__init__', lambda x, *args, **kwargs: None):
            cluster = ScyllaCluster(None, None)
            cluster.monitoring_stack = None
            cluster.monitoring_dir = str(tmp_path)
            cluster.grafana_port = 3000
            cluster.prometheus_port = 9090
            cluster.alertmanager_port = 9093
            cluster.get_path = lambda: str(tmp_path)

            with patch.object(MonitoringStack, '__init__', lambda *a, **kw: None):
                with patch.object(MonitoringStack, 'is_running', return_value=False):
                    cluster._reconnect_monitoring_stack()

            assert cluster.monitoring_stack is None

    def test_skips_if_already_connected(self):
        """If monitoring_stack is already set and running, don't reconnect."""
        from ccmlib.scylla_cluster import ScyllaCluster
        with patch.object(ScyllaCluster, '__init__', lambda x, *args, **kwargs: None):
            cluster = ScyllaCluster(None, None)
            existing_stack = Mock()
            existing_stack.is_running.return_value = True
            cluster.monitoring_stack = existing_stack

            cluster._reconnect_monitoring_stack()

            assert cluster.monitoring_stack is existing_stack

    def test_stop_calls_reconnect(self, tmp_path):
        """ScyllaCluster.stop() should reconnect and stop orphaned containers."""
        from ccmlib.scylla_cluster import ScyllaCluster
        with patch.object(ScyllaCluster, '__init__', lambda x, *args, **kwargs: None):
            cluster = ScyllaCluster(None, None)
            cluster.monitoring_stack = None
            cluster.monitoring_dir = str(tmp_path)
            cluster.grafana_port = 3000
            cluster.prometheus_port = 9090
            cluster.alertmanager_port = 9093
            cluster._scylla_manager = None
            cluster.skip_manager_server = False
            cluster.nodes = {}

            mock_stack = Mock()
            mock_stack.is_running.return_value = True

            with patch.object(cluster, '_reconnect_monitoring_stack') as mock_reconnect:
                def do_reconnect():
                    cluster.monitoring_stack = mock_stack
                mock_reconnect.side_effect = do_reconnect

                with patch.object(cluster, 'stop_nodes'):
                    cluster.stop()

                mock_reconnect.assert_called_once()
                mock_stack.stop.assert_called_once()


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


class TestMonitoringStackPorts:
    """Tests for MonitoringStack port configuration."""

    def test_default_ports(self):
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = '/tmp/test'
        stack = MonitoringStack(cluster)
        assert stack.grafana_port == 3000
        assert stack.prometheus_port == 9090
        assert stack.alertmanager_port == 9093

    def test_custom_ports(self):
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = '/tmp/test'
        stack = MonitoringStack(
            cluster,
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
            cluster,
            grafana_port=4000, prometheus_port=9100
        )
        assert stack.grafana_url() == 'http://localhost:4000'
        assert stack.prometheus_url() == 'http://localhost:9100'

    def test_default_addresses(self):
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = '/tmp/test'
        stack = MonitoringStack(cluster)
        assert stack.grafana_address() == ('localhost', 3000)
        assert stack.prometheus_address() == ('localhost', 9090)

    def test_custom_addresses(self):
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = '/tmp/test'
        stack = MonitoringStack(
            cluster,
            grafana_port=4000, prometheus_port=9100
        )
        assert stack.grafana_address() == ('localhost', 4000)
        assert stack.prometheus_address() == ('localhost', 9100)


class TestPortAvailability:
    """Tests for port sniffing and auto-selection."""

    def test_is_port_available_free_port(self):
        assert _is_port_available(0) is True  # OS picks a free port

    def test_is_port_available_occupied_port(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]
            assert _is_port_available(port) is False

    def test_find_available_port_returns_start_when_free(self):
        # Find a free port first, then confirm _find_available_port returns it
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]
        # Now the port is free again
        assert _find_available_port(port) == port

    def test_find_available_port_skips_occupied(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            occupied = s.getsockname()[1]
            result = _find_available_port(occupied)
            assert result > occupied

    def test_find_available_port_raises_on_exhaustion(self):
        with pytest.raises(RuntimeError, match="Could not find a free port"):
            with patch('ccmlib.scylla_monitoring._is_port_available',
                       return_value=False):
                _find_available_port(3000, max_attempts=5)

    def test_resolve_ports_bumps_occupied(self):
        """_resolve_ports should find next free port and update cluster."""
        # Grab a port by binding, keep it held so it's occupied
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            occupied = s.getsockname()[1]

            cluster = Mock()
            cluster.name = 'test'
            cluster.get_path.return_value = '/tmp/test'
            cluster.grafana_port = occupied
            cluster.prometheus_port = 39901
            cluster.alertmanager_port = 39902

            stack = MonitoringStack(cluster,
                                    grafana_port=occupied,
                                    prometheus_port=39901,
                                    alertmanager_port=39902)

            stack._resolve_ports()

        assert stack.grafana_port != occupied
        assert stack.grafana_port > occupied
        assert cluster.grafana_port == stack.grafana_port

    def test_resolve_ports_keeps_free_ports(self):
        """If all ports are already free, nothing changes."""
        cluster = Mock()
        cluster.name = 'test'
        cluster.get_path.return_value = '/tmp/test'

        # Use high ports very likely to be free
        stack = MonitoringStack(cluster,
                                grafana_port=39123,
                                prometheus_port=39124,
                                alertmanager_port=39125)

        stack._resolve_ports()
        assert stack.grafana_port == 39123
        assert stack.prometheus_port == 39124
        assert stack.alertmanager_port == 39125


class TestCCMMonitoringEnvVar:
    """Tests for CCM_MONITORING environment variable.

    The env var only enables monitoring for ScyllaCluster, not base Cluster
    (Cassandra) or DseCluster.
    """

    def _create_cassandra_cluster(self, tmp_path):
        """Create a base Cluster (Cassandra)."""
        install_dir = tmp_path / 'install'
        install_dir.mkdir(exist_ok=True)
        with patch('ccmlib.common.validate_install_dir'):
            with patch.object(Cluster, '_Cluster__get_version_from_build', return_value='4.0'):
                with patch.object(Cluster, '_update_config'):
                    cluster = Cluster(str(tmp_path), 'test', install_dir=str(install_dir))
        return cluster

    def _create_scylla_cluster(self, tmp_path):
        """Create a ScyllaCluster with minimal setup."""
        from ccmlib.scylla_cluster import ScyllaCluster
        install_dir = tmp_path / 'install'
        install_dir.mkdir(exist_ok=True)
        with patch('ccmlib.common.validate_install_dir'):
            with patch('ccmlib.common.scylla_extract_install_dir_and_mode',
                       return_value=(str(install_dir), 'release')):
                with patch.object(Cluster, '_Cluster__get_version_from_build', return_value='5.4'):
                    with patch.object(ScyllaCluster, '_update_config'):
                        cluster = ScyllaCluster(
                            str(tmp_path), 'test',
                            install_dir=str(install_dir))
        return cluster

    def test_cassandra_ignores_env_var(self, tmp_path, monkeypatch):
        """CCM_MONITORING has no effect on Cassandra clusters."""
        monkeypatch.setenv('CCM_MONITORING', '1')
        cluster = self._create_cassandra_cluster(tmp_path)
        assert cluster.monitoring_enabled is False

    def test_env_var_unset(self, tmp_path, monkeypatch):
        monkeypatch.delenv('CCM_MONITORING', raising=False)
        cluster = self._create_scylla_cluster(tmp_path)
        assert cluster.monitoring_enabled is False

    def test_env_var_empty(self, tmp_path, monkeypatch):
        monkeypatch.setenv('CCM_MONITORING', '')
        cluster = self._create_scylla_cluster(tmp_path)
        assert cluster.monitoring_enabled is False

    def test_env_var_zero(self, tmp_path, monkeypatch):
        monkeypatch.setenv('CCM_MONITORING', '0')
        cluster = self._create_scylla_cluster(tmp_path)
        assert cluster.monitoring_enabled is False

    def test_env_var_one(self, tmp_path, monkeypatch):
        monkeypatch.setenv('CCM_MONITORING', '1')
        cluster = self._create_scylla_cluster(tmp_path)
        assert cluster.monitoring_enabled is True

    def test_env_var_true(self, tmp_path, monkeypatch):
        monkeypatch.setenv('CCM_MONITORING', 'true')
        cluster = self._create_scylla_cluster(tmp_path)
        assert cluster.monitoring_enabled is True

    def test_env_var_any_nonempty_nonzero(self, tmp_path, monkeypatch):
        monkeypatch.setenv('CCM_MONITORING', 'yes')
        cluster = self._create_scylla_cluster(tmp_path)
        assert cluster.monitoring_enabled is True
