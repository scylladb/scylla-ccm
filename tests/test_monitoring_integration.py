"""Integration tests for the monitoring stack with a real Scylla cluster.

Requires:
  - Docker daemon running
  - prom/prometheus, grafana/grafana, prom/alertmanager images available
  - A Scylla relocatable version cached or downloadable

Run with:
  python -m pytest tests/test_monitoring_integration.py -v -s

These tests are slow (cluster startup) and require Docker, so they're
marked with @pytest.mark.integration.
"""

import json
import os
import subprocess
import time
import urllib.request
import urllib.parse
import urllib.error

import pytest

from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_monitoring import MonitoringStack
from ccmlib import common


SCYLLA_VERSION = os.environ.get("SCYLLA_VERSION", "release:6.2.3")
CLUSTER_ID = 5  # Use high ID to avoid port conflicts with anything running


def docker_available():
    try:
        r = subprocess.run(['docker', 'info'], capture_output=True, timeout=5)
        return r.returncode == 0
    except Exception:
        return False


def images_available():
    try:
        for image in ('prom/prometheus', 'grafana/grafana', 'prom/alertmanager'):
            r = subprocess.run(
                ['docker', 'image', 'inspect', image],
                capture_output=True, timeout=5)
            if r.returncode != 0:
                return False
        return True
    except Exception:
        return False


def http_get_json(url, timeout=2):
    """GET a URL and parse JSON response. Returns None on failure."""
    try:
        req = urllib.request.Request(url, method='GET')
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None


def http_post_json(url, body, timeout=5):
    """POST JSON to a URL and parse JSON response. Returns None on failure."""
    try:
        data = json.dumps(body).encode()
        req = urllib.request.Request(
            url, data=data, method='POST',
            headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None


def http_ok(url, timeout=2):
    """Check if a URL returns 200."""
    try:
        req = urllib.request.Request(url, method='GET')
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status == 200
    except Exception:
        return False


def wait_for(predicate, timeout=30, interval=1, msg="condition"):
    """Poll predicate() until True or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    raise TimeoutError(f"Timed out waiting for {msg} after {timeout}s")


def container_exists(name):
    """Check if a Docker container exists (running or stopped)."""
    r = subprocess.run(
        ['docker', 'ps', '-a', '-q', '-f', f'name=^{name}$'],
        capture_output=True, text=True, timeout=5)
    return bool(r.stdout.strip())


def container_running(name):
    """Check if a Docker container is running."""
    r = subprocess.run(
        ['docker', 'ps', '-q', '-f', f'name=^{name}$'],
        capture_output=True, text=True, timeout=5)
    return bool(r.stdout.strip())


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not docker_available(), reason="Docker not available"),
    pytest.mark.skipif(not images_available(), reason="Docker images not pulled"),
]


@pytest.fixture(scope="module")
def cluster_dir(tmp_path_factory):
    return str(tmp_path_factory.mktemp("ccm_monitoring_test"))


@pytest.fixture(scope="module")
def ports():
    return MonitoringStack.default_ports(CLUSTER_ID)


@pytest.fixture(scope="module")
def cluster(cluster_dir, ports):
    """Create a real ScyllaCluster with monitoring enabled."""
    cluster = ScyllaCluster(
        cluster_dir, "mon_test",
        version=SCYLLA_VERSION,
        verbose=True,
    )
    cluster.set_id(CLUSTER_ID)
    cluster.set_ipprefix(f'127.0.{CLUSTER_ID}.')
    cluster.monitoring_enabled = True
    cluster.grafana_port = ports['grafana_port']
    cluster.prometheus_port = ports['prometheus_port']
    cluster.alertmanager_port = ports['alertmanager_port']
    cluster.set_configuration_options(values={
        'skip_wait_for_gossip_to_settle': 0,
        'ring_delay_ms': 0,
    })
    cluster.populate(2)

    yield cluster

    # Final cleanup: nuke everything
    try:
        # Stop monitoring if still running
        if cluster.monitoring_stack:
            cluster.monitoring_stack.stop()
        else:
            # Force cleanup by name
            for svc in ('prometheus', 'grafana', 'alertmanager'):
                name = f'ccm-mon_test-{svc}'
                subprocess.run(['docker', 'rm', '-f', name],
                               capture_output=True, timeout=10)
    except Exception:
        pass  # Best-effort: test already finished, don't fail on cleanup
    try:
        cluster.stop(gently=False)
    except Exception:
        pass  # Best-effort: cluster may already be stopped
    try:
        common.rmdirs(cluster.get_path())
    except Exception:
        pass  # Best-effort: leftover dirs are acceptable


class TestMonitoringLifecycle:
    """Full lifecycle: start -> verify -> stop -> restart -> remove."""

    def test_01_start_cluster_with_monitoring(self, cluster, ports):
        """Start the cluster — monitoring should auto-start."""
        cluster.start(
            wait_for_binary_proto=True,
            wait_other_notice=True,
        )

        # Monitoring should have been started by _ensure_monitoring_started
        assert cluster.monitoring_stack is not None, \
            "monitoring_stack should be set after start()"
        assert cluster.monitoring_stack.is_running(), \
            "monitoring stack should be running after cluster start"

    def test_02_prometheus_is_healthy(self, ports):
        """Prometheus should be up and ready."""
        url = f"http://localhost:{ports['prometheus_port']}/-/ready"
        assert wait_for(lambda: http_ok(url), timeout=10, msg="Prometheus ready")

    def test_03_grafana_is_healthy(self, ports):
        """Grafana should be up and healthy."""
        url = f"http://localhost:{ports['grafana_port']}/api/health"
        assert wait_for(lambda: http_ok(url), timeout=10, msg="Grafana healthy")

    def test_04_alertmanager_is_healthy(self, ports):
        """Alertmanager should be up and healthy."""
        url = f"http://localhost:{ports['alertmanager_port']}/-/healthy"
        assert wait_for(lambda: http_ok(url), timeout=15, msg="Alertmanager healthy")

    def test_05_containers_have_correct_names(self, cluster):
        """Docker containers should be named ccm-{cluster}-{service}."""
        for svc in ('prometheus', 'grafana', 'alertmanager'):
            name = f'ccm-{cluster.name}-{svc}'
            assert container_running(name), \
                f"Container {name} should be running"

    def test_06_prometheus_has_targets(self, cluster, ports):
        """Prometheus should have the cluster nodes as targets."""
        url = f"http://localhost:{ports['prometheus_port']}/api/v1/targets"

        def targets_found():
            data = http_get_json(url)
            if not data or data.get('status') != 'success':
                return False
            active = data.get('data', {}).get('activeTargets', [])
            return len(active) >= 2  # We populated 2 nodes

        assert wait_for(targets_found, timeout=30, interval=2,
                        msg="Prometheus to discover targets")

        # Verify the target addresses
        data = http_get_json(url)
        active = data['data']['activeTargets']
        target_addrs = {t['labels'].get('instance', '') for t in active}
        # Nodes should be on 127.0.{CLUSTER_ID}.x:9180
        for addr in target_addrs:
            assert f'127.0.{CLUSTER_ID}.' in addr, \
                f"Target {addr} should be on 127.0.{CLUSTER_ID}.x"

    def test_07_prometheus_scrapes_data(self, ports):
        """Prometheus should actually scrape metrics from Scylla nodes."""
        prom_url = f"http://localhost:{ports['prometheus_port']}"

        def has_scylla_metrics():
            # Query for a basic Scylla metric
            url = f"{prom_url}/api/v1/query?query=up"
            data = http_get_json(url)
            if not data or data.get('status') != 'success':
                return False
            results = data.get('data', {}).get('result', [])
            # At least one target should have been scraped
            return any(r.get('value', [None, '0'])[1] == '1' for r in results)

        assert wait_for(has_scylla_metrics, timeout=60, interval=3,
                        msg="Prometheus to scrape Scylla metrics")

    def test_08_grafana_has_prometheus_datasource(self, ports):
        """Grafana should have Prometheus configured as a datasource."""
        url = f"http://localhost:{ports['grafana_port']}/api/datasources"
        data = http_get_json(url)
        assert data is not None, "Failed to query Grafana datasources API"
        assert len(data) >= 1, "Grafana should have at least 1 datasource"

        prom_ds = [d for d in data if d.get('type') == 'prometheus']
        assert len(prom_ds) >= 1, "Grafana should have a Prometheus datasource"
        assert str(ports['prometheus_port']) in prom_ds[0].get('url', ''), \
            "Datasource URL should point to our Prometheus port"

    def test_08a_grafana_has_scylla_dashboards(self, ports):
        """Grafana should have scylla-monitoring dashboards provisioned.

        The monitoring stack auto-clones scylla-monitoring and generates
        real dashboards from the templates (overview, cql, detailed, etc.).
        Falls back to a single built-in overview if generation failed.
        """
        url = f"http://localhost:{ports['grafana_port']}/api/search?type=dash-db"

        def dashboards_found():
            data = http_get_json(url)
            if not data:
                return False
            return len(data) >= 1

        assert wait_for(dashboards_found, timeout=15, interval=2,
                        msg="Grafana to load dashboards")

        data = http_get_json(url)
        titles = [d.get('title', '') for d in data]

        # If generation from scylla-monitoring worked we get many dashboards;
        # if it fell back we get at least the built-in overview.
        assert len(data) >= 1, f"Should have at least 1 dashboard, found: {titles}"

        # Fetch one dashboard and verify it has panels
        uid = data[0].get('uid', '')
        dash_url = f"http://localhost:{ports['grafana_port']}/api/dashboards/uid/{uid}"
        dash_data = http_get_json(dash_url)
        assert dash_data is not None, f"Failed to fetch dashboard {uid}"
        panels = dash_data.get('dashboard', {}).get('panels', [])
        assert len(panels) >= 1, \
            f"Dashboard '{data[0].get('title')}' should have panels, got {len(panels)}"

    def test_08a2_dashboard_datasources_resolve(self, ports):
        """Every datasource referenced in dashboards must exist in Grafana.

        This catches the 'Datasource prometheus not found' error that
        occurs when generated dashboards reference a datasource name or
        UID that doesn't match the provisioned datasource.
        """
        grafana = f"http://localhost:{ports['grafana_port']}"

        # Get all provisioned datasources (name→uid, uid→name)
        datasources = http_get_json(f"{grafana}/api/datasources")
        assert datasources, "Failed to fetch datasources"
        ds_by_name = {d['name']: d for d in datasources}
        ds_by_uid = {d['uid']: d for d in datasources}

        # Collect all datasource references from all dashboards
        search = http_get_json(f"{grafana}/api/search?type=dash-db")
        assert search, "No dashboards found"

        def collect_ds_refs(obj, refs):
            """Recursively collect all datasource values."""
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if k == 'datasource' and v is not None:
                        refs.append(v)
                    else:
                        collect_ds_refs(v, refs)
            elif isinstance(obj, list):
                for item in obj:
                    collect_ds_refs(item, refs)

        unresolved = []
        for entry in search:
            uid = entry.get('uid', '')
            dash = http_get_json(f"{grafana}/api/dashboards/uid/{uid}")
            if not dash:
                continue
            refs = []
            collect_ds_refs(dash.get('dashboard', {}), refs)
            for ref in refs:
                if isinstance(ref, str):
                    if ref not in ds_by_name:
                        unresolved.append(
                            f"Dashboard '{entry.get('title')}': "
                            f"string ref '{ref}' not in datasources "
                            f"{list(ds_by_name.keys())}")
                elif isinstance(ref, dict):
                    ds_uid = ref.get('uid')
                    ds_type = ref.get('type', '')
                    # Skip Grafana built-in datasources
                    if ds_type == 'datasource':
                        continue
                    if ds_uid and ds_uid not in ds_by_uid:
                        unresolved.append(
                            f"Dashboard '{entry.get('title')}': "
                            f"uid '{ds_uid}' (type={ds_type}) not in "
                            f"datasources {list(ds_by_uid.keys())}")

        assert not unresolved, (
            f"Found {len(unresolved)} unresolved datasource references:\n"
            + "\n".join(unresolved[:10])
        )

    def test_08b_grafana_dashboards_have_data(self, cluster, ports):
        """Grafana dashboard panels should return actual Scylla data.

        Queries key metrics through Grafana's datasource proxy, proving
        the full pipeline: Scylla nodes → Prometheus scrape → Grafana.
        """
        grafana = f"http://localhost:{ports['grafana_port']}"

        # Get the Prometheus datasource UID and numeric ID
        datasources = http_get_json(f"{grafana}/api/datasources")
        assert datasources, "Failed to fetch Grafana datasources"
        prom_ds = [d for d in datasources if d.get('type') == 'prometheus']
        assert prom_ds, "No Prometheus datasource found"
        ds_id = prom_ds[0]['id']

        # Fetch a dashboard and extract real PromQL expressions from panels
        search = http_get_json(f"{grafana}/api/search?type=dash-db")
        assert search, "No dashboards found in Grafana"

        panel_queries = []
        for dash_entry in search:
            uid = dash_entry.get('uid', '')
            dash_data = http_get_json(f"{grafana}/api/dashboards/uid/{uid}")
            if not dash_data:
                continue
            for panel in dash_data.get('dashboard', {}).get('panels', []):
                for target in panel.get('targets', []):
                    expr = target.get('expr', '').strip()
                    if expr:
                        panel_queries.append((
                            dash_entry.get('title', '?'),
                            panel.get('title', '?'),
                            expr,
                        ))
            if panel_queries:
                break  # one dashboard is enough

        assert panel_queries, "Could not extract any PromQL queries from dashboards"

        # Query each expression through Grafana's datasource proxy.
        # At least some panel queries must return data points.
        proxy_base = f"{grafana}/api/datasources/proxy/{ds_id}"
        queries_with_data = []

        for dash_title, panel_title, expr in panel_queries:
            url = f"{proxy_base}/api/v1/query?query={urllib.parse.quote(expr)}"
            data = http_get_json(url, timeout=5)
            if not data or data.get('status') != 'success':
                continue
            results = data.get('data', {}).get('result', [])
            if results:
                queries_with_data.append((panel_title, expr, len(results)))

        assert len(queries_with_data) >= 1, (
            f"No dashboard panel queries returned data. "
            f"Tried {len(panel_queries)} queries from '{panel_queries[0][0]}'. "
            f"Example query: {panel_queries[0][2]}"
        )

    def test_09_targets_file_matches_nodes(self, cluster):
        """The targets YAML file should list the cluster's running nodes."""
        targets_file = os.path.join(
            cluster.get_path(), 'monitoring', 'prometheus',
            'targets', 'scylla_servers.yml')
        assert os.path.exists(targets_file), "Targets file should exist"

        with open(targets_file) as f:
            targets = json.load(f)

        # Flatten all target addresses
        all_addrs = []
        for entry in targets:
            all_addrs.extend(entry['targets'])

        node_addrs = [n.address() for n in cluster.nodelist()]
        assert sorted(all_addrs) == sorted(node_addrs), \
            f"Targets {all_addrs} should match nodes {node_addrs}"

    def test_10_stop_cluster_stops_monitoring(self, cluster, ports):
        """Stopping the cluster should also stop monitoring."""
        cluster.stop()

        prom_url = f"http://localhost:{ports['prometheus_port']}/-/ready"
        grafana_url = f"http://localhost:{ports['grafana_port']}/api/health"

        # Wait for containers to actually stop
        def monitoring_stopped():
            return not http_ok(prom_url) and not http_ok(grafana_url)

        assert wait_for(monitoring_stopped, timeout=30,
                        msg="monitoring to stop")

        # Containers should be gone
        for svc in ('prometheus', 'grafana', 'alertmanager'):
            name = f'ccm-{cluster.name}-{svc}'
            assert not container_running(name), \
                f"Container {name} should not be running after stop"

    def test_11_start_cluster_restarts_monitoring(self, cluster, ports):
        """Starting the cluster again should restart monitoring."""
        cluster.start(
            wait_for_binary_proto=True,
            wait_other_notice=True,
        )

        assert cluster.monitoring_stack is not None
        assert cluster.monitoring_stack.is_running()

        # Verify services are back up
        prom_url = f"http://localhost:{ports['prometheus_port']}/-/ready"
        grafana_url = f"http://localhost:{ports['grafana_port']}/api/health"
        assert wait_for(lambda: http_ok(prom_url), timeout=30,
                        msg="Prometheus ready after restart")
        assert wait_for(lambda: http_ok(grafana_url), timeout=30,
                        msg="Grafana ready after restart")

    def test_12_remove_cluster_cleans_up_monitoring(self, cluster, ports):
        """Removing the cluster should stop and remove monitoring containers."""
        cluster_name = cluster.name

        # Remove the whole cluster (stop + delete)
        cluster.remove()

        # All containers should be gone
        for svc in ('prometheus', 'grafana', 'alertmanager'):
            name = f'ccm-{cluster_name}-{svc}'
            assert not container_running(name), \
                f"Container {name} should be removed after cluster.remove()"
            assert not container_exists(name), \
                f"Container {name} should not exist after cluster.remove()"

        # Monitoring endpoints should be unreachable
        prom_url = f"http://localhost:{ports['prometheus_port']}/-/ready"
        assert not http_ok(prom_url), \
            "Prometheus should not be reachable after remove"
