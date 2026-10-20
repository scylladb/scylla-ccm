"""Manages a scylla-monitoring stack for a CCM cluster."""

import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import time

import urllib.request
import urllib.error

from ccmlib import common
from ccmlib.common import logger

SCYLLA_MONITORING_REPO = 'https://github.com/scylladb/scylla-monitoring.git'
# Pinned scylla-monitoring commit (master as of 2026-10); image versions
# come from its versions.sh.
SCYLLA_MONITORING_COMMIT = '5a7eb141b6127e62d9e4d259d1d171566e8e963b'
# versions.sh of the pinned commit, for when there is no checkout to read.
_PINNED_IMAGE_VERSIONS = {
    'PROMETHEUS_VERSION': 'v3.14.0',
    'GRAFANA_VERSION': '13.2.2',
    'ALERT_MANAGER_VERSION': 'v0.34.1',
}
# Listen address of the stack; set to 0.0.0.0 to expose it on all interfaces.
BIND_ADDRESS_ENV = 'CCM_MONITORING_BIND_ADDRESS'

# Datasource UID used in built-in (fallback) dashboards.
_DATASOURCE_UID = 'ccm-prometheus'

def _default_monitoring_dir():
    """Default location where scylla-monitoring is cloned."""
    return os.path.join(common.get_default_path(), 'scylla-monitoring')


# ---------------------------------------------------------------------------
# Port availability
# ---------------------------------------------------------------------------

def _is_port_available(port, host='127.0.0.1'):
    """Check whether *port* on *host* is free to bind."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # Like the Go servers that will bind it: lingering TIME_WAIT
            # sockets from a previous stack must not count as "in use".
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((host, port))
            return True
    except OSError:
        return False


def _find_available_port(start_port, max_attempts=100, exclude=(), host='127.0.0.1'):
    """Return *start_port* if it is free, otherwise try successive ports.

    Ports in *exclude* (already picked for a sibling service) are skipped.
    Raises ``RuntimeError`` after *max_attempts* failed probes.
    """
    for offset in range(max_attempts):
        port = start_port + offset
        if port not in exclude and _is_port_available(port, host):
            return port
    raise RuntimeError(
        f"Could not find a free port starting from {start_port} "
        f"(tried {max_attempts} ports)")


# ---------------------------------------------------------------------------
# scylla-monitoring repo & dashboard generation
# ---------------------------------------------------------------------------

def _clone_monitoring_repo(target_dir):
    """Shallow-fetch scylla-monitoring at the pinned commit.  Returns *True* on success."""
    steps = [
        ['git', 'init', '-q', target_dir],
        ['git', '-C', target_dir, 'fetch', '-q', '--depth', '1',
         SCYLLA_MONITORING_REPO, SCYLLA_MONITORING_COMMIT],
        ['git', '-C', target_dir, 'checkout', '-q', 'FETCH_HEAD'],
    ]
    try:
        for cmd in steps:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode != 0:
                logger.warning(
                    f"{' '.join(cmd[:4])} failed "
                    f"(exit {result.returncode}): {result.stderr.strip()}")
                return False
        return True
    except FileNotFoundError:
        logger.warning("git is not installed; cannot clone scylla-monitoring")
    except subprocess.TimeoutExpired:
        logger.warning("git clone scylla-monitoring timed out")
    except Exception as e:
        logger.warning(f"Failed to clone scylla-monitoring: {e}")
    return False


_DS_OBJ = {"type": "prometheus", "uid": _DATASOURCE_UID}


def _fix_dashboard_datasources(dashboard):
    """Walk a dashboard dict and normalise every Prometheus datasource
    reference to the explicit ``{"type": "prometheus", "uid": "<ours>"}``
    object form.

    String-based references (``"datasource": "prometheus"``) are unreliable
    in modern Grafana when dashboards are loaded via file provisioning, so
    every prometheus ref is rewritten to the UID-based object.

    Non-prometheus datasources (``null``, ``"grafana"``, etc.) are left
    untouched.
    """
    if isinstance(dashboard, dict):
        for key in list(dashboard.keys()):
            val = dashboard[key]
            if key == 'datasource':
                if val == 'prometheus':
                    dashboard[key] = dict(_DS_OBJ)
                elif isinstance(val, dict) and val.get('type') == 'prometheus':
                    val['uid'] = _DATASOURCE_UID
                elif isinstance(val, dict) and val.get('name') == 'prometheus':
                    # Schema v2 (DataQuery) refs by "name", which Grafana uses as the uid.
                    val['name'] = _DATASOURCE_UID
                # null, "grafana", other types → leave alone
            else:
                _fix_dashboard_datasources(val)
    elif isinstance(dashboard, list):
        for item in dashboard:
            _fix_dashboard_datasources(item)


def _postprocess_dashboards(output_dir):
    """Fix datasource references in all generated dashboard JSON files."""
    for fname in os.listdir(output_dir):
        if not fname.endswith('.json'):
            continue
        fpath = os.path.join(output_dir, fname)
        try:
            with open(fpath, 'r') as f:
                dashboard = json.load(f)
            _fix_dashboard_datasources(dashboard)
            with open(fpath, 'w') as f:
                json.dump(dashboard, f, indent=2)
                f.write('\n')
        except Exception as e:
            logger.warning(f"Failed to post-process dashboard {fname}: {e}")


def _dashboard_version(cluster):
    """Return ``major.minor`` of the cluster's Scylla version, else ``master``."""
    try:
        m = re.match(r'(\d+\.\d+)', str(cluster.version()))
    except Exception:
        m = None
    return m.group(1) if m else 'master'


def _generate_dashboards(monitoring_dir, output_dir, scylla_version='master'):
    """Run *make_dashboards.py* from a scylla-monitoring checkout to produce
    real Grafana dashboard JSON files in *output_dir*.

    *scylla_version* is the dashboard version (e.g. ``2026.1``), or ``master``.
    Returns the number of dashboards generated (0 on total failure).
    """
    make_dashboards = os.path.join(monitoring_dir, 'make_dashboards.py')
    types_file = os.path.join(monitoring_dir, 'grafana', 'types.json')
    templates_dir = os.path.join(monitoring_dir, 'grafana')

    if not os.path.isfile(make_dashboards) or not os.path.isfile(types_file):
        logger.warning(
            f"scylla-monitoring checkout at {monitoring_dir} is missing "
            f"make_dashboards.py or grafana/types.json")
        return 0

    os.makedirs(output_dir, exist_ok=True)
    generated = 0

    for fname in sorted(os.listdir(templates_dir)):
        if not fname.endswith('.template.json'):
            continue
        template_path = os.path.join(templates_dir, fname)
        try:
            cmd = [
                sys.executable, make_dashboards,
                '-t', types_file,
                '-d', template_path,
                '-af', output_dir,
                # Left literal, Grafana rejects the dashboard (as generate-dashboards.sh sets them).
                '-R', '__REFRESH_INTERVAL__=5m',
                '-R', '__MONITOR_VERSION__=master',
                '-R', '__MONITOR_BRANCH_VERSION=master',
                # make_dashboards.py derives __SCYLLA_VERSION_DASHED__ from this one.
                '-R', f'__SCYLLA_VERSION_DOT__={scylla_version}',
                '-V', scylla_version,
            ]
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                generated += 1
            else:
                logger.warning(
                    f"make_dashboards.py failed for {fname}: "
                    f"{result.stderr.strip()}")
        except Exception as e:
            logger.warning(f"make_dashboards.py failed for {fname}: {e}")

    if generated:
        _postprocess_dashboards(output_dir)

    return generated


# ---------------------------------------------------------------------------
# Fallback: minimal built-in overview dashboard
# ---------------------------------------------------------------------------

def _make_panel(title, expr, legend, unit, grid_pos, datasource, y_max=None):
    """Build a Grafana timeseries panel definition."""
    panel = {
        "type": "timeseries",
        "title": title,
        "gridPos": {"x": grid_pos[0], "y": grid_pos[1],
                     "w": grid_pos[2], "h": grid_pos[3]},
        "targets": [{
            "expr": expr,
            "legendFormat": legend,
            "datasource": datasource,
        }],
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "custom": {
                    "drawStyle": "line",
                    "fillOpacity": 10,
                    "lineWidth": 1,
                    "showPoints": "never",
                },
            },
            "overrides": [],
        },
        "options": {
            "tooltip": {"mode": "multi"},
            "legend": {"displayMode": "table", "placement": "bottom"},
        },
    }
    if y_max is not None:
        panel["fieldConfig"]["defaults"]["max"] = y_max
    return panel


def _scylla_overview_dashboard():
    """Return a minimal built-in Scylla Overview dashboard (fallback)."""
    ds = {"type": "prometheus", "uid": _DATASOURCE_UID}
    return {
        "id": None,
        "uid": "scylla-overview-ccm",
        "title": "Scylla Overview",
        "tags": ["scylla", "ccm"],
        "editable": True,
        "graphTooltip": 1,
        "schemaVersion": 39,
        "version": 1,
        "time": {"from": "now-1h", "to": "now"},
        "refresh": "30s",
        "panels": [
            _make_panel(
                title="CPU Utilization",
                expr="avg by (instance) (scylla_reactor_utilization)",
                legend="{{instance}}",
                unit="percentunit", y_max=1,
                grid_pos=(0, 0, 12, 8), datasource=ds,
            ),
            _make_panel(
                title="CQL Requests / sec",
                expr="sum by (instance) (rate(scylla_transport_requests_served[1m]))",
                legend="{{instance}}",
                unit="ops",
                grid_pos=(12, 0, 12, 8), datasource=ds,
            ),
            _make_panel(
                title="Memory Free",
                expr="sum by (instance) (scylla_memory_free_memory)",
                legend="{{instance}}",
                unit="bytes",
                grid_pos=(0, 8, 12, 8), datasource=ds,
            ),
            _make_panel(
                title="Active CQL Connections",
                expr="sum by (instance) (scylla_transport_current_connections)",
                legend="{{instance}}",
                unit="short",
                grid_pos=(12, 8, 12, 8), datasource=ds,
            ),
            _make_panel(
                title="Total Reads / sec",
                expr="sum by (instance) (rate(scylla_cql_reads[1m]))",
                legend="{{instance}}",
                unit="ops",
                grid_pos=(0, 16, 12, 8), datasource=ds,
            ),
            _make_panel(
                title="Total Writes / sec",
                expr="sum by (instance) (rate(scylla_cql_inserts[1m]))",
                legend="{{instance}}",
                unit="ops",
                grid_pos=(12, 16, 12, 8), datasource=ds,
            ),
        ],
    }


_BUILTIN_DASHBOARDS = {
    'scylla-overview.json': _scylla_overview_dashboard,
}


# ---------------------------------------------------------------------------
# Resolve monitoring directory
# ---------------------------------------------------------------------------

def _resolve_monitoring_dir(monitoring_dir=None, cluster_dir=None):
    """Resolve the scylla-monitoring directory from multiple sources.

    Checks in order:
    1. Explicit monitoring_dir argument
    2. SCYLLA_MONITORING_DIR environment variable
    3. <cluster_dir>/scylla-monitoring/ (if cluster_dir provided)
    4. <ccm config dir>/scylla-monitoring/

    Returns the resolved path, or None if not found.
    """
    # Absolute: a relative path in a -v mount is taken as a named volume.
    if monitoring_dir and os.path.isdir(monitoring_dir):
        return os.path.abspath(monitoring_dir)

    env_dir = os.environ.get('SCYLLA_MONITORING_DIR')
    if env_dir and os.path.isdir(env_dir):
        return os.path.abspath(env_dir)

    if cluster_dir:
        cluster_local = os.path.join(cluster_dir, 'scylla-monitoring')
        if os.path.isdir(cluster_local):
            return cluster_local

    default_dir = _default_monitoring_dir()
    # A failed clone leaves the dir without grafana/; ignore it so the clone is retried.
    if os.path.isdir(os.path.join(default_dir, 'grafana')):
        return default_dir

    return None


# ---------------------------------------------------------------------------
# MonitoringStack
# ---------------------------------------------------------------------------

class MonitoringStack:
    """Manages a scylla-monitoring stack for a CCM cluster.

    Runs Prometheus, Grafana, and Alertmanager as Docker containers
    with --net=host, bound to 127.0.0.1 by default (CCM_MONITORING_BIND_ADDRESS
    overrides), with per-service ports to allow multiple simultaneous stacks.
    """

    def __init__(self, cluster, monitoring_dir=None, grafana_port=3000,
                 prometheus_port=9090, alertmanager_port=9093,
                 start_alertmanager=True, bind_address=None):
        self.cluster = cluster
        self.bind_address = bind_address or os.environ.get(BIND_ADDRESS_ENV) or '127.0.0.1'
        self.monitoring_dir = _resolve_monitoring_dir(monitoring_dir)
        self.grafana_port = grafana_port
        self.prometheus_port = prometheus_port
        self.alertmanager_port = alertmanager_port
        # Alertmanager only matters if something is configured to route
        # alerts somewhere; skip it for ad-hoc/test clusters that just want
        # Grafana dashboards.
        self.start_alertmanager = start_alertmanager
        self._working_dir = os.path.join(cluster.get_path(), 'monitoring')
        self._home_dashboard_filename = None

    @staticmethod
    def default_ports(cluster_id=0):
        """Compute default ports offset by cluster ID to avoid conflicts."""
        return {
            'grafana_port': 3000 + cluster_id * 10,
            'prometheus_port': 9090 + cluster_id * 10,
            'alertmanager_port': 9093 + cluster_id * 10,
        }

    def _image(self, repo, var):
        """Image *repo* tagged with the version *var* pinned in versions.sh."""
        try:
            with open(os.path.join(self.monitoring_dir, 'versions.sh')) as f:
                m = re.search(rf'^{var}=["\']?([^"\'\s]+)', f.read(), re.M)
        except (OSError, TypeError):  # no checkout (built-in dashboards)
            m = None
        return f'docker.io/{repo}:{m.group(1) if m else _PINNED_IMAGE_VERSIONS[var]}'

    def _runtime(self):
        """Container runtime of the owning cluster (podman clusters), else docker."""
        if getattr(self.cluster, 'is_podman', lambda: False)() is True:
            return 'podman'
        get_client = getattr(self.cluster, 'get_container_client', None)
        name = get_client().runtime_name if get_client else None
        return name if isinstance(name, str) else 'docker'

    def _is_podman(self):
        rt = self._runtime()
        if rt == 'podman':
            return True
        try:
            out = subprocess.run([rt, '--version'], capture_output=True, text=True, timeout=30).stdout
        except (OSError, subprocess.SubprocessError):
            return False
        return 'podman' in out.lower()

    def _dial_host(self):
        """Address clients use to reach the stack: the bind address, or loopback
        when bound to all interfaces (not "localhost", which may resolve to ::1)."""
        return '127.0.0.1' if self.bind_address in ('0.0.0.0', '::') else self.bind_address

    def _container_name(self, service):
        """Return the Docker container name for a service."""
        # Include the ccm dir so same-named clusters (e.g. dtest's "test") don't collide.
        dir_name = os.path.basename(os.path.dirname(self.cluster.get_path())).lstrip('.')
        return f'ccm-{dir_name}-{self.cluster.name}-{service}'

    # ------------------------------------------------------------------
    # Port resolution
    # ------------------------------------------------------------------

    def _resolve_ports(self):
        """Ensure every service port is available, bumping to the next
        free port when necessary.  Updates both *self* and the owning
        cluster so the chosen ports are persisted."""
        resolved = {}
        for attr in ('grafana_port', 'prometheus_port', 'alertmanager_port'):
            requested = getattr(self, attr)
            actual = _find_available_port(requested, exclude=set(resolved.values()), host=self.bind_address)
            if actual != requested:
                logger.info(
                    f"Port {requested} in use, using {actual} for "
                    f"{attr.replace('_port', '')}")
            resolved[attr] = actual
        # Apply and persist only once every port resolved, so a failure
        # leaves the saved config untouched.
        changed = False
        for attr, actual in resolved.items():
            changed |= actual != getattr(self.cluster, attr, actual)
            setattr(self, attr, actual)
            if hasattr(self.cluster, attr):
                setattr(self.cluster, attr, actual)
        if changed:
            self.cluster._update_config()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        """Start the monitoring stack containers.

        Writes initial targets, generates Prometheus and Grafana configs,
        starts containers via docker run, and blocks until healthy.

        Before starting, each service port is checked for availability.
        If a port is already in use the next free port is chosen
        automatically and the cluster object is updated so the new port
        is persisted.
        """
        # Remove our own stale containers first so their ports don't look taken.
        self._stop_containers()
        self._resolve_ports()

        # Containers run as non-root uids: mounts must be world-readable, data dirs writable.
        targets_dir = os.path.join(self._working_dir, 'prometheus', 'targets')
        data_dir = os.path.join(self._working_dir, 'data', 'prometheus_data')
        os.makedirs(targets_dir, exist_ok=True)
        os.makedirs(data_dir, exist_ok=True)
        os.chmod(data_dir, 0o777)
        os.chmod(targets_dir, 0o777)

        # Ensure we have a scylla-monitoring checkout for dashboards
        if not self.monitoring_dir:
            self._ensure_monitoring_repo()

        # Generate and write initial targets
        self.update_targets()

        # Generate prometheus.yml
        self._write_prometheus_config()

        # Generate Grafana provisioning + dashboards
        self._write_grafana_provisioning()

        # Start containers
        self._start_prometheus(targets_dir, data_dir)
        self._start_grafana()
        if self.start_alertmanager:
            self._start_alertmanager()

        self._wait_until_ready(timeout=60)
        logger.info(
            f"Monitoring started: Grafana {self.grafana_url()}, "
            f"Prometheus {self.prometheus_url()}"
        )

    def stop(self):
        """Stop all monitoring stack containers."""
        logger.info(f"Stopping monitoring stack for cluster {self.cluster.name}")
        self._stop_containers()

    # ------------------------------------------------------------------
    # Repo / dashboard generation
    # ------------------------------------------------------------------

    def _ensure_monitoring_repo(self):
        """Clone scylla-monitoring to the default location if not present."""
        default_dir = _default_monitoring_dir()
        if os.path.isdir(os.path.join(default_dir, 'grafana')):
            self.monitoring_dir = default_dir
            return

        logger.info(
            f"Cloning scylla-monitoring to {default_dir} "
            f"(one-time setup for dashboards)...")
        if _clone_monitoring_repo(default_dir):
            self.monitoring_dir = default_dir
        else:
            logger.warning(
                "Could not clone scylla-monitoring; "
                "using built-in fallback dashboard")

    # ------------------------------------------------------------------
    # Config generation
    # ------------------------------------------------------------------

    def _write_prometheus_config(self):
        """Write a minimal prometheus.yml configuration."""
        config_dir = os.path.join(self._working_dir, 'prometheus')
        os.makedirs(config_dir, exist_ok=True)
        config_file = os.path.join(config_dir, 'prometheus.yml')

        content = (
            "global:\n"
            "  scrape_interval: 20s\n"
            "  evaluation_interval: 20s\n"
            "\n"
            "scrape_configs:\n"
            "  - job_name: 'scylla'\n"
            "    file_sd_configs:\n"
            "      - files:\n"
            "          - '/etc/prometheus/targets/scylla_servers.yml'\n"
            "        refresh_interval: 10s\n"
            "    relabel_configs:\n"
            "      - source_labels: [__address__]\n"
            "        regex: '(.*)'\n"
            "        target_label: __address__\n"
            "        replacement: '${1}:9180'\n"
            "      - source_labels: [__address__]\n"
            "        target_label: instance\n"
        )

        with open(config_file, 'w') as f:
            f.write(content)

    def _write_grafana_provisioning(self):
        """Write Grafana provisioning files for datasource and dashboards.

        Attempts to generate the full set of scylla-monitoring dashboards
        from templates.  Falls back to a minimal built-in overview dashboard
        when generation is not possible (no repo, missing pyyaml, etc.).
        """
        # --- datasource ---
        datasources_dir = os.path.join(
            self._working_dir, 'grafana', 'provisioning', 'datasources')
        os.makedirs(datasources_dir, exist_ok=True)

        datasource_file = os.path.join(datasources_dir, 'prometheus.yml')
        content = (
            "apiVersion: 1\n"
            "datasources:\n"
            "  - name: prometheus\n"
            f"    uid: {_DATASOURCE_UID}\n"
            "    type: prometheus\n"
            "    access: proxy\n"
            f"    url: http://{self._dial_host()}:{self.prometheus_port}\n"
            "    isDefault: true\n"
        )
        with open(datasource_file, 'w') as f:
            f.write(content)

        # --- dashboards ---
        dashboards_dir = os.path.join(self._working_dir, 'grafana', 'dashboards')
        os.makedirs(dashboards_dir, exist_ok=True)
        # File names carry the Scylla version; stale ones would duplicate dashboards.
        for fname in os.listdir(dashboards_dir):
            if fname.endswith('.json'):
                os.unlink(os.path.join(dashboards_dir, fname))

        generated = 0

        # 1. Generate from scylla-monitoring templates (the real dashboards)
        if self.monitoring_dir:
            generated = _generate_dashboards(
                self.monitoring_dir, dashboards_dir, _dashboard_version(self.cluster))
            if generated:
                logger.info(
                    f"Generated {generated} dashboards from scylla-monitoring")

        # 2. Fallback: write minimal built-in dashboards
        if not generated:
            logger.info("Using built-in fallback dashboards")
            self._write_builtin_dashboards(dashboards_dir)

        # Land on the Overview dashboard instead of Grafana's empty default
        # Home page -- pick the generated "overview" file, or the built-in
        # fallback's fixed name if templates weren't available.
        for fname in sorted(os.listdir(dashboards_dir)):
            if fname.startswith('scylla-overview') and fname.endswith('.json'):
                self._home_dashboard_filename = fname
                break

        # --- dashboard provisioning config ---
        prov_dashboards_dir = os.path.join(
            self._working_dir, 'grafana', 'provisioning', 'dashboards')
        os.makedirs(prov_dashboards_dir, exist_ok=True)

        dashboard_config = os.path.join(prov_dashboards_dir, 'dashboards.yml')
        config_content = (
            "apiVersion: 1\n"
            "providers:\n"
            "  - name: 'scylla'\n"
            "    type: file\n"
            "    allowUiUpdates: true\n"
            "    options:\n"
            "      path: /var/lib/grafana/dashboards\n"
        )
        with open(dashboard_config, 'w') as f:
            f.write(config_content)

    def _write_builtin_dashboards(self, dest_dir):
        """Write built-in Scylla dashboard JSON files to *dest_dir*."""
        for filename, dashboard_fn in _BUILTIN_DASHBOARDS.items():
            filepath = os.path.join(dest_dir, filename)
            with open(filepath, 'w') as f:
                json.dump(dashboard_fn(), f, indent=2)
                f.write('\n')

    # ------------------------------------------------------------------
    # Container management
    # ------------------------------------------------------------------

    def _start_prometheus(self, targets_dir, data_dir):
        """Start the Prometheus container."""
        name = self._container_name('prometheus')
        prometheus_config = os.path.join(
            self._working_dir, 'prometheus', 'prometheus.yml')

        cmd = [
            self._runtime(), 'run', '-d',
            '--name', name,
            '--net=host',
        ]
        # Run as the host uid so the data dir stays deletable by it
        # (keep-id: rootless podman, incl. the `docker` emulation shim).
        if self._is_podman():
            cmd.append('--userns=keep-id')
        cmd.extend(['--user', f'{os.getuid()}:{os.getgid()}'])
        cmd.extend([
            '-v', f'{prometheus_config}:/etc/prometheus/prometheus.yml:z',
            '-v', f'{targets_dir}:/etc/prometheus/targets:z',
            '-v', f'{data_dir}:/prometheus:z',
            self._image('prom/prometheus', 'PROMETHEUS_VERSION'),
            f'--web.listen-address={self.bind_address}:{self.prometheus_port}',
            '--config.file=/etc/prometheus/prometheus.yml',
            '--storage.tsdb.path=/prometheus',
        ])

        logger.info(f"Starting Prometheus: {' '.join(cmd)}")
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=600)  # includes first image pull
            if result.returncode != 0:
                raise RuntimeError(
                    f"Failed to start Prometheus container (exit {result.returncode}):\n"
                    f"stdout: {result.stdout}\nstderr: {result.stderr}")
        except subprocess.TimeoutExpired:
            raise RuntimeError("Docker run for Prometheus timed out")

    def _start_grafana(self):
        """Start the Grafana container."""
        name = self._container_name('grafana')
        provisioning_dir = os.path.join(
            self._working_dir, 'grafana', 'provisioning')
        dashboards_dir = os.path.join(
            self._working_dir, 'grafana', 'dashboards')

        cmd = [
            self._runtime(), 'run', '-d',
            '--name', name,
            '--net=host',
        ]
        # Host uid so Grafana can write the mounted plugins dir; else its bundled-plugin
        # update fails and leaves prometheus unregistered (as scylla-monitoring does).
        if self._is_podman():
            cmd.append('--userns=keep-id')
        cmd.extend(['--user', f'{os.getuid()}:{os.getgid()}'])
        cmd.extend([
            '-e', f'GF_SERVER_HTTP_ADDR={self.bind_address}',
            '-e', f'GF_SERVER_HTTP_PORT={self.grafana_port}',
            '-e', 'GF_AUTH_ANONYMOUS_ENABLED=true',
            '-e', 'GF_AUTH_ANONYMOUS_ORG_ROLE=Admin',
            '-v', f'{provisioning_dir}:/etc/grafana/provisioning:z',
            '-v', f'{dashboards_dir}:/var/lib/grafana/dashboards:z',
        ])
        if self._home_dashboard_filename:
            cmd.extend([
                '-e',
                'GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH='
                f'/var/lib/grafana/dashboards/{self._home_dashboard_filename}',
            ])

        # Mount scylla plugin if available
        if self.monitoring_dir:
            plugins_dir = os.path.join(self.monitoring_dir, 'grafana', 'plugins')
            if os.path.isdir(plugins_dir):
                cmd.extend([
                    '-v', f'{plugins_dir}:/var/lib/grafana/plugins:z',
                    '-e', 'GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=scylladb-scylla-datasource',
                ])

        cmd.append(self._image('grafana/grafana', 'GRAFANA_VERSION'))

        logger.info(f"Starting Grafana: {' '.join(cmd)}")
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=600)  # includes first image pull
            if result.returncode != 0:
                raise RuntimeError(
                    f"Failed to start Grafana container (exit {result.returncode}):\n"
                    f"stdout: {result.stdout}\nstderr: {result.stderr}")
        except subprocess.TimeoutExpired:
            raise RuntimeError("Docker run for Grafana timed out")

    def _write_alertmanager_config(self):
        """Write a minimal alertmanager.yml configuration."""
        config_dir = os.path.join(self._working_dir, 'alertmanager')
        os.makedirs(config_dir, exist_ok=True)
        config_file = os.path.join(config_dir, 'alertmanager.yml')

        content = (
            "route:\n"
            "  receiver: 'default'\n"
            "receivers:\n"
            "  - name: 'default'\n"
        )

        with open(config_file, 'w') as f:
            f.write(content)

    def _start_alertmanager(self):
        """Start the Alertmanager container."""
        self._write_alertmanager_config()
        name = self._container_name('alertmanager')
        alertmanager_config = os.path.join(
            self._working_dir, 'alertmanager', 'alertmanager.yml')

        cmd = [
            self._runtime(), 'run', '-d',
            '--name', name,
            '--net=host',
        ]
        cmd.extend([
            '-v', f'{alertmanager_config}:/etc/alertmanager/alertmanager.yml:z',
            self._image('prom/alertmanager', 'ALERT_MANAGER_VERSION'),
            f'--web.listen-address={self.bind_address}:{self.alertmanager_port}',
            # clustering port would otherwise listen on all interfaces
            '--cluster.listen-address=',
            '--config.file=/etc/alertmanager/alertmanager.yml',
        ])

        logger.info(f"Starting Alertmanager: {' '.join(cmd)}")
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=600)  # includes first image pull
            if result.returncode != 0:
                raise RuntimeError(
                    f"Failed to start Alertmanager container (exit {result.returncode}):\n"
                    f"stdout: {result.stdout}\nstderr: {result.stderr}")
        except subprocess.TimeoutExpired:
            raise RuntimeError("Docker run for Alertmanager timed out")

    def _stop_containers(self):
        """Stop and remove all monitoring containers for this cluster."""
        for service in ('prometheus', 'grafana', 'alertmanager'):
            name = self._container_name(service)
            try:
                subprocess.run(
                    [self._runtime(), 'stop', name],
                    capture_output=True, timeout=30)
            except Exception as e:
                logger.debug("Failed to stop container %s: %s", name, e)
            try:
                subprocess.run(
                    [self._runtime(), 'rm', '-f', name],
                    capture_output=True, timeout=10)
            except Exception as e:
                logger.debug("Failed to remove container %s: %s", name, e)

    # ------------------------------------------------------------------
    # Health / status
    # ------------------------------------------------------------------

    def is_running(self):
        """Check whether all of our own containers are running (not an HTTP
        probe: another service may own the port), so a half-started stack is restarted."""
        services = ['prometheus', 'grafana'] + (['alertmanager'] if self.start_alertmanager else [])
        running = self._running_containers()
        return all(self._container_name(svc) in running for svc in services)

    def has_containers(self):
        """True if any of our containers runs, e.g. a half-started stack that must be torn down."""
        return any(self._container_name(svc) in self._running_containers()
                   for svc in ('prometheus', 'grafana', 'alertmanager'))

    def _running_containers(self):
        try:
            result = subprocess.run(
                [self._runtime(), 'ps', '--format', '{{.Names}}'],
                capture_output=True, text=True, timeout=5)
        except Exception:
            return set()
        return set(result.stdout.split())

    def update_targets(self):
        """Regenerate scylla_servers.yml from current cluster state."""
        targets = self._generate_targets_yaml()
        self._write_targets_file(targets)

    def _generate_targets_yaml(self):
        """Build the Prometheus targets list from cluster nodes.

        Output format (matches scylla-monitoring's scylla_servers.yml):
        [
            {
                "targets": ["127.0.0.1"],
                "labels": {"cluster": "test", "dc": "dc1"}
            }
        ]

        Includes any running node that is not decommissioned, regardless of gossip
        status - the IP is known and scrapable as soon as the process starts,
        without waiting for it to join the ring.
        """
        from ccmlib.node import Status
        dc_targets = {}
        for node in self.cluster.nodelist():
            if node.status == Status.DECOMMISSIONED or not node.is_running():
                continue
            dc = node.data_center or "datacenter1"
            dc_targets.setdefault(dc, []).append(node.address())

        return [
            {"targets": targets, "labels": {"cluster": self.cluster.name, "dc": dc}}
            for dc, targets in dc_targets.items()
        ]

    def _write_targets_file(self, targets):
        """Atomic write targets to the file that Prometheus watches."""
        targets_dir = os.path.join(self._working_dir, 'prometheus', 'targets')
        os.makedirs(targets_dir, exist_ok=True)

        targets_file = os.path.join(targets_dir, 'scylla_servers.yml')

        # Write to temp file then atomically rename.
        # Prometheus runs as nobody (uid 65534) so the file must be
        # world-readable.
        fd, tmp_path = tempfile.mkstemp(
            dir=targets_dir, prefix='.scylla_servers_', suffix='.yml.tmp'
        )
        try:
            with os.fdopen(fd, 'w') as f:
                json.dump(targets, f, indent=2)
                f.write('\n')
            os.chmod(tmp_path, 0o644)
            os.replace(tmp_path, targets_file)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass  # Best-effort cleanup of temp file
            raise

    def _wait_until_ready(self, timeout=60):
        """Block until Prometheus and Grafana health endpoints respond.

        Raises TimeoutError if not ready within timeout seconds.
        """
        deadline = time.time() + timeout

        # Wait for Prometheus
        while time.time() < deadline:
            try:
                url = f"http://{self._dial_host()}:{self.prometheus_port}/-/ready"
                req = urllib.request.Request(url, method='GET')
                with urllib.request.urlopen(req, timeout=2) as resp:
                    if resp.status == 200:
                        break
            except Exception:
                logger.debug("Prometheus not ready yet at port %s", self.prometheus_port)
            time.sleep(1)
        else:
            raise TimeoutError(
                f"Prometheus not ready at {self._dial_host()}:{self.prometheus_port} "
                f"after {timeout}s"
            )

        # Wait for Grafana
        while time.time() < deadline:
            try:
                url = f"http://{self._dial_host()}:{self.grafana_port}/api/health"
                req = urllib.request.Request(url, method='GET')
                with urllib.request.urlopen(req, timeout=2) as resp:
                    if resp.status == 200:
                        break
            except Exception:
                logger.debug("Grafana not ready yet at port %s", self.grafana_port)
            time.sleep(1)
        else:
            raise TimeoutError(
                f"Grafana not ready at {self._dial_host()}:{self.grafana_port} "
                f"after {timeout}s"
            )

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------

    def grafana_address(self):
        """Return (host, port) tuple for Grafana."""
        return (self._dial_host(), self.grafana_port)

    def prometheus_address(self):
        """Return (host, port) tuple for Prometheus."""
        return (self._dial_host(), self.prometheus_port)

    def grafana_url(self):
        host, port = self.grafana_address()
        return f"http://{host}:{port}"

    def prometheus_url(self):
        host, port = self.prometheus_address()
        return f"http://{host}:{port}"
