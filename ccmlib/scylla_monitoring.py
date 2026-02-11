"""Manages a scylla-monitoring stack for a CCM cluster."""

import json
import os
import subprocess
import tempfile
import time

import urllib.request
import urllib.error

from ccmlib.common import logger


def _resolve_monitoring_dir(monitoring_dir=None):
    """Resolve the scylla-monitoring directory from multiple sources.

    Checks in order:
    1. Explicit monitoring_dir argument
    2. SCYLLA_MONITORING_DIR environment variable
    3. ~/.ccm/scylla-monitoring/

    Returns the resolved path, or None if not found.
    """
    if monitoring_dir and os.path.isdir(monitoring_dir):
        return monitoring_dir

    env_dir = os.environ.get('SCYLLA_MONITORING_DIR')
    if env_dir and os.path.isdir(env_dir):
        return env_dir

    default_dir = os.path.expanduser('~/.ccm/scylla-monitoring')
    if os.path.isdir(default_dir):
        return default_dir

    return None


class MonitoringStack:
    """Manages a scylla-monitoring stack for a CCM cluster."""

    def __init__(self, cluster, monitoring_dir, grafana_port=3000,
                 prometheus_port=9090, alertmanager_port=9093):
        self.cluster = cluster
        self.monitoring_dir = _resolve_monitoring_dir(monitoring_dir)
        self.grafana_port = grafana_port
        self.prometheus_port = prometheus_port
        self.alertmanager_port = alertmanager_port
        self._working_dir = os.path.join(cluster.get_path(), 'monitoring')

    def start(self):
        """Start the monitoring stack containers.

        Writes initial targets, starts containers via start-all.sh,
        and blocks until healthy. Raises if monitoring fails to become ready.
        """
        if not self.monitoring_dir:
            raise EnvironmentError(
                "Cannot start monitoring: scylla-monitoring directory not found. "
                "Set SCYLLA_MONITORING_DIR or pass --monitoring-dir."
            )

        start_all = os.path.join(self.monitoring_dir, 'start-all.sh')
        if not os.path.isfile(start_all):
            raise EnvironmentError(
                f"start-all.sh not found in {self.monitoring_dir}. "
                "Is this a valid scylla-monitoring checkout?"
            )

        # Create working directory structure
        targets_dir = os.path.join(self._working_dir, 'prometheus', 'targets')
        data_dir = os.path.join(self._working_dir, 'data', 'prometheus_data')
        os.makedirs(targets_dir, exist_ok=True)
        os.makedirs(data_dir, exist_ok=True)

        # Generate and write initial targets
        self.update_targets()

        # Write empty scylla_manager_servers.yml (required by start-all.sh)
        manager_targets_file = os.path.join(targets_dir, 'scylla_manager_servers.yml')
        if not os.path.exists(manager_targets_file):
            with open(manager_targets_file, 'w') as f:
                f.write('[]')

        # Build start-all.sh command
        scylla_targets = os.path.join(targets_dir, 'scylla_servers.yml')
        cmd = [
            start_all,
            '-s', scylla_targets,
            '-d', data_dir,
            '-l',            # host networking (needed for 127.0.0.x nodes)
            '--no-renderer',
            '-b', f'ccm-{self.cluster.name}',  # unique stack ID per cluster
        ]

        if self.grafana_port != 3000:
            cmd.extend(['-G', str(self.grafana_port)])
        if self.prometheus_port != 9090:
            cmd.extend(['-p', str(self.prometheus_port)])

        logger.info(f"Starting monitoring stack: {' '.join(cmd)}")

        try:
            result = subprocess.run(
                cmd, cwd=self.monitoring_dir,
                capture_output=True, text=True, timeout=120
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"start-all.sh failed (exit {result.returncode}):\n"
                    f"stdout: {result.stdout}\nstderr: {result.stderr}"
                )
        except subprocess.TimeoutExpired:
            raise RuntimeError("start-all.sh timed out after 120 seconds")

        self._wait_until_ready(timeout=60)
        logger.info(
            f"Monitoring started: Grafana {self.grafana_url()}, "
            f"Prometheus {self.prometheus_url()}"
        )

    def stop(self):
        """Stop all monitoring stack containers."""
        if not self.monitoring_dir:
            return

        kill_all = os.path.join(self.monitoring_dir, 'kill-all.sh')
        if not os.path.isfile(kill_all):
            logger.warning(f"kill-all.sh not found in {self.monitoring_dir}")
            return

        cmd = [kill_all, '-b', f'ccm-{self.cluster.name}']
        logger.info(f"Stopping monitoring stack: {' '.join(cmd)}")

        try:
            subprocess.run(
                cmd, cwd=self.monitoring_dir,
                capture_output=True, text=True, timeout=60
            )
        except subprocess.TimeoutExpired:
            logger.warning("kill-all.sh timed out after 60 seconds")
        except Exception as e:
            logger.warning(f"Error stopping monitoring: {e}")

    def is_running(self):
        """Check if the monitoring stack is healthy by probing Prometheus."""
        try:
            url = f"http://localhost:{self.prometheus_port}/-/ready"
            req = urllib.request.Request(url, method='GET')
            with urllib.request.urlopen(req, timeout=2) as resp:
                return resp.status == 200
        except Exception:
            return False

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

        Only includes nodes with status UP or DECOMMISSIONED (still running).
        Excludes DOWN and UNINITIALIZED nodes.
        """
        from ccmlib.node import Status

        dc_targets = {}
        for node in self.cluster.nodelist():
            if node.status not in (Status.UP, Status.DECOMMISSIONED):
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

        # Write to temp file then atomically rename
        fd, tmp_path = tempfile.mkstemp(
            dir=targets_dir, prefix='.scylla_servers_', suffix='.yml.tmp'
        )
        try:
            with os.fdopen(fd, 'w') as f:
                json.dump(targets, f, indent=2)
                f.write('\n')
            os.replace(tmp_path, targets_file)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def _wait_until_ready(self, timeout=60):
        """Block until Prometheus and Grafana health endpoints respond.

        Raises TimeoutError if not ready within timeout seconds.
        """
        deadline = time.time() + timeout

        # Wait for Prometheus
        while time.time() < deadline:
            try:
                url = f"http://localhost:{self.prometheus_port}/-/ready"
                req = urllib.request.Request(url, method='GET')
                with urllib.request.urlopen(req, timeout=2) as resp:
                    if resp.status == 200:
                        break
            except Exception:
                pass
            time.sleep(1)
        else:
            raise TimeoutError(
                f"Prometheus not ready at localhost:{self.prometheus_port} "
                f"after {timeout}s"
            )

        # Wait for Grafana
        while time.time() < deadline:
            try:
                url = f"http://localhost:{self.grafana_port}/api/health"
                req = urllib.request.Request(url, method='GET')
                with urllib.request.urlopen(req, timeout=2) as resp:
                    if resp.status == 200:
                        break
            except Exception:
                pass
            time.sleep(1)
        else:
            raise TimeoutError(
                f"Grafana not ready at localhost:{self.grafana_port} "
                f"after {timeout}s"
            )

    def grafana_url(self):
        return f"http://localhost:{self.grafana_port}"

    def prometheus_url(self):
        return f"http://localhost:{self.prometheus_port}"
