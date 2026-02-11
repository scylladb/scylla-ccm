# Integrated Monitoring Stack

CCM can start and maintain a [scylla-monitoring](https://github.com/scylladb/scylla-monitoring)
stack (Prometheus, Grafana, Alertmanager) alongside the cluster it manages.
When a cluster undergoes topology changes (add node, remove node, decommission,
start, stop), the monitoring stack's scrape targets are kept in sync.

## Prerequisites

- **Docker** — scylla-monitoring runs Prometheus and Grafana as containers.
- **scylla-monitoring checkout** — a local clone of the
  [scylla-monitoring](https://github.com/scylladb/scylla-monitoring) repository.
  CCM calls its `start-all.sh` and `kill-all.sh` scripts directly.

CCM resolves the scylla-monitoring directory in this order:

1. Explicit `--monitoring-dir` CLI flag
2. `SCYLLA_MONITORING_DIR` environment variable
3. `~/.ccm/scylla-monitoring/` (default location)

## Two Modes of Operation

### Automatic mode

Enabled at cluster creation time with `--monitoring`, or on an existing cluster
with `ccm monitoring enable`. Once set, **every** operation that changes node
topology automatically updates the monitoring stack's targets. The flag is
persisted in `cluster.conf` so all future operations honour it.

```bash
ccm create mycluster --scylla -n 3 -v release:2024.2 --monitoring -s
# monitoring starts automatically with the cluster

ccm add node4 --scylla        # targets updated automatically
ccm node4 start               # targets updated automatically
ccm node2 stop                # targets updated automatically
ccm stop                      # monitoring stops automatically
ccm start                     # monitoring restarts automatically
```

The flag can also be toggled on an existing cluster:

```bash
ccm monitoring enable          # sets the flag, starts monitoring, syncs targets
ccm add node5 --scylla         # from now on, auto-updates

ccm monitoring disable         # clears the flag, stops monitoring
```

### Manual mode

The default — no monitoring unless explicitly requested. Start and sync on
demand; no automatic updates happen on topology changes.

```bash
ccm create mycluster --scylla -n 3 -v release:2024.2 -s
# ... some time later ...
ccm monitoring start           # starts monitoring, syncs current state
ccm add node4 --scylla         # monitoring is NOT updated
ccm monitoring sync            # manually refresh targets
ccm monitoring stop            # stops monitoring
```

## Environment Variables

| Variable | Description |
|---|---|
| `CCM_MONITORING` | When non-empty and not `0`, every newly created cluster behaves as if `--monitoring` was passed. Equivalent to `ccm create ... --monitoring`. Example: `export CCM_MONITORING=1`. |
| `SCYLLA_MONITORING_DIR` | Path to the scylla-monitoring checkout. Used as the default when `--monitoring-dir` is not provided. |

## CLI Reference

### `ccm create` flags

| Flag | Description |
|---|---|
| `--monitoring` | Enable automatic monitoring for this cluster. |
| `--monitoring-dir=PATH` | Path to scylla-monitoring checkout. |
| `--grafana-port=PORT` | Grafana port (default: 3000). |
| `--prometheus-port=PORT` | Prometheus port (default: 9090). |
| `--alertmanager-port=PORT` | Alertmanager port (default: 9093). |

### `ccm monitoring` subcommands

| Subcommand | Description |
|---|---|
| `start` | Start the monitoring stack for the current cluster (manual mode). Blocks until Prometheus and Grafana are healthy. |
| `stop` | Stop the monitoring stack. |
| `enable` | Set automatic mode, persist the flag, start monitoring if the cluster is running, and sync targets. |
| `disable` | Unset automatic mode and stop monitoring. |
| `sync` | Force-regenerate Prometheus targets from current cluster state. Requires monitoring to be running. |
| `status` | Show whether automatic mode is enabled, whether the stack is running, URLs, and target count. |

All subcommands accept `--monitoring-dir`, `--grafana-port`, `--prometheus-port`,
and `--alertmanager-port` overrides.

## How It Works

### Target generation

CCM generates a `scylla_servers.yml` file in the Prometheus `file_sd_configs`
format. Prometheus watches this file and automatically reloads targets when it
changes — no container restart or API call needed.

Targets are grouped by datacenter:

```json
[
  {
    "targets": ["127.0.0.1", "127.0.0.2"],
    "labels": {"cluster": "mycluster", "dc": "dc1"}
  },
  {
    "targets": ["127.0.0.3"],
    "labels": {"cluster": "mycluster", "dc": "dc2"}
  }
]
```

Only nodes with status **UP** or **DECOMMISSIONED** (still running, still
scrapeable) are included. **DOWN** and **UNINITIALIZED** nodes are excluded.

### Topology change hooks

In automatic mode, a lightweight hook (`_notify_topology_change()`) fires after
every topology mutation:

| Operation | When hook fires |
|---|---|
| `cluster.start()` | After all nodes are UP |
| `cluster.stop()` | Monitoring stopped before nodes shut down |
| `node.start()` | After the node is UP |
| `node.stop()` | After the node is DOWN |
| `cluster.add(node)` | After the node is registered |
| `cluster.remove(node)` | After the node is removed |
| `node.decommission()` | After decommission completes |

The hook is a no-op when automatic mode is disabled or monitoring is not running.

### Container management

CCM delegates to scylla-monitoring's `start-all.sh` / `kill-all.sh` scripts.
Key flags used:

- `-l` — host networking, so containers can reach `127.0.0.x` nodes
- `-b ccm-<cluster_name>` — unique stack ID per cluster, allowing multiple
  clusters to run monitoring simultaneously
- `--no-renderer` — skip the optional renderer component

### Configuration persistence

Monitoring settings are stored in the existing `cluster.conf` YAML file:

```yaml
monitoring_enabled: true
monitoring_dir: /path/to/scylla-monitoring
grafana_port: 3000
prometheus_port: 9090
alertmanager_port: 9093
```

On `ClusterFactory.load()`, these fields are restored. The monitoring stack
itself is **not** auto-started on load — it starts when `cluster.start()` is
called (if `monitoring_enabled` is true) or when the user runs
`ccm monitoring start`.

### Directory structure

```
~/.ccm/<cluster>/
├── cluster.conf                          # monitoring_enabled, ports, etc.
├── node1/
├── node2/
└── monitoring/                           # created on first monitoring start
    ├── prometheus/
    │   └── targets/
    │       ├── scylla_servers.yml         # auto-generated, watched by Prometheus
    │       └── scylla_manager_servers.yml # empty placeholder (required)
    └── data/
        └── prometheus_data/              # persistent Prometheus TSDB data
```

## Programmatic Usage

```python
from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_monitoring import MonitoringStack

# Create cluster with monitoring enabled
cluster = ScyllaCluster('.', 'test', version='release:2024.2')
cluster.monitoring_enabled = True
cluster.monitoring_dir = '/path/to/scylla-monitoring'
cluster.populate(3).start()
# monitoring is now running, targets auto-update

# Or start monitoring manually
stack = MonitoringStack(cluster, '/path/to/scylla-monitoring')
stack.start()                 # blocks until healthy
print(stack.grafana_url())    # http://localhost:3000
print(stack.prometheus_url()) # http://localhost:9090

# After topology changes, sync manually
stack.update_targets()

# Stop
stack.stop()
```

## Edge Cases

- **Port conflicts**: Use `--grafana-port` / `--prometheus-port` to avoid
  conflicts when running multiple monitored clusters.
- **Monitoring dir not found**: In automatic mode, a warning is logged but
  `ccm start` succeeds. In manual mode (`ccm monitoring start`), an error
  is raised.
- **Docker not available**: `start-all.sh` fails with a clear error.
- **Prometheus scraping delay**: After targets are written, Prometheus discovers
  them within seconds but the first scrape may take up to `scrape_interval`
  (default 20s).
- **Cluster loaded from disk**: `monitoring_enabled` and ports are restored,
  but monitoring is not auto-started until `cluster.start()` or
  `ccm monitoring start`. If containers from a previous session are still
  running, `ccm monitoring status` detects and reconnects them.
- **Monitoring failures in automatic mode**: Logged as warnings, never fail the
  cluster operation. Monitoring is auxiliary.
