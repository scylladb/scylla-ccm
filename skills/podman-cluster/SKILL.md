---
name: podman-cluster
description: Start a multi-DC podman-based Scylla cluster with configurable topology, latency simulation, and CPU pinning
license: Apache-2.0
compatibility: opencode
metadata:
  audience: scylla-developers
  workflow: operational
---

**IMPORTANT**: All skill code must be edited in the source repository at `~/github/scylla-ccm/skills/podman-cluster/SKILL.md`, not the installed copy under `~/.config/opencode/skills/podman-cluster/`.

## What I do

Start a multi-DC, multi-rack Scylla cluster using podman containers with optional inter-DC/inter-rack latency simulation, packet loss, and CPU pinning. Creates a CCM-based podman cluster using the `ScyllaPodmanCluster` API.

## Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `--version` | no | `master` | ScyllaDB version (e.g. `master`, `5.4`, `2024.1`). When `master`, uses the nightly image. Otherwise maps to `docker.io/scylladb/scylla:{version}`. |
| `--image` | no | derived from `--version` | Scylla container image (overrides version-based default) |
| `--topology` | no | `dc1:rack1:5,rack2:5,rack3:5;dc2:rack1:5,rack2:5,rack3:5;dc3:rack1:5,rack2:5,rack3:5` | Topology as `dc:rack:N,...;dc:rack:N,...` |
| `--concurrency` | no | `2` | Number of parallel node starts |
| `--inter-dc-delay` | no | `40` | Simulated inter-DC latency in ms |
| `--inter-rack-delay` | no | `1` | Simulated inter-rack latency in ms |
| `--packet-loss` | no | `0` | Cross-DC packet loss percentage |
| `--pinning` | no | `false` | Pin each node to dedicated CPU cores |
| `--keep` | no | `false` | Keep cluster running after success |
| `--name` | no | `test-cluster` | Cluster name |
| `--dir` | no | `/tmp` | Directory holding the cluster data (the containers' disks). Defaults to `/tmp` (tmpfs) for speed; point it at a real filesystem for large clusters |

All parameters are optional. The default is a 45-node cluster across 3 DCs × 3 racks × 5 nodes.

## Prerequisites

- Linux host with podman 4.0+
- `tc` (iproute2) and `nsenter` (util-linux) for latency simulation
- `python3` with `ccm` installed from this repo
- Sufficient disk/memory for the containers
- For `--pinning`: enough host CPUs (`>= total_nodes * smp`)

## Workflow

### Step 1: Validate prerequisites

Check that podman, tc, and nsenter are available:

```bash
command -v podman >/dev/null || { echo "podman not found"; exit 1; }
command -v tc >/dev/null || { echo "tc not found"; exit 1; }
command -v nsenter >/dev/null || { echo "nsenter not found"; exit 1; }
```

### Step 2: Parse topology argument

Parse the `--topology` string into a Python nested dict:

```python
topology = {}
for dc_part in topology_str.split(";"):
    dc_name, _, rest = dc_part.strip().partition(":")
    racks = {}
    for rp in rest.split(","):
        rp = rp.strip()
        if not rp:
            continue
        rack_name, count = rp.split(":")
        racks[rack_name] = int(count)
    topology[dc_name] = racks
```

### Step 3: Ask for ScyllaDB version (if not already provided)

If `--version` was not passed on the command line, use the `question` tool to ask the user which ScyllaDB version they want. Offer `master` (Recommended), `2024.1`, `2025.1`, `5.4`, and `6.0` as options, with `master` as the default. Set `custom: true` so they can type any version string.

If `--version` was provided on the command line, use that value directly.

### Step 4: Resolve image from version

```python
if not image:
    if version == "master":
        image = "docker.io/scylladb/scylla-nightly:latest"
    else:
        image = f"docker.io/scylladb/scylla:{version}"
```

### Step 5: Create and populate the cluster

```python
from ccmlib.scylla_podman_cluster import ScyllaPodmanCluster
import tempfile

data_dir = dir if dir else "/tmp"
cluster = ScyllaPodmanCluster(
    str(tempfile.mkdtemp(prefix="ccm-podman-", dir=data_dir)),
    name=cluster_name,
    podman_image=image,
    inter_dc_delay_ms=inter_dc_delay,
    inter_rack_delay_ms=inter_rack_delay,
    packet_loss_percent=packet_loss,
    pinning=pinning,
)
cluster.populate(topology)
total_nodes = len(cluster.nodelist())
```

### Step 6: Configure and start nodes

```python
import concurrent.futures
import time
import logging

LOG = logging.getLogger("podman-cluster")

cluster.set_configuration_options(values={
    "read_request_timeout_in_ms": 10000,
    "range_request_timeout_in_ms": 10000,
    "write_request_timeout_in_ms": 10000,
    "truncate_request_timeout_in_ms": 10000,
    "request_timeout_in_ms": 10000,
})

nodes = cluster.nodelist()
t0 = time.time()

def start_node(node):
    LOG.info("Starting %s ...", node.name)
    node.start(wait_for_binary_proto=True, wait_other_notice=False)
    LOG.info("Started %s", node.name)
    return node

with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
    futures = {pool.submit(start_node, n): n for n in nodes}
    done = 0
    for future in concurrent.futures.as_completed(futures):
        n = futures[future]
        done += 1
        try:
            future.result()
            elapsed = time.time() - t0
            LOG.info("[%d/%d] %s UP in %.1f s", done, total_nodes, n.name, elapsed)
        except Exception as exc:
            LOG.error("%s FAILED: %s", n.name, exc)

# Nodes above were started concurrently via node.start() directly, bypassing
# cluster.start_nodes() (which applies tc/netem serially after each start).
# Apply the topology's inter-rack/inter-DC latency and packet-loss rules now
# that all nodes are confirmed running -- otherwise the cluster would come up
# looking healthy but with none of the advertised network simulation active.
LOG.info("Applying tc/netem network shaping...")
cluster.apply_network_shaping()
```

### Step 7: Report status

```python
total = time.time() - t0
running = sum(1 for n in nodes if n.is_running())
nodetool_status = nodes[0].nodetool('status')[0]
print(f"Total time: {total:.1f} s ({total/60:.1f} min)")
print(f"Nodes UP: {running}/{total_nodes}")
```

Print the output exactly as:

```
[Step 1/7] Validating prerequisites...
[Step 2/7] Parsing topology: <topology_str>
[Step 3/7] Asking for ScyllaDB version...
[Step 4/7] Resolving image from version: <version>
[Step 5/7] Creating and populating cluster...
[Step 6/7] Starting <N> nodes with concurrency <M>...
[Step 7/7] Reporting status...
Total time: <X> s (<Y> min)
Nodes UP: <running>/<total>
```

### Step 8: Clean up (unless --keep)

```python
if not keep:
    print("Cleaning up...")
    cluster.remove()
    print("Done.")
```

If `--keep` is set, print the cluster path and instructions:

```
Cluster path: <path>
Keep it with: ccm switch <name> && ccm start
Remove it with: ccm remove <name>
```

Print the end result -- `nodetool status` for the whole cluster (all nodes, with
Datacenter/Rack topology) -- as the final output:

```python
print("\nEnd result -- nodetool status (all nodes, DC/rack topology):")
print(nodetool_status)
```

```
End result -- nodetool status (all nodes, DC/rack topology):
Datacenter: dc1
================
Rack    Rack1
--------------
...
<full nodetool status output>
```

## When to use me

- You need a realistic multi-DC Scylla cluster for manual testing
- You want to test LWT or Raft behavior under inter-DC latency
- You need a quick performance benchmark environment
- You want to reproduce cluster-level issues without provisioning real VMs

## Example usage

```
# Default 45-node cluster (ScyllaDB master/nightly)
/podman-cluster

# Specific ScyllaDB version
/podman-cluster --version "2024.1"

# Small 2-node cluster
/podman-cluster --topology "dc1:rack1:1,rack2:1" --concurrency 1

# 2 DC with 50ms inter-DC delay
/podman-cluster --topology "dc1:rack1:3;dc2:rack1:3" --inter-dc-delay 50

# CPU-pinned 12-node cluster (requires 24+ host CPUs with smp=2)
/podman-cluster --topology "dc1:rack1:4,rack2:4,rack3:4" --pinning --concurrency 4

# Keep running for investigation
/podman-cluster --topology "dc1:rack1:2" --keep

# Large cluster on a real filesystem instead of /tmp (tmpfs)
/podman-cluster --topology "dc1:rack1:7,rack2:7,rack3:7;dc2:rack1:7,rack2:7,rack3:7;dc3:rack1:7,rack2:7,rack3:7" --dir ~/scylla-clusters
```

## Critical implementation constraints

1. ALWAYS print progress steps with `[Step N/M]` prefix.
2. Default topology creates a 45-node cluster (3 DC × 3 rack × 5 node) — warn the user about resource usage.
3. All parameters are optional; use defaults when omitted.
4. Do NOT implement anything that requires root privileges (podman is rootless).
5. The run_big_cluster.py in tests/ is a reference, but this skill uses its own Python code inline.
