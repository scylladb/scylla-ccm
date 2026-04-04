# Podman Network Topology for CCM

CCM supports running ScyllaDB clusters in rootless podman containers with
topology-aware network simulation. Each rack gets its own podman network, and
the host's `tc`/`netem` binaries (applied via `nsenter`) simulate realistic
latency between racks and datacenters.

## Prerequisites

- **Podman 4.0+** (rootless mode works)
- **Linux kernel with `sch_netem` module** (`modprobe sch_netem`)
- **iproute2** installed on the host (provides `tc` and `ip`)
- **util-linux** installed on the host (provides `nsenter`)
- A ScyllaDB container image (e.g. `scylladb/scylla:2026.1`)
- IP forwarding enabled: `sysctl net.ipv4.ip_forward=1`

## Architecture

```
Host (rootless podman, ip_forward=1, routes between rack bridges)
  ├── ccm-{cluster}-dc1-rac1 (10.<prefix>.1.0/24) — node1, node2, ccm-client
  ├── ccm-{cluster}-dc1-rac2 (10.<prefix>.2.0/24) — node3, node4
  └── ccm-{cluster}-dc2-rac1 (10.<prefix>.3.0/24) — node5
```

- Each rack has its own podman network with a /24 subnet
- Each node connects to its rack network only (single interface `eth0`)
- Intra-rack: bridged directly, 0 latency
- Cross-rack/DC: routed through host, `tc`/`netem` applied via `nsenter` adds per-destination delay
- CQL client container sits on Rack1's network with same tc rules

### IP Scheme

| Component      | IP Pattern                           |
|----------------|--------------------------------------|
| Rack subnets   | `10.{prefix}.{rack_idx}.0/24`      |
| Gateways       | `10.{prefix}.{rack_idx}.254`       |
| Node IPs       | `10.{prefix}.{rack_idx}.{node_offset}` |
| Client         | `10.{prefix}.1.100` (on first rack) |

Where `rack_idx` starts at 1 and increments globally across all DCs.
By default CCM prefers the `10.89.x.0/24` range, but if that overlaps with
existing podman networks it automatically picks another free `10.x.0.0/16`
prefix. To force a specific prefix, set `CCM_PODMAN_SUBNET_PREFIX`, for example
`CCM_PODMAN_SUBNET_PREFIX=10.123`.

### Latency Simulation

Traffic shaping uses the host's `tc`/`netem` binaries applied via `nsenter`
into each container's network namespace.  This avoids installing networking
tools inside the container image.  The host must have `iproute2` installed
(provides `tc`) and the kernel's `sch_netem` module loaded.

Each container's `eth0` gets a classful `prio` qdisc with `u32` filters:

- **Band 1** (default): intra-rack traffic — no delay
- **Band 2**: inter-rack same DC — configurable (default: 1ms)
- **Band 3**: inter-DC — configurable (default: 50ms) + optional packet loss

## Usage

### CLI

Create a multi-DC cluster with podman:

```bash
# 2 DCs: DC1 has 2 racks (2 nodes each), DC2 has 1 rack (1 node)
ccm create mycluster --podman-image scylladb/scylla:2026.1 \
    -n 4:1 \
    --inter-rack-delay 2 \
    --inter-dc-delay 100 \
    --packet-loss 0.1

ccm start
```

The `-n` argument uses colon-separated notation for multi-DC. For multi-rack
within a DC, use the Python API.

### Python API

```python
from collections import OrderedDict
from ccmlib.scylla_podman_cluster import ScyllaPodmanCluster

cluster = ScyllaPodmanCluster(
    "/path/to/ccm",
    "mycluster",
    podman_image="scylladb/scylla:2026.1",
    inter_rack_delay_ms=1,
    inter_dc_delay_ms=50,
    packet_loss_percent=0.5,
)

# Multi-DC, multi-rack topology:
# DC1: RAC1 (2 nodes), RAC2 (2 nodes)
# DC2: RAC1 (1 node)
cluster.populate({"DC1": [2, 2], "DC2": [1]})
cluster.start(wait_for_binary_proto=True)

# Access node IPs from the topology
topo = cluster.network_topology
print(topo.get_node_ip("node1"))  # for example: 10.89.1.1

# Start a CQL client container on Rack1's network
cluster.start_client_container()

# Clean up
cluster.remove()
```

### Topology Argument Formats

The `populate()` method accepts several formats:

| Format | Example | Result |
|--------|---------|--------|
| `int` | `3` | 1 DC, 1 rack, 3 nodes |
| `list` | `[2, 3]` | 2 DCs, 1 rack each, 2+3 nodes |
| `dict(str→int)` | `{"DC1": 2, "DC2": 1}` | 2 DCs, 1 rack each |
| `dict(str→list)` | `{"DC1": [2, 2], "DC2": [1]}` | DC1: 2 racks, DC2: 1 rack |
| `dict(str→dict)` | `{"DC1": {"R1": 2, "R2": 2}}` | Explicit rack names |

## Class Hierarchy

```
Cluster → ScyllaCluster → ScyllaPodmanCluster
Node    → ScyllaNode    → ScyllaPodmanNode
```

Key classes:

- `PodmanNetworkTopology` — manages subnet allocation, IP assignment, route
  calculation, and tc command generation
- `ScyllaPodmanCluster` — overrides `populate()`, `create_node()`,
  `get_node_ip()`, `remove()`, `_update_config()`; manages client container
- `ScyllaPodmanNode` — overrides container lifecycle, service management,
  tool execution; handles route setup and tc application

## Cluster Configuration Persistence

Podman clusters save their state to `cluster.conf` with these keys:

- `docker_image` — the container image (same key as Docker clusters)
- `network_topology` — serialized topology including rack assignments, delays,
  and packet loss settings

The `ClusterFactory.load()` method detects the presence of `network_topology`
in `cluster.conf` to distinguish a podman/topology cluster from a plain Docker
one, and restores the full `ScyllaPodmanCluster` with its network topology.

## Running Tests

Unit tests (no podman required):

```bash
pytest tests/test_scylla_podman_cluster.py::TestPodmanNetworkTopology -v
```

Integration tests (require podman and a ScyllaDB image):

```bash
export SCYLLA_PODMAN_IMAGE=scylladb/scylla:2026.1
pytest tests/test_scylla_podman_cluster.py -v -m network_topology
```

## Differences from Docker Support

| Feature | Docker (broken) | Podman |
|---------|----------------|--------|
| Runtime | Docker | Podman (rootless) |
| Network | Single flat network | Per-rack networks |
| Latency sim | None | tc/netem via nsenter from host |
| Multi-DC | Not topology-aware | Full DC/rack topology |
| Client | Direct host access | Client container on Rack1 |
| Status | Broken in master | Active |

## Example: 2 DCs x 3 AZs

A realistic deployment with 2 datacenters, 3 availability zones (racks) per DC,
1 node per AZ, and configurable inter-AZ/inter-DC latency:

```
Host (rootless podman)
  ├── ccm-mycluster-dc1-az1 (10.<prefix>.1.0/24) — node1, ccm-client
  ├── ccm-mycluster-dc1-az2 (10.<prefix>.2.0/24) — node2
  ├── ccm-mycluster-dc1-az3 (10.<prefix>.3.0/24) — node3
  ├── ccm-mycluster-dc2-az1 (10.<prefix>.4.0/24) — node4
  ├── ccm-mycluster-dc2-az2 (10.<prefix>.5.0/24) — node5
  └── ccm-mycluster-dc2-az3 (10.<prefix>.6.0/24) — node6
```

### Python API

```python
from collections import OrderedDict
from ccmlib.scylla_podman_cluster import ScyllaPodmanCluster

cluster = ScyllaPodmanCluster(
    "/path/to/ccm",
    "mycluster",
    podman_image="scylladb/scylla:2026.1",
    inter_rack_delay_ms=1,    # 1ms between AZs in the same DC
    inter_dc_delay_ms=40,     # 40ms between DCs
    packet_loss_percent=0.0,
)

# 2 DCs, 3 AZs each, 1 node per AZ
cluster.populate({
    "DC1": {"AZ1": 1, "AZ2": 1, "AZ3": 1},
    "DC2": {"AZ1": 1, "AZ2": 1, "AZ3": 1},
})

cluster.start(wait_for_binary_proto=True)

# Start the CQL client container on the first AZ's network
cluster.start_client_container()

# Node IPs:
topo = cluster.network_topology
print(topo.get_node_ip("node1"))  # for example: 10.89.1.1 (DC1/AZ1)
print(topo.get_node_ip("node4"))  # for example: 10.89.4.1 (DC2/AZ1)

# CQL client contact points
print(cluster.get_client_contact_points())

# Latency from DC1/AZ1 to DC1/AZ2: ~1ms (inter-rack same DC)
# Latency from DC1/AZ1 to DC2/AZ1: ~40ms (inter-DC)

cluster.remove()
```

### Connecting a CQL Driver

To connect a Python CQL driver (e.g., `scylla-driver` or `cassandra-driver`) to the
cluster from the host, use the node IPs from the topology:

```python
from cassandra.cluster import Cluster as CQLCluster
from cassandra.policies import DCAwareRoundRobinPolicy

contact_points = [ip for ip, port in cluster.get_client_contact_points()]
cql = CQLCluster(
    contact_points=contact_points,
    port=9042,
    load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="DC1"),
)
session = cql.connect()

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS test_ks
    WITH replication = {
        'class': 'NetworkTopologyStrategy',
        'DC1': 3, 'DC2': 3
    }
""")
session.execute("USE test_ks")
session.execute("CREATE TABLE IF NOT EXISTS test (k int PRIMARY KEY, v text)")
session.execute("INSERT INTO test (k, v) VALUES (1, 'hello')")
print(session.execute("SELECT * FROM test").one())

session.shutdown()
cql.shutdown()
```

Note: if running from the host (outside containers), the host must have routes
to the selected `10.<prefix>.x.0/24` subnets. Podman rootless mode typically handles this
automatically. Alternatively, use `cluster.run_cqlsh_on_client()` to execute
CQL commands from inside the client container, which is always on the correct
network.

### tc rules applied (via nsenter) to node1 (DC1/AZ1)

```
# Root: prio qdisc, all traffic defaults to band 1 (no delay)
tc qdisc add dev eth0 root handle 1: prio bands 4 priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0

# Band 2: inter-AZ same DC (1ms delay)
tc qdisc add dev eth0 parent 1:2 handle 20: netem delay 1ms
tc filter add dev eth0 parent 1:0 protocol ip u32 match ip dst 10.89.2.0/24 flowid 1:2
tc filter add dev eth0 parent 1:0 protocol ip u32 match ip dst 10.89.3.0/24 flowid 1:2

# Band 3: inter-DC (40ms delay)
tc qdisc add dev eth0 parent 1:3 handle 30: netem delay 40ms
tc filter add dev eth0 parent 1:0 protocol ip u32 match ip dst 10.89.4.0/24 flowid 1:3
tc filter add dev eth0 parent 1:0 protocol ip u32 match ip dst 10.89.5.0/24 flowid 1:3
tc filter add dev eth0 parent 1:0 protocol ip u32 match ip dst 10.89.6.0/24 flowid 1:3
```


### nodetool status output (2 DC x 3 AZ, scylladb/scylla:2026.1)

```
Datacenter: dc1
===============
Status=Up/Down/eXcluded
|/ State=Normal/Leaving/Joining/Moving
-- Address   Load      Tokens Owns Host ID                              Rack
UN 10.89.1.1 338.62 KB 1      ?    31dcc8a7-b63d-4b7e-8cc0-ebde48abc56c az1
UN 10.89.2.1 347.41 KB 1      ?    a663710c-8f2c-44ef-a116-48f973e29417 az2
UN 10.89.3.1 366.19 KB 1      ?    fa8b8a5d-1dbc-4eed-a283-c5f00a5b464c az3
Datacenter: dc2
===============
Status=Up/Down/eXcluded
|/ State=Normal/Leaving/Joining/Moving
-- Address   Load      Tokens Owns Host ID                              Rack
UN 10.89.4.1 421.56 KB 1      ?    1c1a314f-b2ca-4934-9058-f95e678086a3 az1
UN 10.89.5.1 307.51 KB 1      ?    78c91881-80c5-4212-973b-aeb0ebb3c43e az2
UN 10.89.6.1 330.97 KB 1      ?    34414cd8-57c2-4857-884b-048491d4b2c9 az3
```

Each node is UN (Up/Normal) with its own IP on the corresponding rack subnet:

| Node  | DC  | Rack | IP        | Subnet        |
|-------|-----|------|-----------|---------------|
| node1 | dc1 | az1  | 10.89.1.1 | 10.89.1.0/24  |
| node2 | dc1 | az2  | 10.89.2.1 | 10.89.2.0/24  |
| node3 | dc1 | az3  | 10.89.3.1 | 10.89.3.0/24  |
| node4 | dc2 | az1  | 10.89.4.1 | 10.89.4.0/24  |
| node5 | dc2 | az2  | 10.89.5.1 | 10.89.5.0/24  |
| node6 | dc2 | az3  | 10.89.6.1 | 10.89.6.0/24  |

CQL client container: 10.89.1.100 on the dc1/az1 network.
