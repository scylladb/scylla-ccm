# latte loader containers attached to a podman cluster's rack networks. Each
# container runs the vendored latte_cs_alike.rn workload (from scylla-cluster-tests)
# in a loop against its rack-group's targets, per the lb_policy/workload_mode knobs.
#
# Podman-only (not Docker): loaders reuse ScyllaPodmanCluster's per-rack podman
# networks, static IP assignment, and nsenter-based route/tc setup, none of
# which the Docker cluster path provides. Supporting Docker would mean giving
# ScyllaDockerCluster the same rack-network/static-IP topology first, not a
# small addition here.

import logging
import os
import shlex
from collections import OrderedDict

from ccmlib.container_client import ContainerClientError

LOGGER = logging.getLogger("ccm")


def _force_remove_loader_container(name):
    """Force-remove a loader container by durable CCM ownership, not creator PID.

    Node containers refuse removal unless the *current* process's PID matches
    the creator label, because a live node is stateful and every `ccm`
    invocation is a new process. That check makes loader removal fail on
    virtually every real invocation (see review discussion on #788), so
    loaders -- which hold no persistent data -- are instead authorized by the
    presence of our resource-owner label alone, regardless of which PID wrote it.
    """
    from ccmlib.scylla_podman_cluster import (
        PODMAN_RESOURCE_OWNER_LABEL,
        _container_owner_labels,
        _get_podman_client,
        _inspect_container,
    )
    info = _inspect_container(name)
    if info is None:
        return
    if PODMAN_RESOURCE_OWNER_LABEL not in _container_owner_labels(info):
        raise RuntimeError(
            f"Refusing to remove container {name}: missing {PODMAN_RESOURCE_OWNER_LABEL} label"
        )
    try:
        _get_podman_client().remove_container(name, force=True, volumes=True, check=True)
    except ContainerClientError as exc:
        raise RuntimeError(f"Failed to remove loader container {name}: {exc}")

LOADER_LOG_TAIL_LINES = 200
# Workload writes its exit code here on failure; pid 1 exits with it. Signalling
# pid 1 instead doesn't work: the kernel drops in-namespace signals to init.
LOADER_EXIT_FILE = "/tmp/ccm-loader-exit"


def _pid1_script(exit_file=LOADER_EXIT_FILE):
    q = shlex.quote(exit_file)
    return f"while [ ! -s {q} ]; do sleep 1; done; exit $(cat {q})"


def _workload_script(schema_cmd, run_cmd, exit_file=LOADER_EXIT_FILE):
    # Stop on the first schema/run failure so the container exits and status shows it.
    return (
        f"{schema_cmd}; rc=$?; "
        f"while [ $rc -eq 0 ]; do {run_cmd}; rc=$?; [ $rc -eq 0 ] && sleep 1; done; "
        f"echo $rc > {shlex.quote(exit_file)}"
    )


def _dump_loader_logs(name):
    from ccmlib.scylla_podman_cluster import _get_podman_client
    try:
        logs = _get_podman_client().stream_logs(name, tail=LOADER_LOG_TAIL_LINES)
    except Exception:
        LOGGER.warning("Failed to fetch logs of loader container %s", name, exc_info=True)
        return
    LOGGER.error("Last %d log lines of loader container %s:\n%s", LOADER_LOG_TAIL_LINES, name, logs)


DEFAULT_LOADER_IMAGE = "scylladb/latte:latest"

RUNE_SCRIPT_HOST_PATH = os.path.join(os.path.dirname(__file__), "resources", "latte_cs_alike.rn")
RUNE_SCRIPT_CONTAINER_PATH = "/workloads/latte_cs_alike.rn"

# Load-balancing policy knob: which nodes a loader's driver connects to.
LB_POLICIES = ("local-rack", "local-dc", "whole-cluster")
DEFAULT_LB_POLICY = "local-rack"

DEFAULT_SCHEMA_CONFIG = {"keyspaces": 1, "tables_per_keyspace": 1, "replication_factor": 3}

# Workload knob: which keyspace/table a loader (or rather, its whole rack-group
# -- see LoaderSet.add) targets, and whether it only writes or also reads.
WORKLOAD_MODES = {"write": ["-f", "write"], "read-write": ["-f", "write:1", "-f", "read:1"]}
DEFAULT_WORKLOAD_MODE = "read-write"


class Loader:
    """A single latte loader container, running latte_cs_alike.rn on one rack's network."""

    def __init__(self, cluster, name, dc, rack, ip, network, image, lb_policy=DEFAULT_LB_POLICY,
                 workload_mode=DEFAULT_WORKLOAD_MODE, keyspace_index=0, table_index=0,
                 schema_config=None):
        if lb_policy not in LB_POLICIES:
            raise ValueError(f"Invalid lb_policy {lb_policy!r}: must be one of {LB_POLICIES}")
        if workload_mode not in WORKLOAD_MODES:
            raise ValueError(f"Invalid workload_mode {workload_mode!r}: must be one of {list(WORKLOAD_MODES)}")
        self.cluster = cluster
        self.name = name
        self.dc = dc
        self.rack = rack
        self.ip = ip
        self.network = network
        self.image = image
        self.lb_policy = lb_policy
        self.workload_mode = workload_mode
        self.keyspace_index = keyspace_index
        self.table_index = table_index
        self.schema_config = dict(schema_config or DEFAULT_SCHEMA_CONFIG)

    def container_name(self):
        return f"ccm-{self.cluster.name}-{self.name}"

    def keyspace(self):
        return f"ks{self.keyspace_index}"

    def table(self):
        return f"tbl{self.table_index}"

    def _target_ips(self):
        """Nodes this loader's driver connects to, per lb_policy."""
        topo = self.cluster.network_topology
        if self.lb_policy == "local-rack":
            ips = [i["ip"] for i in topo.node_assignments.values() if i["dc"] == self.dc and i["rack"] == self.rack]
        elif self.lb_policy == "local-dc":
            ips = [i["ip"] for i in topo.node_assignments.values() if i["dc"] == self.dc]
        else:  # whole-cluster
            ips = [i["ip"] for i in topo.node_assignments.values()]
        if not ips:
            raise RuntimeError(f"No target nodes found for loader {self.name} (lb_policy={self.lb_policy})")
        return ips

    def _ks_table_params(self):
        return ["-P", f"keyspace={self.keyspace()}", "-P", f"table={self.table()}"]

    def _schema_cmd(self):
        return [
            "latte", "schema", RUNE_SCRIPT_CONTAINER_PATH, ",".join(self._target_ips()),
            *self._ks_table_params(),
            "-P", f"replication_factor={self.schema_config['replication_factor']}",
        ]

    def _run_cmd(self):
        return [
            "latte", "run", RUNE_SCRIPT_CONTAINER_PATH, ",".join(self._target_ips()),
            "--warmup", "0", *WORKLOAD_MODES[self.workload_mode],
            *self._ks_table_params(),
        ]

    def start(self):
        from ccmlib.scylla_podman_cluster import _get_podman_client, _resource_labels

        name = self.container_name()
        _force_remove_loader_container(name)
        client = _get_podman_client()
        try:
            client.run_container(
                image=self.image,
                name=name,
                network=self.network,
                ip=self.ip,
                labels=_resource_labels(),
                cap_add=["NET_ADMIN"],
                volumes={RUNE_SCRIPT_HOST_PATH: RUNE_SCRIPT_CONTAINER_PATH},
                # Don't auto-run the workload: routes/tc shaping must be in place
                # first (see below), or cross-rack/whole-cluster loaders could hit
                # unrouted/unshaped targets and abort on their first command.
                entrypoint="sh",
                command=["-c", _pid1_script()],
            )
        except ContainerClientError as exc:
            raise RuntimeError(f"Failed to start loader container {name}: {exc}")

        try:
            status = client.get_container_status(name)
            if status != "running":
                raise RuntimeError(f"Loader container {name} is not running after start (status: {status})")
            # Give the loader the same cross-rack/cross-DC routes and tc/netem
            # shaping as a real node in its rack, mirroring
            # ScyllaPodmanCluster.start_client_container() /
            # ScyllaPodmanNode._apply_tc_rules().
            node_name = self._rack_node_name()
            self.cluster._setup_container_routes(name, node_name)
            self._apply_tc_rules(node_name)
            self._start_workload(client)
            self._start_log_stream(name)
        except Exception:
            _dump_loader_logs(name)
            _force_remove_loader_container(name)
            raise

    def _apply_tc_rules(self, node_name):
        from ccmlib.scylla_podman_cluster import _nsenter_net_run, CONTAINER_NET_INTERFACE

        topo = self.cluster.network_topology
        tc_commands = topo.build_tc_commands(node_name)
        if not tc_commands:
            return
        # Reset first (root qdisc del is a no-op on first start), then "&&"
        # so a failing rule aborts instead of leaving partial shaping in place.
        script = " && ".join([
            f"tc qdisc del dev {CONTAINER_NET_INTERFACE} root 2>/dev/null || true",
            *tc_commands,
        ])
        res = _nsenter_net_run(self.container_name(), ["sh", "-c", script])
        if res.returncode != 0:
            raise RuntimeError(f"Failed to apply tc rules in {self.container_name()}: {res.stderr}")

    def _start_workload(self, client):
        # Create schema once, then loop the workload until it fails -- the script's
        # CREATE ... IF NOT EXISTS makes repeated schema runs across loaders safe.
        # Backgrounded via nohup/disown and redirected to the container's pid-1
        # stdout (rather than run as the container command) so it starts only
        # after routes/tc are set up, while still being captured by `logs -f`.
        schema_and_loop = _workload_script(shlex.join(self._schema_cmd()), shlex.join(self._run_cmd()))
        bg_cmd = f"nohup sh -c {shlex.quote(schema_and_loop)} </dev/null >>/proc/1/fd/1 2>&1 & disown"
        returncode, _, stderr = client.exec_command(self.container_name(), ["sh", "-c", bg_cmd])
        if returncode != 0:
            raise RuntimeError(f"Failed to launch loader workload in {self.container_name()}: {stderr}")

    def _start_log_stream(self, name):
        # Stream container output live into CCM's logs, same mechanism used for
        # Scylla nodes and the monitoring stack (ContainerLogManager), instead
        # of buffering it in memory.
        self.cluster._ensure_managers()
        log_manager = getattr(self.cluster, "_log_manager", None)
        if log_manager is None:
            return
        log_dir = os.path.join(self.cluster.get_path(), "loaders")
        os.makedirs(log_dir, exist_ok=True)
        log_manager.start_stream(name, os.path.join(log_dir, f"{self.name}.log"))

    def _rack_node_name(self):
        topo = self.cluster.network_topology
        for node_name, info in topo.node_assignments.items():
            if info["dc"] == self.dc and info["rack"] == self.rack:
                return node_name
        raise RuntimeError(
            f"No node found in {self.dc}/{self.rack} to derive loader routing from"
        )

    def stop(self):
        name = self.container_name()
        log_manager = getattr(self.cluster, "_log_manager", None)
        if log_manager is not None:
            log_manager.stop_stream(name)
        if not self.is_running():
            # Died on its own (workload failure) -- keep why before it's removed.
            _dump_loader_logs(name)
        _force_remove_loader_container(name)

    def is_running(self):
        from ccmlib.scylla_podman_cluster import _inspect_container, _RUNNING_CONTAINER_STATES
        info = _inspect_container(self.container_name())
        if info is None:
            return False
        return info.get("State", {}).get("Status") in _RUNNING_CONTAINER_STATES

    def to_dict(self):
        return {
            "dc": self.dc, "rack": self.rack, "image": self.image, "ip": self.ip,
            "lb_policy": self.lb_policy, "workload_mode": self.workload_mode,
            "keyspace_index": self.keyspace_index, "table_index": self.table_index,
        }


class LoaderSet:
    """Manages the collection of loader containers for a podman cluster."""

    def __init__(self, cluster):
        self.cluster = cluster
        self.loaders = OrderedDict()  # name -> Loader
        # Schema knobs are cluster-wide (shared by every loader), not per-loader.
        self.schema_config = dict(DEFAULT_SCHEMA_CONFIG)

    def configure_schema(self, keyspaces=None, tables_per_keyspace=None, replication_factor=None):
        for name, value in (("keyspaces", keyspaces), ("tables_per_keyspace", tables_per_keyspace),
                            ("replication_factor", replication_factor)):
            if value is None:
                continue
            if not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer, got {value!r}")
            self.schema_config[name] = value
        self.cluster._update_config()

    def add(self, dc, rack, count=1, image=DEFAULT_LOADER_IMAGE, lb_policy=DEFAULT_LB_POLICY,
            workload_mode=DEFAULT_WORKLOAD_MODE, keyspace_index=0, table_index=0):
        """Add `count` loaders to one rack. All loaders created by a single call share
        the same lb_policy/workload_mode/keyspace_index/table_index -- i.e. the workload
        is assigned per rack-group (this call), not per individual loader."""
        from ccmlib.scylla_podman_cluster import (
            LOADER_HOST_BASE,
            LOADER_HOST_MAX,
            _sanitize_podman_name,
        )
        if not isinstance(count, int) or count < 1:
            raise ValueError(f"count must be a positive integer, got {count!r}")
        if not (0 <= keyspace_index < self.schema_config["keyspaces"]):
            raise ValueError(
                f"keyspace_index {keyspace_index} out of range for "
                f"{self.schema_config['keyspaces']} configured keyspace(s)"
            )
        if not (0 <= table_index < self.schema_config["tables_per_keyspace"]):
            raise ValueError(
                f"table_index {table_index} out of range for "
                f"{self.schema_config['tables_per_keyspace']} configured table(s) per keyspace"
            )
        topo = self.cluster.network_topology
        if topo is None:
            raise RuntimeError("Cluster has no network topology; loaders require a podman cluster")
        key = (dc, rack)
        if key not in topo.rack_networks:
            raise RuntimeError(
                f"Unknown rack {dc}/{rack}: available racks are {list(topo.rack_networks.keys())}"
            )
        rack_info = topo.rack_networks[key]
        network = rack_info["network_name"]
        rack_idx = rack_info["rack_idx"]

        used_offsets = {
            int(loader.ip.rsplit(".", 1)[-1])
            for loader in self.loaders.values()
            if (loader.dc, loader.rack) == key and loader.ip
        }
        created = []
        offset = LOADER_HOST_BASE
        for _ in range(count):
            while offset in used_offsets:
                offset += 1
            if offset > LOADER_HOST_MAX:
                raise RuntimeError(
                    f"Loader band exhausted for {dc}/{rack}: max "
                    f"{LOADER_HOST_MAX - LOADER_HOST_BASE + 1} loaders per rack"
                )
            name = f"loader-{_sanitize_podman_name(dc)}-{_sanitize_podman_name(rack)}-{offset}"
            ip = f"{topo.subnet_prefix}.{rack_idx}.{offset}"
            loader = Loader(self.cluster, name, dc, rack, ip, network, image, lb_policy=lb_policy,
                            workload_mode=workload_mode, keyspace_index=keyspace_index,
                            table_index=table_index, schema_config=self.schema_config)
            # loader.start() force-removes the container it just created on
            # failure; nothing to roll back here besides re-raising, and
            # loaders already persisted below survive the failure intact.
            loader.start()
            # Persist immediately: a later loader in this batch failing must
            # not strand this one running-but-unrecorded.
            self.loaders[name] = loader
            self.cluster._update_config()
            used_offsets.add(offset)
            created.append(loader)
            offset += 1
        return created

    def remove(self, names=None):
        targets = list(self.loaders.values()) if names is None else [
            self.loaders[n] for n in names if n in self.loaders
        ]
        for loader in targets:
            try:
                loader.stop()
            except Exception:
                LOGGER.warning("Failed to stop loader %s", loader.name, exc_info=True)
                continue
            # Only drop the bookkeeping entry once the container is actually gone.
            self.loaders.pop(loader.name, None)
            self.cluster._update_config()

    def status(self):
        return {name: loader.is_running() for name, loader in self.loaders.items()}

    def to_dict(self):
        return {
            "loaders": OrderedDict((name, loader.to_dict()) for name, loader in self.loaders.items()),
            "schema_config": self.schema_config,
        }

    @classmethod
    def from_dict(cls, cluster, data):
        """Reconstruct a LoaderSet from persisted config.

        Containers themselves are re-probed by name (see Loader.is_running/stop),
        not trusted from disk -- same reconnect philosophy as the monitoring stack.
        """
        loader_set = cls(cluster)
        loader_set.schema_config.update((data or {}).get("schema_config", {}))
        topo = cluster.network_topology
        for name, info in (data or {}).get("loaders", {}).items():
            dc, rack = info["dc"], info["rack"]
            image = info.get("image", DEFAULT_LOADER_IMAGE)
            lb_policy = info.get("lb_policy", DEFAULT_LB_POLICY)
            workload_mode = info.get("workload_mode", DEFAULT_WORKLOAD_MODE)
            keyspace_index = info.get("keyspace_index", 0)
            table_index = info.get("table_index", 0)
            key = (dc, rack)
            network = topo.rack_networks[key]["network_name"] if topo and key in topo.rack_networks else None
            ip = info.get("ip")
            loader_set.loaders[name] = Loader(cluster, name, dc, rack, ip, network, image, lb_policy=lb_policy,
                                               workload_mode=workload_mode, keyspace_index=keyspace_index,
                                               table_index=table_index, schema_config=loader_set.schema_config)
        return loader_set
