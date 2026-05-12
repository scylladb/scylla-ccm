# ccm podman-based scylla cluster with network topology support

import ipaddress
import hashlib
import json
import logging
import os
import re
import subprocess
import threading
import time
import warnings
from collections import OrderedDict
from multiprocessing import cpu_count as host_cpu_count
from shutil import copyfile, which
from subprocess import run, PIPE, DEVNULL, STDOUT, Popen

from ruamel.yaml import YAML

from ccmlib import common
from ccmlib.node import (
    NodeError,
    Status,
)
from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_node import ScyllaNode
from ccmlib.utils.version import parse_version

LOGGER = logging.getLogger("ccm")

# Subnet allocation scheme:
#   Rack networks:  10.{prefix_octet}.{rack_idx}.0/24  (rack_idx starts at 1)
#   Gateway for each rack network: 10.{prefix_octet}.{rack_idx}.254
#   Node IPs within a rack: 10.{prefix_octet}.{rack_idx}.{node_idx}
#   Client container on Rack1: 10.{prefix_octet}.1.100
DEFAULT_RACK_SUBNET_PREFIX = "10.89"
RACK_GATEWAY_HOST = 254
CLIENT_CONTAINER_HOST = 100
SUBNET_PREFIX_ENV = "CCM_PODMAN_SUBNET_PREFIX"
CONTAINER_NET_INTERFACE = os.environ.get("CCM_PODMAN_NET_INTERFACE", "eth0")
if not re.fullmatch(r"[a-zA-Z0-9._-]{1,15}", CONTAINER_NET_INTERFACE):
    raise ValueError(
        f"Invalid CCM_PODMAN_NET_INTERFACE value {CONTAINER_NET_INTERFACE!r}: "
        "must be 1-15 alphanumeric, dot, hyphen, or underscore characters"
    )
PODMAN_RESOURCE_OWNER_LABEL = "org.scylladb.ccm-owner-pid"
BUSYBOX_IMAGE = os.environ.get("CCM_PODMAN_BUSYBOX_IMAGE", "busybox")
_IMAGE_RUNTIME_USER_CACHE = {}
_RUNNING_CONTAINER_STATES = frozenset(("running", "created", "paused"))


def _busybox_chmod(host_path, container_path, permissions, description="busybox chmod"):
    """Run busybox chmod inside podman, logging a warning on failure."""
    res = run(
        [
            "podman", "run", "--rm",
            "-v", f"{host_path}:{container_path}",
            BUSYBOX_IMAGE,
            "chmod", "-R", permissions, container_path,
        ],
        stdout=DEVNULL,
        stderr=PIPE,
        text=True,
    )
    if res.returncode != 0:
        LOGGER.warning(
            "%s on %s failed (rc=%d): %s",
            description, host_path, res.returncode, res.stderr.strip(),
        )


def _sanitize_podman_name(name):
    """Return a podman-safe name component with a stable fallback."""

    original = str(name)
    sanitized = re.sub(r"-+", "-", re.sub(r"[^a-z0-9-]", "-", original.lower())).strip(
        "-"
    )
    if sanitized:
        return sanitized
    return f"unnamed-{hashlib.sha1(original.encode('utf-8'), usedforsecurity=False).hexdigest()[:8]}"


def _pid_is_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _resource_owner_pid(labels):
    if not isinstance(labels, dict):
        return None
    owner_pid = labels.get(PODMAN_RESOURCE_OWNER_LABEL)
    if owner_pid is None:
        return None
    try:
        return int(owner_pid)
    except (TypeError, ValueError):
        return None


def _inspect_podman_json(command):
    res = run(command, stdout=PIPE, stderr=DEVNULL, text=True)
    if res.returncode != 0 or not res.stdout.strip():
        return None
    try:
        payload = json.loads(res.stdout)
    except json.JSONDecodeError:
        LOGGER.warning("Failed to parse podman inspect output for %r", command)
        return None
    if isinstance(payload, list):
        return payload[0] if payload else None
    return payload


def _inspect_container(name_or_id):
    return _inspect_podman_json(["podman", "inspect", name_or_id])


def _inspect_network(name):
    return _inspect_podman_json(["podman", "network", "inspect", name])


def _container_owner_labels(container_info):
    if not isinstance(container_info, dict):
        return {}
    config = container_info.get("Config", {})
    if not isinstance(config, dict):
        return {}
    labels = config.get("Labels", {})
    return labels if isinstance(labels, dict) else {}


def _network_owner_labels(network_info):
    if not isinstance(network_info, dict):
        return {}
    labels = network_info.get("labels", {})
    return labels if isinstance(labels, dict) else {}


def _network_attached_container_names(network_info):
    if not isinstance(network_info, dict):
        return set()
    containers = network_info.get("containers", {})
    if not isinstance(containers, dict):
        return set()
    return {
        details.get("name")
        for details in containers.values()
        if isinstance(details, dict) and details.get("name")
    }


def _remove_named_container_if_safe(
    container_name,
    allow_reuse_current_running=False,
    allow_remove_current_running=False,
):
    """Safely handle an existing deterministic-name container.

    Returns the inspected container info when ``allow_reuse_current_running`` is
    True and a current-process running container is reused. Otherwise returns
    ``None`` after removing a stale container or when no container exists.
    """
    container_info = _inspect_container(container_name)
    if container_info is None:
        return None

    owner_pid = _resource_owner_pid(_container_owner_labels(container_info))
    state = container_info.get("State", {}).get("Status", "unknown")
    if owner_pid is None:
        raise RuntimeError(
            f"Refusing to remove existing container {container_name}: "
            f"missing {PODMAN_RESOURCE_OWNER_LABEL} label"
        )
    if owner_pid != os.getpid() and _pid_is_alive(owner_pid):
        raise RuntimeError(
            f"Refusing to remove existing container {container_name}: "
            f"owned by live process {owner_pid}"
        )
    if owner_pid == os.getpid() and state in _RUNNING_CONTAINER_STATES:
        if allow_reuse_current_running:
            return container_info
        if not allow_remove_current_running:
            raise RuntimeError(
                f"Refusing to remove existing running container {container_name}: "
                "it is already owned by this process"
            )

    res = run(
        ["podman", "rm", "--volumes", "-f", container_name],
        stdout=PIPE,
        stderr=PIPE,
        text=True,
    )
    if res.returncode != 0:
        raise RuntimeError(
            f"Failed to remove existing container {container_name}: {res.stderr}"
        )
    return None


class PodmanProcess:
    """A lightweight adapter that mimics subprocess.Popen for podman containers.

    The parent ScyllaNode.start() expects _start_scylla() to return a Popen-like
    object with a .pid attribute. This adapter wraps a podman container ID to
    satisfy that interface.
    """

    def __init__(self, container_id):
        self.pid = container_id
        self.returncode = None

    def poll(self):
        """Check if the container is still running; update returncode if it exited."""
        if self.returncode is not None:
            return self.returncode
        try:
            res = run(
                [
                    "podman",
                    "inspect",
                    "--format",
                    "{{.State.Status}}:{{.State.ExitCode}}",
                    self.pid,
                ],
                stdout=PIPE,
                stderr=DEVNULL,
                text=True,
            )
            if res.returncode != 0:
                # Container doesn't exist anymore
                self.returncode = -1
                return self.returncode
            output = res.stdout.strip()
            if ":" in output:
                status, exit_code = output.rsplit(":", 1)
                if status not in _RUNNING_CONTAINER_STATES:
                    try:
                        self.returncode = int(exit_code)
                    except ValueError:
                        LOGGER.warning(
                            "Unexpected exit code %r for container %s",
                            exit_code,
                            self.pid,
                        )
                        self.returncode = -1
        except FileNotFoundError:
            LOGGER.warning("podman not found; marking container %s as dead", self.pid)
            self.returncode = -1
        except Exception:
            LOGGER.debug("poll() failed for container %s", self.pid, exc_info=True)
        return self.returncode


def _get_container_host_pid(container_id):
    """Return the host-visible PID of a podman container's init process.

    This PID is used with ``nsenter`` to enter the container's network namespace
    from the host, allowing us to run ``ip`` and ``tc`` commands using the host's
    binaries rather than requiring them inside the container.
    """
    res = run(
        ["podman", "inspect", "--format", "{{.State.Pid}}", container_id],
        stdout=PIPE,
        stderr=PIPE,
        text=True,
    )
    if res.returncode != 0:
        raise RuntimeError(
            f"Failed to get host PID for container {container_id}: {res.stderr}"
        )
    pid = res.stdout.strip()
    if not pid or pid == "0":
        raise RuntimeError(f"Container {container_id} is not running (host PID={pid})")
    try:
        return int(pid)
    except ValueError as exc:
        raise RuntimeError(
            f"Unexpected host PID value for container {container_id}: {pid!r}"
        ) from exc


def _nsenter_net_run(container_id, command, check=False):
    """Run a command inside a container's network namespace using nsenter.

    Uses ``nsenter --user --net`` to enter the container's user and network
    namespaces, then executes the given command using the *host's* binaries
    (e.g. ``ip``, ``tc``).  This avoids installing networking tools inside the
    container image.

    Args:
        container_id: podman container name or ID
        command: list of command arguments (e.g. ["ip", "route", "add", ...])
        check: if True, raise on non-zero exit

    Returns:
        subprocess.CompletedProcess
    """
    host_pid = _get_container_host_pid(container_id)
    full_cmd = ["nsenter", "-t", str(host_pid), "--user", "--net"] + list(command)
    try:
        res = run(full_cmd, stdout=PIPE, stderr=PIPE, text=True)
    except FileNotFoundError as exc:
        # nsenter (from util-linux) or the command itself (e.g. ip, tc
        # from iproute2) is not installed on the host.
        raise RuntimeError(
            f"Host binary not found while running {full_cmd!r}: {exc}. "
            f"Ensure 'nsenter' (util-linux) and 'ip'/'tc' (iproute2) "
            f"are installed on the host."
        ) from exc
    if check and res.returncode != 0:
        raise RuntimeError(
            f"nsenter command failed (container={container_id}): "
            f"cmd={command} stderr={res.stderr}"
        )
    return res


def _make_path_container_writable(path):
    """Make a host path writable for non-root users inside a bind-mounted container.

    Uses 0o775/0o664 (group-writable) rather than world-writable permissions.
    The container user (uid=999) typically shares the host user's group via
    podman's user namespace mapping, so group-write is sufficient.
    """
    if not os.path.exists(path):
        return

    def chmod_if_possible(target_path, mode):
        try:
            os.chmod(target_path, mode)
        except OSError as exc:
            LOGGER.warning("Failed to chmod %s to %s: %s", target_path, oct(mode), exc)

    if os.path.isdir(path):
        chmod_if_possible(path, 0o775)
        for root, dirs, files in os.walk(path):
            for dirname in dirs:
                chmod_if_possible(os.path.join(root, dirname), 0o775)
            for filename in files:
                chmod_if_possible(os.path.join(root, filename), 0o664)
    else:
        chmod_if_possible(path, 0o664)


def _get_image_runtime_user(image_name):
    cached = _IMAGE_RUNTIME_USER_CACHE.get(image_name)
    if cached is not None:
        return cached

    res = run(
        [
            "podman",
            "run",
            "--rm",
            "--entrypoint",
            "sh",
            image_name,
            "-lc",
            "id -u; id -g",
        ],
        stdout=PIPE,
        stderr=PIPE,
        text=True,
    )
    if res.returncode != 0:
        LOGGER.warning(
            "Failed to determine runtime user for image %s: %s",
            image_name, res.stderr,
        )
        return None

    lines = [line.strip() for line in res.stdout.splitlines() if line.strip()]
    if len(lines) < 2:
        LOGGER.warning(
            "Unexpected runtime user output for image %s: %s",
            image_name, res.stdout,
        )
        return None

    try:
        runtime_user = (int(lines[0]), int(lines[1]))
    except ValueError:
        LOGGER.warning(
            "Invalid runtime user output for image %s: %s",
            image_name, res.stdout,
        )
        return None

    _IMAGE_RUNTIME_USER_CACHE[image_name] = runtime_user
    return runtime_user


def _chown_path_for_container(path, uid, gid):
    res = run(
        ["podman", "unshare", "chown", "-R", f"{uid}:{gid}", path],
        stdout=PIPE,
        stderr=PIPE,
        text=True,
    )
    if res.returncode != 0:
        LOGGER.warning(
            "Failed to chown %s to %s:%s for container access: %s",
            path, uid, gid, res.stderr,
        )
        return False
    return True


def _list_podman_ipv4_networks():
    """Return all IPv4 podman network subnets visible to the local podman daemon."""
    res = run(
        ["podman", "network", "ls", "--format", "json"],
        stdout=PIPE,
        stderr=PIPE,
        text=True,
    )
    if res.returncode != 0:
        LOGGER.warning("Failed to list podman networks: %s", res.stderr)
        return []

    try:
        networks = json.loads(res.stdout)
    except json.JSONDecodeError:
        LOGGER.warning("Failed to parse podman network ls output as JSON")
        return []

    subnets = []
    for network in networks:
        for subnet_info in network.get("subnets", []):
            subnet = subnet_info.get("subnet")
            if not subnet:
                continue
            try:
                parsed = ipaddress.ip_network(subnet, strict=False)
            except ValueError:
                continue
            if parsed.version == 4:
                subnets.append(parsed)
    return subnets


def _find_available_subnet_prefix(exclude_prefixes=None):
    """Pick a free 10.x.0.0/16 prefix for podman rack networks."""
    exclude_prefixes = set(exclude_prefixes or [])
    env_prefix = os.environ.get(SUBNET_PREFIX_ENV)
    if env_prefix:
        # Basic validation: must be "10.X" where X is 0-255
        parts = env_prefix.split(".")
        if len(parts) != 2 or parts[0] != "10":
            raise ValueError(
                f"{SUBNET_PREFIX_ENV}={env_prefix!r} is invalid; "
                f"expected format '10.X' where X is 0-255"
            )
        try:
            second = int(parts[1])
        except ValueError:
            raise ValueError(
                f"{SUBNET_PREFIX_ENV}={env_prefix!r} is invalid; "
                f"second octet must be an integer"
            )
        if not 0 <= second <= 255:
            raise ValueError(
                f"{SUBNET_PREFIX_ENV}={env_prefix!r} is invalid; "
                f"second octet must be 0-255, got {second}"
            )
        # Check for conflicts with existing podman networks even when an
        # explicit prefix is given — the user may not be aware of collisions.
        used_subnets = _list_podman_ipv4_networks()
        candidate = ipaddress.ip_network(f"{env_prefix}.0.0/16")
        for used in used_subnets:
            if candidate.overlaps(used):
                LOGGER.warning(
                    "%s=%s overlaps existing podman network %s — using anyway",
                    SUBNET_PREFIX_ENV,
                    env_prefix,
                    used,
                )
                break
        return env_prefix

    used_subnets = _list_podman_ipv4_networks()
    # Start at 10.89 to avoid common ranges: 10.0/8 (cloud VPCs),
    # 10.88.0.0/16 (podman default CNI bridge).
    for second_octet in range(89, 256):
        prefix = f"10.{second_octet}"
        if prefix in exclude_prefixes:
            continue
        candidate = ipaddress.ip_network(f"{prefix}.0.0/16")
        if any(candidate.overlaps(used_subnet) for used_subnet in used_subnets):
            continue
        return prefix

    raise RuntimeError("Could not find a free 10.x.0.0/16 subnet prefix for podman")


def _is_subnet_conflict(stderr_text):
    if not stderr_text:
        return False
    lowered = stderr_text.lower()
    return "subnet" in lowered and ("already used" in lowered or "overlaps" in lowered)


class PodmanNetworkTopology:
    """Manages podman networks for a topology-aware ScyllaDB cluster.

    Creates one podman network per rack. Nodes in the same rack share a network.
    The host routes between rack subnets. Latency is simulated by applying
    ``tc``/``netem`` rules via ``nsenter`` from the host into each container's
    network namespace — the host's ``tc`` binary is used, so no networking
    tools need to be installed inside the container image.
    """

    def __init__(
        self,
        cluster_name,
        topology,
        inter_rack_delay_ms=1,
        inter_dc_delay_ms=50,
        packet_loss_percent=0.0,
        subnet_prefix=DEFAULT_RACK_SUBNET_PREFIX,
    ):
        """
        Args:
            cluster_name: CCM cluster name (used in network naming)
            topology: OrderedDict[dc_name -> OrderedDict[rack_name -> node_count]]
            inter_rack_delay_ms: Latency in ms between racks in the same DC
            inter_dc_delay_ms: Latency in ms between different DCs
            packet_loss_percent: Packet loss percentage for cross-DC traffic
            subnet_prefix: The 10.x prefix used for rack subnets (for example 10.89)
        """
        self.cluster_name = cluster_name
        self.topology = topology
        if inter_rack_delay_ms < 0:
            raise ValueError(
                f"inter_rack_delay_ms must be >= 0, got {inter_rack_delay_ms}"
            )
        if inter_dc_delay_ms < 0:
            raise ValueError(f"inter_dc_delay_ms must be >= 0, got {inter_dc_delay_ms}")
        if not (0.0 <= packet_loss_percent <= 100.0):
            raise ValueError(
                f"packet_loss_percent must be between 0 and 100, got {packet_loss_percent}"
            )
        self.inter_rack_delay_ms = inter_rack_delay_ms
        self.inter_dc_delay_ms = inter_dc_delay_ms
        self.packet_loss_percent = packet_loss_percent
        self.subnet_prefix = subnet_prefix

        # Mapping: (dc, rack) -> {network_name, subnet, gateway, rack_idx}
        self.rack_networks = OrderedDict()
        # Mapping: node_name -> {dc, rack, ip, network_name, subnet}
        self.node_assignments = OrderedDict()
        # Mapping: dc_name -> set of rack subnets in that DC
        self.dc_subnets = {}

        self._build_assignments()

    def _build_assignments(self):
        """Build the IP/network assignments from the topology."""
        rack_idx = 0
        node_idx_global = 0

        for dc, racks in self.topology.items():
            dc_rack_subnets = []
            for rack, node_count in racks.items():
                rack_idx += 1
                if rack_idx > 255:
                    raise ValueError(
                        f"Too many racks ({rack_idx}): max 255 racks supported "
                        f"(subnet {self.subnet_prefix}.{rack_idx}.0/24 would be invalid)"
                    )
                subnet = f"{self.subnet_prefix}.{rack_idx}.0/24"
                gateway = f"{self.subnet_prefix}.{rack_idx}.{RACK_GATEWAY_HOST}"
                network_name = self._network_name(dc, rack)

                # Validate node count won't collide with gateway
                if node_count >= RACK_GATEWAY_HOST:
                    raise ValueError(
                        f"Too many nodes ({node_count}) in {dc}/{rack}: "
                        f"max {RACK_GATEWAY_HOST - 1} nodes per rack "
                        f"(gateway is at .{RACK_GATEWAY_HOST})"
                    )

                self.rack_networks[(dc, rack)] = {
                    "network_name": network_name,
                    "subnet": subnet,
                    "gateway": gateway,
                    "rack_idx": rack_idx,
                }
                dc_rack_subnets.append(subnet)

                for node_offset in range(1, node_count + 1):
                    node_idx_global += 1
                    node_name = f"node{node_idx_global}"
                    ip = f"{self.subnet_prefix}.{rack_idx}.{node_offset}"
                    self.node_assignments[node_name] = {
                        "dc": dc,
                        "rack": rack,
                        "ip": ip,
                        "network_name": network_name,
                        "subnet": subnet,
                        "rack_idx": rack_idx,
                    }
            self.dc_subnets[dc] = dc_rack_subnets

        # Validate that the first rack can fit the client container IP and
        # has at least one node.  The CQL client container sits on the first
        # rack network and ``start_client_container()`` computes routes from
        # the perspective of the first node (assumed to be on this rack).
        if self.rack_networks:
            first_rack_key = list(self.rack_networks.keys())[0]
            first_rack_nodes = self.topology[first_rack_key[0]][first_rack_key[1]]
            if first_rack_nodes < 1:
                raise ValueError(
                    f"First rack ({first_rack_key[0]}/{first_rack_key[1]}) must "
                    f"have at least 1 node (client container shares its network)"
                )
            if first_rack_nodes >= CLIENT_CONTAINER_HOST:
                raise ValueError(
                    f"Too many nodes ({first_rack_nodes}) in first rack: "
                    f"max {CLIENT_CONTAINER_HOST - 1} nodes in first rack "
                    f"(client container uses .{CLIENT_CONTAINER_HOST})"
                )

    def _network_name(self, dc, rack):
        """Generate a podman network name for a rack."""

        safe_cluster = _sanitize_podman_name(self.cluster_name)
        safe_dc = _sanitize_podman_name(dc)
        safe_rack = _sanitize_podman_name(rack)
        return f"ccm-{safe_cluster}-{safe_dc}-{safe_rack}"

    def create_networks(self):
        """Create all podman networks for the topology."""
        for (dc, rack), info in self.rack_networks.items():
            name = info["network_name"]
            subnet = info["subnet"]
            gateway = info["gateway"]
            network_info = _inspect_network(name)
            if network_info is not None:
                owner_pid = _resource_owner_pid(_network_owner_labels(network_info))
                attached_names = _network_attached_container_names(network_info)
                subnets = network_info.get("subnets", [])
                network_subnet = None
                network_gateway = None
                if isinstance(subnets, list) and subnets:
                    first_subnet = subnets[0]
                    if isinstance(first_subnet, dict):
                        network_subnet = first_subnet.get("subnet")
                        network_gateway = first_subnet.get("gateway")
                if owner_pid is None:
                    raise RuntimeError(
                        f"Refusing to remove existing network {name}: "
                        f"missing {PODMAN_RESOURCE_OWNER_LABEL} label"
                    )
                if owner_pid != os.getpid() and _pid_is_alive(owner_pid):
                    raise RuntimeError(
                        f"Refusing to remove existing network {name}: "
                        f"owned by live process {owner_pid}"
                    )
                if owner_pid == os.getpid():
                    if network_subnet == subnet and network_gateway == gateway:
                        LOGGER.debug("Reusing existing podman network %s (%s)", name, subnet)
                        continue
                    if attached_names:
                        raise RuntimeError(
                            f"Refusing to recreate in-use network {name}: "
                            f"containers still attached: {sorted(attached_names)}"
                        )
                rm_res = run(
                    ["podman", "network", "rm", "-f", name],
                    stdout=PIPE,
                    stderr=PIPE,
                    text=True,
                )
                if rm_res.returncode != 0:
                    raise RuntimeError(
                        f"Failed to remove existing podman network {name}: {rm_res.stderr}"
                    )
            res = run(
                [
                    "podman",
                    "network",
                    "create",
                    "--label",
                    f"{PODMAN_RESOURCE_OWNER_LABEL}={os.getpid()}",
                    "--subnet",
                    subnet,
                    "--gateway",
                    gateway,
                    name,
                ],
                stdout=PIPE,
                stderr=PIPE,
                text=True,
            )
            if res.returncode != 0:
                raise RuntimeError(
                    f"Failed to create podman network {name}: {res.stderr}"
                )
            LOGGER.debug("Created podman network %s (%s)", name, subnet)

    def destroy_networks(self):
        """Remove all podman networks for this topology."""
        for (dc, rack), info in self.rack_networks.items():
            name = info["network_name"]
            network_info = _inspect_network(name)
            if network_info is None:
                continue
            owner_pid = _resource_owner_pid(_network_owner_labels(network_info))
            if owner_pid is not None and owner_pid != os.getpid() and _pid_is_alive(owner_pid):
                LOGGER.warning(
                    "Skipping removal of network %s owned by live process %s",
                    name,
                    owner_pid,
                )
                continue
            res = run(
                ["podman", "network", "rm", "-f", name],
                stdout=PIPE,
                stderr=PIPE,
                text=True,
            )
            if res.returncode != 0:
                LOGGER.warning(
                    "Failed to remove podman network %s: %s",
                    name,
                    res.stderr.strip(),
                )
            else:
                LOGGER.debug("Removed podman network %s", name)

    def get_node_ip(self, node_name):
        """Get the assigned rack IP for a node."""
        return self.node_assignments[node_name]["ip"]

    def get_node_network(self, node_name):
        """Get the podman network name for a node's rack."""
        return self.node_assignments[node_name]["network_name"]

    def get_all_rack_subnets(self):
        """Return list of all rack subnets."""
        return [info["subnet"] for info in self.rack_networks.values()]

    def get_foreign_subnets(self, node_name):
        """Get subnets that are not the node's own rack subnet, grouped by relationship.

        Returns:
            dict with keys 'inter_rack' and 'inter_dc', each a list of subnet strings.
        """
        node_info = self.node_assignments[node_name]
        own_dc = node_info["dc"]
        own_subnet = node_info["subnet"]

        inter_rack = []
        inter_dc = []

        for (dc, rack), info in self.rack_networks.items():
            if info["subnet"] == own_subnet:
                continue
            if dc == own_dc:
                inter_rack.append(info["subnet"])
            else:
                inter_dc.append(info["subnet"])

        return {"inter_rack": inter_rack, "inter_dc": inter_dc}

    def get_routes_for_node(self, node_name):
        """Get the ip route commands needed inside a container for cross-rack connectivity.

        Returns a list of (destination_subnet, gateway_ip) tuples.
        """
        node_info = self.node_assignments[node_name]
        own_subnet = node_info["subnet"]
        own_gateway = self.rack_networks[(node_info["dc"], node_info["rack"])][
            "gateway"
        ]

        routes = []
        for (dc, rack), info in self.rack_networks.items():
            if info["subnet"] != own_subnet:
                routes.append((info["subnet"], own_gateway))
        return routes

    def build_tc_commands(self, node_name):
        """Build tc/netem commands to apply inside a container.

        Creates a classful qdisc with prio bands:
        - Band 1: default (no delay) — intra-rack traffic
        - Band 2: inter-rack same DC (configurable delay)
        - Band 3: inter-DC (configurable delay + optional packet loss)
        """
        foreign = self.get_foreign_subnets(node_name)
        commands = []

        iface = CONTAINER_NET_INTERFACE
        # Root qdisc: prio with 4 bands, all traffic defaults to band 1 (no delay)
        commands.append(
            f"tc qdisc add dev {iface} root handle 1: prio bands 4 "
            "priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0"
        )

        # Band 2: inter-rack same DC
        if foreign["inter_rack"] and self.inter_rack_delay_ms > 0:
            commands.append(
                f"tc qdisc add dev {iface} parent 1:2 handle 20: "
                f"netem delay {self.inter_rack_delay_ms}ms"
            )
            for subnet in foreign["inter_rack"]:
                commands.append(
                    f"tc filter add dev {iface} parent 1:0 protocol ip u32 "
                    f"match ip dst {subnet} flowid 1:2"
                )

        # Band 3: inter-DC
        if foreign["inter_dc"] and (self.inter_dc_delay_ms > 0 or self.packet_loss_percent > 0):
            netem_parts = []
            if self.inter_dc_delay_ms > 0:
                netem_parts.append(f"delay {self.inter_dc_delay_ms}ms")
            if self.packet_loss_percent > 0:
                netem_parts.append(f"loss {self.packet_loss_percent}%")
            commands.append(
                f"tc qdisc add dev {iface} parent 1:3 handle 30: "
                f"netem {' '.join(netem_parts)}"
            )
            for subnet in foreign["inter_dc"]:
                commands.append(
                    f"tc filter add dev {iface} parent 1:0 protocol ip u32 "
                    f"match ip dst {subnet} flowid 1:3"
                )

        return commands

    def get_client_ip(self):
        """Return the IP address for the CQL client container (on Rack1 network)."""
        if not self.rack_networks:
            raise RuntimeError("No rack networks have been created")
        # Client sits on the first rack network
        first_rack_key = next(iter(self.rack_networks))
        rack_idx = self.rack_networks[first_rack_key]["rack_idx"]
        return f"{self.subnet_prefix}.{rack_idx}.{CLIENT_CONTAINER_HOST}"

    def get_client_network(self):
        """Return the podman network name for the CQL client container."""
        if not self.rack_networks:
            raise RuntimeError("No rack networks have been created")
        first_rack_key = next(iter(self.rack_networks))
        return self.rack_networks[first_rack_key]["network_name"]

    def to_dict(self):
        """Serialize network state for persistence.

        Only the topology and delay parameters are persisted. node_assignments
        and rack_networks are deterministically recomputed from the topology
        by _build_assignments() on load.

        Note: ``topology`` is an ``OrderedDict`` but we convert to plain
        ``dict`` here.  This is intentional — ``ruamel.yaml`` preserves
        insertion order for mappings, so the round-trip is order-stable.
        """
        return {
            "topology": {dc: dict(racks) for dc, racks in self.topology.items()},
            "inter_rack_delay_ms": self.inter_rack_delay_ms,
            "inter_dc_delay_ms": self.inter_dc_delay_ms,
            "packet_loss_percent": self.packet_loss_percent,
            "subnet_prefix": self.subnet_prefix,
        }

    @classmethod
    def from_dict(cls, cluster_name, data):
        """Deserialize network state."""
        topology = OrderedDict()
        for dc, racks in data["topology"].items():
            topology[dc] = OrderedDict(racks)
        return cls(
            cluster_name=cluster_name,
            topology=topology,
            inter_rack_delay_ms=data.get("inter_rack_delay_ms", 1),
            inter_dc_delay_ms=data.get("inter_dc_delay_ms", 50),
            packet_loss_percent=data.get("packet_loss_percent", 0.0),
            subnet_prefix=data.get("subnet_prefix", DEFAULT_RACK_SUBNET_PREFIX),
        )


class ScyllaPodmanCluster(ScyllaCluster):
    """A ScyllaDB cluster running in podman containers with network topology support.

    Each node runs in a podman container on a per-rack podman network.
    The host routes between rack networks.  Latency simulation uses the
    host's ``tc``/``netem`` binaries applied via ``nsenter`` into each
    container's network namespace — no networking tools need to be
    installed inside the container image.
    A dedicated CQL client container sits on Rack1's network.
    """

    def __init__(self, *args, **kwargs):
        podman_img = kwargs.pop("podman_image", None)
        docker_img = kwargs.pop("docker_image", None)
        self.podman_image = podman_img or docker_img
        if not self.podman_image:
            raise common.ArgumentError(
                "podman_image is required for ScyllaPodmanCluster"
            )
        self.inter_rack_delay_ms = kwargs.pop("inter_rack_delay_ms", 1)
        self.inter_dc_delay_ms = kwargs.pop("inter_dc_delay_ms", 50)
        self.packet_loss_percent = kwargs.pop("packet_loss_percent", 0.0)
        self.pinning = kwargs.pop("pinning", False)
        self.network_topology = None
        self._client_container_id = None
        self._cpu_assignments = {}
        # Pass docker_image to parent so it skips install_dir validation
        kwargs["docker_image"] = self.podman_image
        super(ScyllaPodmanCluster, self).__init__(*args, **kwargs)

    def get_install_dir(self):
        return None

    def populate(
        self,
        nodes,
        debug=False,
        tokens=None,
        use_vnodes=False,
        ipprefix=None,
        ipformat=None,
    ):
        """Populate the cluster, creating podman networks and assigning IPs based on topology."""
        if ipprefix is not None:
            LOGGER.warning(
                "ipprefix is ignored for podman clusters (IPs come from network topology)"
            )
        if ipformat is not None:
            LOGGER.warning(
                "ipformat is ignored for podman clusters (IPs come from network topology)"
            )

        # Parse the topology exactly like the base class does
        topology = self._parse_topology(nodes)

        # Create the network topology manager
        tried_prefixes = set()
        self.network_topology = None
        max_subnet_retries = 167  # 10.89 through 10.255
        for _attempt in range(max_subnet_retries):
            subnet_prefix = _find_available_subnet_prefix(
                exclude_prefixes=tried_prefixes
            )
            tried_prefixes.add(subnet_prefix)
            self.network_topology = PodmanNetworkTopology(
                cluster_name=self.name,
                topology=topology,
                inter_rack_delay_ms=self.inter_rack_delay_ms,
                inter_dc_delay_ms=self.inter_dc_delay_ms,
                packet_loss_percent=self.packet_loss_percent,
                subnet_prefix=subnet_prefix,
            )
            try:
                self.network_topology.create_networks()
                break
            except RuntimeError as exc:
                # Clean up any partially-created networks before retrying
                self.network_topology.destroy_networks()
                if not _is_subnet_conflict(str(exc)) or os.environ.get(
                    SUBNET_PREFIX_ENV
                ):
                    raise
                LOGGER.warning(
                    "Podman subnet prefix %s is already in use; retrying with another prefix",
                    subnet_prefix,
                )
        else:
            raise RuntimeError(
                f"Could not find a free subnet prefix after {max_subnet_retries} attempts"
            )

        # Override ipformat so that get_node_ip returns our assigned IPs
        # We can't use the standard ip format since IPs come from the topology
        self.use_vnodes = use_vnodes

        # Build node_locations from topology
        node_count = 0
        node_locations = []
        dcs = list(topology.keys())
        for dc, racks in topology.items():
            for rack, n in racks.items():
                node_count += n
                for _ in range(n):
                    node_locations.append((dc, rack))

        if dcs != [None]:
            self.set_configuration_options(values={"endpoint_snitch": self.snitch})

        if node_count < 1:
            raise common.ArgumentError(f"invalid topology {topology}")

        for i in range(1, node_count + 1):
            if f"node{i}" in self.nodes:
                raise common.ArgumentError(f"Cannot create existing node node{i}")

        if tokens is None and not use_vnodes:
            if len(dcs) <= 1:
                tokens = self.balanced_tokens(node_count)
            else:
                tokens = self.balanced_tokens_across_dcs(node_locations)

        try:
            for i in range(1, node_count + 1):
                tk = None
                if tokens is not None and i - 1 < len(tokens):
                    tk = tokens[i - 1]
                dc, rack = node_locations[i - 1]
                self.new_node(
                    i, debug=debug, initial_token=tk, data_center=dc, rack=rack
                )
                self._update_config()
        except Exception:
            # Clean up any partially-created node directories, then destroy
            # networks to avoid leaked resources.
            LOGGER.warning(
                "populate() failed; cleaning up %d node(s) and podman networks for cluster %s",
                len(self.nodes), self.name,
            )
            for node in list(self.nodes.values()):
                try:
                    LOGGER.debug("Removing node directory: %s", node.get_path())
                    common.rmdirs(node.get_path())
                except Exception:
                    pass
            self.nodes.clear()
            self.network_topology.destroy_networks()
            raise

        self.cluster_cleanup()
        if self.pinning:
            self._refresh_cpu_assignments()
        return self

    def _compute_cpu_assignments(self):
        """Compute non-overlapping CPU assignments for each node.

        When pinning is enabled and there are enough host CPUs, each
        node is assigned a contiguous block of CPUs sized to that
        node's ``smp()`` value. If there are not enough host CPUs,
        pinning is disabled with a warning.

        The assignments are stored in ``self._cpu_assignments`` as
        ``{node_name: [cpu_id, ...]}``. The map is intentionally NOT
        persisted in cluster.conf -- it is recomputed on populate and
        before node starts so that a cluster loaded on a different
        machine (or after a CPU hotplug) gets a valid assignment.
        """
        nodes = list(self.nodes.values())
        if not nodes:
            self._cpu_assignments = {}
            return

        node_cpu_counts = [(node, int(node.smp())) for node in nodes]
        total_cores_needed = sum(node_smp for _, node_smp in node_cpu_counts)
        try:
            available = host_cpu_count()
        except NotImplementedError:
            LOGGER.warning("Cannot determine host CPU count; disabling CPU pinning")
            self.pinning = False
            self._cpu_assignments = {}
            return

        if total_cores_needed > available:
            LOGGER.warning(
                "CPU pinning requires %d cores across %d node(s) but host "
                "has only %d; disabling CPU pinning for this cluster",
                total_cores_needed,
                len(nodes),
                available,
            )
            self.pinning = False
            self._cpu_assignments = {}
            return

        LOGGER.info(
            "CPU pinning enabled: %d node(s) require %d cores (host has %d)",
            len(nodes),
            total_cores_needed,
            available,
        )
        assignments = {}
        cpu_offset = 0
        for node, node_smp in node_cpu_counts:
            core_list = list(range(cpu_offset, cpu_offset + node_smp))
            assignments[node.name] = core_list
            cpu_offset += node_smp
        self._cpu_assignments = assignments

    def _refresh_cpu_assignments(self):
        """Recompute CPU pinning from current node state when enabled."""
        if not self.pinning:
            self._cpu_assignments = {}
            return

        previous_pinning = self.pinning
        self._compute_cpu_assignments()
        if self.pinning != previous_pinning:
            self._update_config()

    def _parse_topology(self, nodes):
        """Parse the nodes argument into an OrderedDict topology, same as base class."""
        topology = OrderedDict()
        if isinstance(nodes, int):
            topology["dc1"] = OrderedDict([("RAC1", nodes)])
        elif isinstance(nodes, list):
            for i, n in enumerate(nodes):
                dc = f"dc{i + 1}"
                topology[dc] = OrderedDict([("RAC1", n)])
        elif isinstance(nodes, dict):
            for dc, x in nodes.items():
                if isinstance(x, int):
                    topology[dc] = OrderedDict([("RAC1", x)])
                elif isinstance(x, list):
                    topology[dc] = OrderedDict(
                        [(f"RAC{i}", n) for i, n in enumerate(x, start=1)]
                    )
                elif isinstance(x, dict):
                    topology[dc] = OrderedDict([(rack, n) for rack, n in x.items()])
                else:
                    raise common.ArgumentError(
                        f"invalid dc racks type {type(x)}: {x}: nodes={nodes}"
                    )
        else:
            raise common.ArgumentError(f"invalid nodes type {type(nodes)}: {nodes}")
        return topology

    def get_node_ip(self, nodeid):
        """Return the rack IP for a node from the topology."""
        if self.network_topology:
            node_name = f"node{nodeid}"
            if node_name in self.network_topology.node_assignments:
                return self.network_topology.node_assignments[node_name]["ip"]
        # Fallback during early initialization
        return super().get_node_ip(nodeid)

    def create_node(
        self,
        name,
        auto_bootstrap,
        storage_interface,
        jmx_port,
        remote_debug_port,
        initial_token,
        save=True,
        binary_interface=None,
        thrift_interface=None,
    ):
        if thrift_interface is not None:
            warnings.warn(
                "thrift_interface is deprecated and will be removed in a future version",
                DeprecationWarning,
                stacklevel=2,
            )

        return ScyllaPodmanNode(
            name,
            self,
            auto_bootstrap,
            storage_interface,
            jmx_port,
            remote_debug_port,
            initial_token,
            save=save,
            binary_interface=binary_interface,
            scylla_manager=self._scylla_manager,
        )

    def start_client_container(self):
        """Start a lightweight CQL client container on Rack1's network."""
        if not self.network_topology:
            return

        client_name = self._client_container_name()
        client_ip = self.network_topology.get_client_ip()
        client_network = self.network_topology.get_client_network()

        existing_container = _remove_named_container_if_safe(
            client_name, allow_reuse_current_running=True
        )
        if existing_container is not None:
            self._client_container_id = existing_container.get("Id", client_name)
            LOGGER.debug("Reusing existing CQL client container %s", client_name)
            return

        # Use the Scylla image so cqlsh is available in the client container.
        res = run(
            [
                "podman",
                "run",
                "-d",
                "--name",
                client_name,
                "--network",
                client_network,
                "--ip",
                client_ip,
                "--label",
                f"{PODMAN_RESOURCE_OWNER_LABEL}={os.getpid()}",
                "--cap-add",
                "NET_ADMIN",
                "--entrypoint",
                "sh",
                self.podman_image,
                "-lc",
                "sleep infinity",
            ],
            stdout=PIPE,
            stderr=PIPE,
            text=True,
        )

        if res.returncode != 0:
            raise RuntimeError(f"Failed to start CQL client container: {res.stderr}")

        self._client_container_id = res.stdout.strip()
        LOGGER.debug("Started CQL client container %s at %s", client_name, client_ip)

        try:
            # Set up routes to other rack subnets.
            # Routes and tc rules use nsenter (host's ip/tc binaries), so no tools
            # need to be installed inside the client container.
            if not self.network_topology.node_assignments:
                raise RuntimeError(
                    "Cannot set up client container routes: no nodes have been assigned"
                )
            first_node_name = next(iter(self.network_topology.node_assignments))
            self._setup_container_routes(client_name, first_node_name)

            # Apply tc rules (client is on Rack1, so same rules as a Rack1 node)
            tc_commands = self.network_topology.build_tc_commands(first_node_name)
            for cmd in tc_commands:
                res = _nsenter_net_run(self._client_container_id, ["sh", "-c", cmd])
                if res.returncode != 0:
                    LOGGER.warning(
                        "Failed to apply tc rule on client container: "
                        "cmd=%s stderr=%s",
                        cmd, res.stderr,
                    )
        except Exception:
            LOGGER.error(
                "Client container setup failed, cleaning up container %s",
                client_name,
            )
            self.stop_client_container()
            raise

    def stop_client_container(self):
        """Stop and remove the CQL client container."""
        client_name = self._client_container_name()
        container_info = _inspect_container(client_name)
        if container_info is not None:
            owner_pid = _resource_owner_pid(_container_owner_labels(container_info))
            if owner_pid is not None and owner_pid != os.getpid() and _pid_is_alive(owner_pid):
                LOGGER.warning(
                    "Skipping removal of client container %s owned by live process %s",
                    client_name,
                    owner_pid,
                )
            else:
                _remove_named_container_if_safe(
                    client_name,
                    allow_remove_current_running=True,
                )
        self._client_container_id = None

    def _client_container_name(self):
        dir_name = os.path.basename(os.path.dirname(self.get_path())).lstrip(".")
        return f"ccm-{_sanitize_podman_name(dir_name)}-{_sanitize_podman_name(self.name)}-client"

    def get_client_contact_points(self):
        """Return (host, port) list for CQL clients to connect to from the client container.

        The client container is on Rack1's network, so it can reach all nodes
        by their rack IPs (through host routing).
        """
        contact_points = []
        for node in self.nodelist():
            if node.is_running():
                ip = node.network_interfaces["binary"][0]
                port = node.network_interfaces["binary"][1]
                contact_points.append((ip, port))
        return contact_points

    def run_cqlsh_on_client(self, cql_command, node=None):
        """Execute a CQL command via the client container.

        Args:
            cql_command: CQL string to execute
            node: target node (defaults to first node)
        """
        if not self._client_container_id:
            self.start_client_container()
        if not self._client_container_id:
            raise RuntimeError("Client container did not start correctly")

        if node is None:
            nodes = self.nodelist()
            if not nodes:
                raise RuntimeError("No nodes available to run CQL against")
            node = next((candidate for candidate in nodes if candidate.is_running()), None)
            if node is None:
                raise RuntimeError("No running nodes available to run CQL against")

        ip = node.network_interfaces["binary"][0]
        port = node.network_interfaces["binary"][1]

        res = run(
            [
                "podman",
                "exec",
                self._client_container_id,
                "cqlsh",
                ip,
                str(port),
                "-e",
                cql_command,
            ],
            stdout=PIPE,
            stderr=PIPE,
            text=True,
        )
        if res.returncode != 0:
            LOGGER.warning(
                "cqlsh on client container returned non-zero exit code %d: %s",
                res.returncode, res.stderr.strip(),
            )
        return res.stdout, res.stderr

    def _setup_container_routes(self, container_name_or_id, node_name):
        """Set up IP routes inside a container for cross-rack connectivity.

        Uses ``nsenter`` to run the host's ``ip`` binary in the container's
        network namespace.
        """
        if not self.network_topology:
            return

        failed_routes = []
        routes = self.network_topology.get_routes_for_node(node_name)
        for dest_subnet, gateway in routes:
            res = _nsenter_net_run(
                container_name_or_id,
                ["ip", "route", "add", dest_subnet, "via", gateway],
            )
            if res.returncode != 0:
                failed_routes.append(
                    f"{dest_subnet} via {gateway}: {res.stderr.strip()}"
                )
        if failed_routes:
            raise RuntimeError(
                "Failed to add %d route(s) in %s: %s"
                % (len(failed_routes), container_name_or_id, "; ".join(failed_routes))
            )

    def start_nodes(
        self,
        nodes=None,
        no_wait=False,
        verbose=False,
        wait_for_binary_proto=None,
        wait_other_notice=None,
        wait_normal_token_owner=None,
        jvm_args=None,
        profile_options=None,
        quiet_start=False,
    ):
        """Start nodes, then apply tc/netem rules for latency simulation.

        Overrides ScyllaCluster.start_nodes() to defer tc rule application
        until after all nodes have their CQL interface ready.  This avoids
        Raft topology bootstrap being slowed by artificial inter-DC latency,
        which can cause joins to exceed the wait timeout.
        """
        if self.pinning:
            self._refresh_cpu_assignments()

        started = super().start_nodes(
            nodes=nodes,
            no_wait=no_wait,
            verbose=verbose,
            wait_for_binary_proto=wait_for_binary_proto,
            wait_other_notice=wait_other_notice,
            wait_normal_token_owner=wait_normal_token_owner,
            jvm_args=jvm_args,
            profile_options=profile_options,
            quiet_start=quiet_start,
        )
        # Apply tc/netem rules now that all nodes are running
        if self.network_topology:
            skipped = []
            for node in self.nodelist():
                if node.is_running() and node.pid:
                    node._apply_tc_rules()
                else:
                    skipped.append(node.name)
            if skipped:
                LOGGER.warning(
                    "Skipped tc/netem rules for %d node(s) that are not running: %s",
                    len(skipped), ", ".join(skipped),
                )
        return started

    def clear(self):
        """Remove all containers, then wipe node data directories.

        Overrides Cluster.clear() because the base implementation only stops
        the Scylla process inside containers via supervisorctl, leaving the
        containers themselves running. This override force-removes containers
        before wiping data, while preserving the current topology so the
        cluster can be started again in the same process.
        """
        # Force-stop and remove all node containers (podman rm -f handles running containers).
        # Keep the topology in memory so `cluster.start()` can recreate the
        # containers and reuse the same rack networks in this process.
        try:
            self.stop_client_container()
        except Exception:
            LOGGER.warning(
                "Failed to stop client container during clear()", exc_info=True
            )
        for n in list(self.nodes.values()):
            try:
                n.remove()
            except Exception:
                LOGGER.warning(
                    "Failed to remove container for node %s during clear()",
                    n.name,
                    exc_info=True,
                )
        # Wipe node data directories (node.pid is None after remove() so no container access)
        for n in list(self.nodes.values()):
            try:
                n.clear()
            except Exception:
                LOGGER.warning(
                    "Failed to clear data for node %s during clear()",
                    n.name,
                    exc_info=True,
                )

    def remove(
        self, node=None, wait_other_notice=False, other_nodes=None, remove_node_dir=True
    ):
        """Remove the cluster or a single node: stop containers, remove networks."""
        if node is not None:
            # Let the base class do orderly teardown first (removes from
            # self.nodes, honours wait_other_notice, calls node.stop()).
            # Only then force-remove the container to clean up any
            # residual process/volumes.
            super(ScyllaPodmanCluster, self).remove(
                node=node,
                wait_other_notice=wait_other_notice,
                other_nodes=other_nodes,
                remove_node_dir=remove_node_dir,
            )
            node.remove()
        else:
            # Full cluster removal: remove all containers, client, and networks.
            # Wrap each step in try/except to ensure we always attempt network
            # cleanup, even if earlier steps fail.
            try:
                self.stop_client_container()
            except Exception:
                LOGGER.warning(
                    "Failed to stop client container during remove()",
                    exc_info=True,
                )
            for n in list(self.nodes.values()):
                try:
                    n.remove()
                except Exception:
                    LOGGER.warning(
                        "Failed to remove container for node %s during remove()",
                        n.name,
                        exc_info=True,
                    )
            try:
                super(ScyllaPodmanCluster, self).remove(
                    node=None,
                    wait_other_notice=wait_other_notice,
                    other_nodes=other_nodes,
                    remove_node_dir=remove_node_dir,
                )
            finally:
                if self.network_topology:
                    try:
                        self.network_topology.destroy_networks()
                    except Exception:
                        LOGGER.warning(
                            "Failed to destroy podman networks during remove()",
                            exc_info=True,
                        )

    def _update_config(self, install_dir=None):
        """Persist podman and network topology config to cluster.conf."""
        node_list = [node.name for node in list(self.nodes.values())]
        seed_list = [node.name for node in self.seeds]
        filename = os.path.join(self.get_path(), "cluster.conf")

        cluster_config = {
            "name": self.name,
            "nodes": node_list,
            "seeds": seed_list,
            "partitioner": self.partitioner,
            "config_options": self._config_options,
            "dse_config_options": self._dse_config_options,
            "log_level": getattr(self, "_Cluster__log_level", "INFO"),
            "use_vnodes": self.use_vnodes,
            "id": self.id,
            "ipprefix": self.ipprefix,
            "docker_image": self.podman_image,
            "pinning": self.pinning,
        }
        if self.network_topology:
            cluster_config["network_topology"] = self.network_topology.to_dict()

        with open(filename, "w", encoding="utf-8") as f:
            YAML().dump(cluster_config, f)

    def remove_dir_with_retry(self, path):
        """Use podman to fix permissions before removing directories."""
        _busybox_chmod(path, "/node", "777", "remove_dir_with_retry chmod")
        super(ScyllaPodmanCluster, self).remove_dir_with_retry(path)

    @staticmethod
    def is_docker():
        return True

    @staticmethod
    def is_podman():
        return True


class ScyllaPodmanNode(ScyllaNode):
    """A ScyllaDB node running in a podman container with topology-aware networking.

    TODO: Cluster startup takes ~2 minutes per node (3-node cluster ~6-7 min total).
          This is significantly longer than expected; investigate root cause.
          Candidates: supervisorctl update triggering a second Scylla start cycle,
          gossip ring settling with --seeds handshake, or iproute install latency.
    """

    def __init__(self, *args, **kwargs):
        kwargs["save"] = False
        self.share_directories = [
            "data",
            "commitlogs",
            "hints",
            "view_hints",
            "saved_caches",
            "keys",
            "logs",
        ]
        super(ScyllaPodmanNode, self).__init__(*args, **kwargs)
        self.base_data_path = "/usr/lib/scylla"
        self.local_base_data_path = os.path.join(self.get_path(), "data")
        self.local_yaml_path = os.path.join(self.get_path(), "conf")
        dir_name = os.path.basename(os.path.dirname(self.cluster.get_path())).lstrip(".")
        self.podman_name = "-".join(
            [
                _sanitize_podman_name(dir_name),
                _sanitize_podman_name(self.cluster.name),
                _sanitize_podman_name(self.name),
            ]
        )
        self.jmx_port = "7199"
        self.log_thread = None
        self._cached_nodetool_support = {}
        self._cached_supervisor_programs = None

    def _supervisor_program_names(self):
        if self.pid is None:
            return set()
        # Return cached result if available (program names don't change
        # during the lifetime of a container).
        if self._cached_supervisor_programs is not None:
            return self._cached_supervisor_programs
        res = run(
            ["podman", "exec", self.pid, "supervisorctl", "status"],
            stdout=PIPE,
            stderr=PIPE,
            text=True,
        )
        # supervisorctl status exits with code 1 whenever any process is not
        # RUNNING (e.g. STOPPED after a graceful stop).  The output is still
        # valid program-list output, so parse it regardless of returncode.
        # Only bail out if stdout is completely empty (podman exec failed).
        if not res.stdout.strip():
            return set()
        programs = {
            line.split()[0]
            for line in res.stdout.splitlines()
            if line.strip() and ":" not in line.split()[0]
        }
        if programs:
            self._cached_supervisor_programs = programs
        return programs

    def _scylla_service_name(self):
        programs = self._supervisor_program_names()
        if "scylla" in programs:
            return "scylla"
        return "scylla-server"

    def _jmx_service_name(self):
        programs = self._supervisor_program_names()
        if "scylla-jmx" in programs:
            return "scylla-jmx"
        return None

    @property
    def has_jmx(self):
        return self._jmx_service_name() is not None

    def nodetool(self, cmd, capture_output=True, wait=True, timeout=None, verbose=True):
        """Run nodetool inside the podman container via 'podman exec'."""
        if self.pid is None:
            raise RuntimeError(
                f"Cannot run nodetool on {self.name}: no running container"
            )
        nodetool = ["podman", "exec", self.pid, "scylla", "nodetool"]
        # Check if the command is supported by the native nodetool
        command = next(
            (arg for arg in cmd.split() if not arg.startswith("-")), None
        )
        if command is None:
            raise RuntimeError(f"Could not determine nodetool subcommand from: {cmd!r}")
        cache = getattr(self, "_cached_nodetool_support", None)
        if cache is None:
            cache = {}
            self._cached_nodetool_support = cache
        if command not in cache:
            try:
                subprocess.check_call(
                    nodetool + [command, "--help"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.STDOUT,
                    timeout=30,
                )
                cache[command] = True
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                cache[command] = False
        if cache[command]:
            nodetool.extend(["-h", "localhost", "-p", str(self.api_port)])
            nodetool.extend(cmd.split())
            return self._do_run_nodetool(
                nodetool, capture_output, wait, timeout, verbose
            )

        if not self.has_jmx:
            raise RuntimeError(
                f"Node {self.name}: native nodetool does not support '{cmd}' "
                f"and JMX is not available for fallback"
            )
        # Fall back to java nodetool via JMX (running inside the container)
        jmx_nodetool = [
            "podman",
            "exec",
            self.pid,
            "nodetool",
            f"-Dcom.scylladb.apiPort={self.api_port}",
        ] + cmd.split()
        return self._do_run_nodetool(
            jmx_nodetool, capture_output, wait, timeout, verbose
        )

    def _prepare_bind_mounts(self):
        _make_path_container_writable(self.local_yaml_path)
        runtime_user = _get_image_runtime_user(self.cluster.podman_image)
        for host_path in [
            os.path.join(self.get_path(), directory)
            for directory in self.share_directories
        ]:
            _make_path_container_writable(host_path)
            if runtime_user is not None:
                uid, gid = runtime_user
                _chown_path_for_container(host_path, uid, gid)
                # After handing ownership to the container user via podman unshare,
                # the host user (uid != container subuid) can no longer write into
                # these directories.  Run chmod a+rwX inside the same user-namespace
                # so that both the container user AND the host user retain write
                # access.  This is critical for the logs/ directory: PodmanLogger
                # opens logs/system.log from the host side after the container has
                # taken ownership of the directory.
                res = run(
                    ["podman", "unshare", "chmod", "-R", "a+rwX", host_path],
                    stdout=PIPE,
                    stderr=PIPE,
                    text=True,
                )
                if res.returncode != 0:
                    LOGGER.warning(
                        "Failed to chmod %s to a+rwX via podman unshare: %s",
                        host_path, res.stderr,
                    )
                _make_path_container_writable(host_path)

    def _pinning_container_args(self):
        """Return podman run flags for CPU pinning, or empty list if not pinned."""
        assignments = getattr(self.cluster, "_cpu_assignments", {})
        cpus = assignments.get(self.name)
        if not cpus:
            return []
        cpuset_str = ",".join(str(c) for c in cpus)
        return ["--cpuset-cpus", cpuset_str]

    def _pinning_scylla_args(self, args):
        """Adjust Scylla command-line args for CPU pinning.

        When pinning is active for this node:
        - Inject ``--cpuset`` so Scylla binds to the assigned cores.
        - Remove ``--overprovisioned`` (the whole point of pinning is
          dedicated cores, so overprovisioned mode is wrong).
        - Add ``--io-setup 0`` to skip iotune and provide an
          ``io_properties.yaml`` file with tuning appropriate for the
          number of pinned cores.

        Returns a new args list (the original is not mutated).
        """
        assignments = getattr(self.cluster, "_cpu_assignments", {})
        cpus = assignments.get(self.name)
        if not cpus:
            return args

        args = list(args)  # don't mutate caller

        cpuset_str = ",".join(str(c) for c in cpus)
        # Add --cpuset if not already present
        if "--cpuset" not in args:
            args.extend(["--cpuset", cpuset_str])

        # Remove --overprovisioned (may appear as "--overprovisioned 1"
        # after filter_args conversion)
        while "--overprovisioned" in args:
            idx = args.index("--overprovisioned")
            # Remove flag and its value if the next element looks like
            # a value (not a flag)
            if idx + 1 < len(args) and not args[idx + 1].startswith("--"):
                del args[idx : idx + 2]
            else:
                del args[idx]

        # Write io_properties.yaml and tell Scylla to use it
        self._write_io_properties(len(cpus))
        # Replace any existing --io-setup value or add it
        if "--io-setup" in args:
            idx = args.index("--io-setup")
            if idx + 1 < len(args) and not args[idx + 1].startswith("--"):
                args[idx + 1] = "0"
            else:
                args.insert(idx + 1, "0")
        else:
            args.extend(["--io-setup", "0"])

        # Add --io-properties-file pointing to our generated file.
        # The file sits in the bind-mounted /etc/scylla inside the container.
        if "--io-properties-file" not in args:
            args.extend(["--io-properties-file", "/etc/scylla/io_properties.yaml"])

        return args

    def _write_io_properties(self, num_cpus):
        """Write an io_properties.yaml tuned for *num_cpus* pinned cores.

        The file is written into the node's conf directory which is
        bind-mounted to ``/etc/scylla`` inside the container.  The
        values are deliberately generous so that Scylla does not
        throttle itself unnecessarily in a test/dev environment.

        Returns the host-side path to the file.
        """
        # Use generous values: 100k IOPS per core, 1 GB/s bandwidth per core.
        # These are intentionally high to prevent Scylla from throttling I/O
        # in a test environment where we want maximum throughput.
        mountpoint = getattr(self, "base_data_path", "/usr/lib/scylla")
        io_props = {
            "disks": [
                {
                    "mountpoint": mountpoint,
                    "read_iops": 100000 * num_cpus,
                    "read_bandwidth": 1073741824 * num_cpus,
                    "write_iops": 100000 * num_cpus,
                    "write_bandwidth": 1073741824 * num_cpus,
                }
            ]
        }
        io_props_path = os.path.join(self.local_yaml_path, "io_properties.yaml")
        yaml = YAML()
        yaml.default_flow_style = False
        with open(io_props_path, "w", encoding="utf-8") as f:
            yaml.dump(io_props, f)
        return io_props_path

    def _get_directories(self):
        dirs = {}
        for dir_name in self.share_directories + ["conf"]:
            dirs[dir_name] = os.path.join(self.get_path(), dir_name)
        return dirs

    def is_scylla(self):
        return True

    @staticmethod
    def is_docker():
        return True

    @staticmethod
    def is_podman():
        return True

    def read_scylla_yaml(self):
        conf_file = os.path.join(self.get_conf_dir(), common.SCYLLA_CONF)
        with open(conf_file, "r", encoding="utf-8") as f:
            return YAML().load(f)

    def update_yaml(self):
        """Extract config from image if needed, then update scylla.yaml with podman-specific settings."""
        if not os.path.exists(os.path.join(self.local_yaml_path, "scylla.yaml")):
            # Extract /etc/scylla from the image using a temporary container
            res = run(
                [
                    "podman",
                    "run",
                    "-d",
                    "--label",
                    f"{PODMAN_RESOURCE_OWNER_LABEL}={os.getpid()}",
                    self.cluster.podman_image,
                    "tail",
                    "-f",
                    "/dev/null",
                ],
                stdout=PIPE,
                stderr=PIPE,
                text=True,
            )
            if res.returncode != 0:
                raise RuntimeError(
                    f"Failed to start temp container for config extraction: {res.stderr}"
                )
            container_id = res.stdout.strip()
            try:
                # Copy /etc/scylla/ contents from the container
                cp_res = run(
                    [
                        "podman",
                        "container",
                        "cp",
                        "-a",
                        f"{container_id}:/etc/scylla/",
                        "-",
                    ],
                    stdout=PIPE,
                    stderr=PIPE,
                )
                if cp_res.returncode == 0:
                    # Extract the tar archive into the local yaml path
                    # Use --skip-old-files instead of --keep-old-files to avoid
                    # spurious warnings about existing files
                    tar_res = run(
                        [
                            "tar",
                            "--skip-old-files",
                            "-x",
                            "--strip-components=1",
                            "-C",
                            self.local_yaml_path,
                        ],
                        input=cp_res.stdout,
                        stderr=PIPE,
                    )
                    if tar_res.returncode != 0:
                        stderr_text = tar_res.stderr.decode("utf-8", errors="replace") if isinstance(tar_res.stderr, bytes) else tar_res.stderr
                        LOGGER.warning(
                            "Failed to extract scylla config: %s", stderr_text
                        )
                else:
                    stderr_text = cp_res.stderr.decode("utf-8", errors="replace") if isinstance(cp_res.stderr, bytes) else cp_res.stderr
                    LOGGER.warning(
                        "Failed to copy config from container: %s", stderr_text
                    )
            finally:
                run(
                    ["podman", "rm", "-f", container_id],
                    stdout=DEVNULL,
                    stderr=DEVNULL,
                )
        super(ScyllaPodmanNode, self).update_yaml()

        conf_file = os.path.join(self.get_conf_dir(), common.SCYLLA_CONF)
        data = self.read_scylla_yaml()

        # Get the node's rack IP from the network topology
        node_ip = self._get_rack_ip()

        # ScyllaDB addresses
        data["listen_address"] = node_ip
        data["broadcast_address"] = node_ip
        data["rpc_address"] = "0.0.0.0"
        data["broadcast_rpc_address"] = node_ip
        data["api_address"] = "0.0.0.0"

        if "alternator_port" in data or "alternator_https_port" in data:
            data["alternator_address"] = "0.0.0.0"

        # Data directories inside the container
        data["data_file_directories"] = [os.path.join(self.base_data_path, "data")]
        data["commitlog_directory"] = os.path.join(self.base_data_path, "commitlogs")
        for directory in ["hints", "view_hints", "saved_caches"]:
            data[f"{directory}_directory"] = os.path.join(
                self.base_data_path, directory
            )

        # Override workdir to a container-internal path.  The parent
        # update_yaml() sets workdir,W to the host-side node directory
        # which does not exist inside the container and would cause
        # ScyllaDB to fail when creating the maintenance socket.
        data["workdir,W"] = self.base_data_path

        # Handle server encryption options
        server_encryption_options = data.get("server_encryption_options", {})
        if server_encryption_options:
            keys_dir_path = os.path.join(self.get_path(), "keys")
            os.makedirs(keys_dir_path, exist_ok=True)
            for key, file_path in list(server_encryption_options.items()):
                if isinstance(file_path, str) and os.path.isfile(file_path):
                    file_name = os.path.split(file_path)[1]
                    copyfile(src=file_path, dst=os.path.join(keys_dir_path, file_name))
                    server_encryption_options[key] = os.path.join(
                        self.base_data_path, "keys", file_name
                    )

        with open(conf_file, "w", encoding="utf-8") as f:
            YAML().dump(data, f)

    def _get_rack_ip(self):
        """Get this node's IP from the cluster's network topology."""
        if self.cluster.network_topology:
            return self.cluster.network_topology.get_node_ip(self.name)
        # Fallback
        return self.network_interfaces["storage"][0]

    def create_container(self, args):
        """Create and start the podman container for this node.

        The container is connected to its rack network with a static IP.
        After creation, IP routes and tc rules are set up.
        """
        if self.pid:
            # Verify the container is still alive.  If it was killed
            # externally (OOM, ``podman stop``, etc.) we need to clear
            # the stale pid so a fresh container can be created.
            res = run(
                ["podman", "inspect", "--format", "{{.State.Status}}", self.pid],
                stdout=PIPE, stderr=DEVNULL, text=True,
            )
            if res.returncode == 0 and res.stdout.strip() in ("running", "created", "paused"):
                return
            LOGGER.warning(
                "Container %s for node %s is no longer running (status: %s); "
                "will recreate",
                self.pid,
                self.name,
                res.stdout.strip() if res.returncode == 0 else "not found",
            )
            # Clean up the dead container
            run(["podman", "rm", "-f", self.pid], stdout=DEVNULL, stderr=DEVNULL)
            self.pid = None
            self._cached_supervisor_programs = None
            if self.log_thread:
                self.log_thread.stop()
                self.log_thread = None

        if not self.cluster.network_topology:
            raise RuntimeError(
                f"Cannot create container for {self.name}: "
                f"cluster network topology is not initialized"
            )

        self.cluster.network_topology.create_networks()

        node_ip = self._get_rack_ip()
        network_name = self.cluster.network_topology.get_node_network(self.name)

        # Build seed list
        node1 = self.cluster.nodelist()[0]
        seed_args = []
        if self.name != node1.name:
            seed_args = ["--seeds", node1.network_interfaces["storage"][0]]

        scylla_yaml = self.read_scylla_yaml()
        # Do not publish Alternator to fixed host ports. Each node already has a
        # stable per-rack container IP; host-port publishing would collide when
        # multiple nodes enable Alternator in the same cluster.
        port_args = []

        self._prepare_bind_mounts()

        # Volume mounts
        # Use :z for SELinux relabeling so rootless podman can read the config
        mount_args = [
            "-v",
            f"{self.local_yaml_path}:/etc/scylla:z",
            "-v",
            "/tmp:/tmp",
        ]
        for d in self.share_directories:
            mount_args.extend(
                [
                    "-v",
                    f"{os.path.join(self.get_path(), d)}:{os.path.join(self.base_data_path, d)}:z",
                ]
            )

        existing_container = _remove_named_container_if_safe(
            self.podman_name, allow_reuse_current_running=True
        )
        if existing_container is not None:
            self.pid = existing_container.get("Id", self.podman_name)
            self.network_interfaces = {
                k: (node_ip, v[1]) for k, v in list(self.network_interfaces.items())
            }
            return

        # Run the container on its rack network
        cmd = [
            "podman",
            "run",
            *port_args,
            *mount_args,
            "--name",
            self.podman_name,
            "--network",
            network_name,
            "--ip",
            node_ip,
            "--label",
            f"{PODMAN_RESOURCE_OWNER_LABEL}={os.getpid()}",
            "--cap-add",
            "NET_ADMIN",
            *self._pinning_container_args(),
            "-d",
            self.cluster.podman_image,
            *seed_args,
            *args,
        ]
        res = run(cmd, stdout=PIPE, stderr=PIPE, text=True)

        if res.returncode != 0:
            LOGGER.error(res)
            raise RuntimeError(
                f"Failed to create podman container {self.podman_name}: {res.stderr}"
            )

        self.pid = res.stdout.strip()

        try:
            # Start log streaming (stop any previous logger first for restart case)
            if self.log_thread:
                self.log_thread.stop()
            self.log_thread = PodmanLogger(
                self, os.path.join(self.get_path(), "logs", "system.log")
            )
            self.log_thread.start()

            # TODO: Replace this supervisord-specific readiness check with a runtime-
            # agnostic container startup probe once the image no longer uses supervisord.
            def is_container_runtime_ready():
                res = run(
                    ["podman", "exec", self.pid, "supervisorctl", "status"],
                    stdout=DEVNULL,
                    stderr=DEVNULL,
                )
                return res.returncode == 0

            if not common.wait_for(
                func=is_container_runtime_ready, timeout=30, step=0.2
            ):
                raise TimeoutError(
                    f"Container runtime for {self.name} did not become ready within 30 seconds"
                )

            # Stop scylla so that _start_scylla() can do a controlled
            # start after routes and tc rules are in place.  We do NOT
            # modify supervisord config files inside the container —
            # supervisorctl stop/start is sufficient for lifecycle
            # control.  The image's default autorestart=true is
            # harmless: supervisorctl stop sets the desired state to
            # STOPPED, and supervisord will not auto-restart a process
            # that was explicitly stopped.  For ungraceful stops
            # (kill -9), do_stop() calls supervisorctl stop immediately
            # after the kill to prevent an auto-restart.
            scylla_service = self._scylla_service_name()
            self.service_stop(scylla_service)
            jmx_service = self._jmx_service_name()
            if jmx_service:
                self.service_stop(jmx_service)

            # The image's entrypoint auto-starts scylla via supervisord
            # before we get a chance to stop it.  That transient run may
            # write Raft group0 state (system tables, commitlogs, etc.)
            # which will be stale when _start_scylla() does the real,
            # controlled restart — especially for non-seed nodes that
            # briefly attempt to join the seed.  Wipe the bind-mounted
            # data directories from inside the container (where the
            # scylla user has write permission) so the controlled start
            # begins with a clean slate.
            _transient_dirs = ("data", "commitlogs", "hints", "view_hints", "saved_caches")
            clean_res = run(
                [
                    "podman", "exec", self.pid, "bash", "-c",
                    # Remove directory *contents* rather than the directories
                    # themselves — the bind-mount points must remain.
                    " && ".join(
                        f"find {os.path.join(self.base_data_path, d)} -mindepth 1 -delete"
                        for d in _transient_dirs
                    ),
                ],
                stdout=DEVNULL, stderr=PIPE, text=True,
            )
            if clean_res.returncode != 0:
                LOGGER.warning(
                    "Failed to clean transient data for %s: %s",
                    self.name, clean_res.stderr.strip(),
                )

            # Update network interfaces with the actual rack IP
            self.network_interfaces = {
                k: (node_ip, v[1]) for k, v in list(self.network_interfaces.items())
            }

            # Set up routes to other rack subnets (for cross-rack/DC communication).
            # Routes and tc rules are applied via nsenter from the host, using
            # the host's ip/tc binaries — no tools need to be installed inside
            # the container.
            self._setup_routes()

            # tc/netem rules are applied later by the cluster's
            # start_nodes() method — after all nodes are running — so that
            # artificial latency does not slow down Raft topology bootstrap.
        except Exception:
            LOGGER.error(
                "Container setup failed for %s, cleaning up container %s",
                self.name, self.pid,
            )
            self._cached_supervisor_programs = None
            self._cached_nodetool_support = {}
            if self.log_thread:
                self.log_thread.stop()
                self.log_thread = None
            if self.pid:
                run(
                    ["podman", "rm", "--volumes", "-f", self.pid],
                    stdout=DEVNULL,
                    stderr=DEVNULL,
                )
                self.pid = None
            raise

    def _setup_routes(self):
        """Add IP routes inside the container for cross-rack connectivity.

        Uses ``nsenter`` to run the host's ``ip`` binary in the container's
        network namespace, avoiding any dependency on tools inside the image.
        """
        if not self.cluster.network_topology:
            return

        failed_routes = []
        routes = self.cluster.network_topology.get_routes_for_node(self.name)
        for dest_subnet, gateway in routes:
            res = _nsenter_net_run(
                self.pid,
                ["ip", "route", "add", dest_subnet, "via", gateway],
            )
            if res.returncode != 0:
                failed_routes.append(
                    f"{dest_subnet} via {gateway}: {res.stderr.strip()}"
                )
        if failed_routes:
            raise RuntimeError(
                "Failed to add %d route(s) in %s: %s"
                % (len(failed_routes), self.name, "; ".join(failed_routes))
            )

    def _apply_tc_rules(self):
        """Apply tc/netem rules for latency simulation.

        Uses ``nsenter`` to run the host's ``tc`` binary in the container's
        network namespace.  This avoids requiring ``iproute-tc`` inside the
        container image.

        On restart (container still alive from previous start), the old rules
        are removed first so the new rules can be applied cleanly.
        """
        if not self.cluster.network_topology:
            return

        tc_commands = self.cluster.network_topology.build_tc_commands(self.name)
        if not tc_commands:
            return

        # Remove any existing root qdisc so we can re-apply rules cleanly
        # (e.g. on restart when the container was not recreated).
        # Failure is expected on first start (no qdisc to delete).
        _nsenter_net_run(self.pid, ["sh", "-c", f"tc qdisc del dev {CONTAINER_NET_INTERFACE} root 2>/dev/null || true"])

        for cmd in tc_commands:
            res = _nsenter_net_run(self.pid, ["sh", "-c", cmd])
            if res.returncode != 0:
                LOGGER.warning(
                    "Failed to apply tc rule in %s: cmd=%s stderr=%s",
                    self.name, cmd, res.stderr,
                )

    def service_start(self, service_name):
        # Clear any FATAL/EXITED state so supervisord will accept the start
        # command.  After an ungraceful stop (kill -9) the process lands in
        # FATAL and supervisorctl refuses a plain "start" until cleared.
        status = self.service_status(service_name)
        if status and status.upper() in ("FATAL", "EXITED", "BACKOFF"):
            run(
                ["podman", "exec", self.pid, "supervisorctl", "clear", service_name],
                stdout=PIPE,
                stderr=PIPE,
                text=True,
            )
        res = run(
            [
                "podman",
                "exec",
                self.pid,
                "supervisorctl",
                "start",
                service_name,
            ],
            stdout=PIPE,
            stderr=PIPE,
            text=True,
        )
        if res.returncode != 0:
            LOGGER.debug(res.stdout)
            raise RuntimeError(
                f"service {service_name} failed to start in {self.name}: {res.stderr}"
            )

    def service_stop(self, service_name):
        # Pre-check: if the service is already stopped/exited/fatal, return
        # early to make stop idempotent.
        current_status = self.service_status(service_name)
        if current_status.upper() in ("STOPPED", "EXITED", "FATAL", "DOWN"):
            LOGGER.debug(
                "service %s in %s already %s; skipping stop",
                service_name, self.name, current_status,
            )
            return
        res = run(
            ["podman", "exec", self.pid, "supervisorctl", "stop", service_name],
            stdout=PIPE,
            stderr=PIPE,
            text=True,
        )
        if res.returncode != 0:
            LOGGER.debug(res.stdout)
            raise RuntimeError(
                f"service {service_name} failed to stop in {self.name}: {res.stderr}"
            )

    def service_status(self, service_name):
        if self.pid is None:
            return "DOWN"
        res = run(
            ["podman", "exec", self.pid, "supervisorctl", "status", service_name],
            stdout=PIPE,
            stderr=PIPE,
            text=True,
        )
        # supervisorctl status <name> exits with 1 when the process exists but
        # is not RUNNING (e.g. STOPPED/EXITED/FATAL).  The second token on
        # stdout is still the status string we need.  Only fall back to DOWN
        # when podman exec itself failed (empty stdout).
        parts = res.stdout.split()
        if len(parts) > 1:
            return parts[1]
        LOGGER.debug("service %s failed to get status in %s: %s", service_name, self.name, res.stderr)
        return "DOWN"

    def wait_for_binary_interface(self, **kwargs):
        timeout = kwargs.get("timeout", 420)
        process = kwargs.get("process")
        from_mark = kwargs.get("from_mark")
        start = time.time()

        def remaining_timeout():
            return max(0.0, timeout - (time.time() - start))

        if self.cluster.version() and parse_version(self.cluster.version()) >= parse_version("1.2"):
            self.watch_log_for(
                "Starting listening for CQL clients",
                from_mark=from_mark,
                process=process,
                timeout=remaining_timeout(),
            )

        binary_itf = self.network_interfaces["binary"]
        container_id = self.pid

        def is_binary_interface_listening():
            if process is not None:
                if process.poll() is not None:
                    raise NodeError(
                        f"Container {self.name} exited (rc={process.returncode}) "
                        f"before CQL interface became ready"
                    )
            if container_id is None:
                return False
            res = run(
                [
                    "podman",
                    "exec",
                    container_id,
                    "bash",
                    "-c",
                    f"echo > /dev/tcp/{binary_itf[0]}/{binary_itf[1]}",
                ],
                stdout=DEVNULL,
                stderr=DEVNULL,
            )
            return res.returncode == 0

        remaining = remaining_timeout()
        if not common.wait_for(func=is_binary_interface_listening, timeout=remaining, step=0.2):
            raise TimeoutError(
                f"Binary interface {binary_itf[0]}:{binary_itf[1]} did not start listening within {timeout} seconds"
            )

    def show(self, only_status=False, show_cluster=True):
        self._update_podman_status()
        indent = " " * (len(self.name) + 2)
        print(f"{self.name}: {self.__get_status_string()}")
        if not only_status:
            if show_cluster:
                print(f"{indent}cluster={self.cluster.name}")
            print(f"{indent}auto_bootstrap={self.auto_bootstrap}")
            if self.network_interfaces["binary"] is not None:
                print(f"{indent}binary={self.network_interfaces['binary']}")
            print(f"{indent}storage={self.network_interfaces['storage']}")
            print(f"{indent}jmx_port={self.jmx_port}")
            print(f"{indent}remote_debug_port={self.remote_debug_port}")
            print(f"{indent}initial_token={self.initial_token}")
            if self.data_center:
                print(f"{indent}data_center={self.data_center}")
            if self.rack:
                print(f"{indent}rack={self.rack}")
            if self.pid:
                print(f"{indent}pid={self.pid}")

    def __get_status_string(self):
        if self.status == Status.UNINITIALIZED:
            return f"{Status.DOWN} (Not initialized)"
        return self.status

    def _update_config(self):
        dir_name = self.get_path()
        if not os.path.exists(dir_name):
            return
        filename = os.path.join(dir_name, "node.conf")
        values = {
            "name": self.name,
            "status": self.status,
            "auto_bootstrap": self.auto_bootstrap,
            "interfaces": self.network_interfaces,
            "jmx_port": self.jmx_port,
            "docker_id": self.pid,  # reuse docker_id key for compat
            "podman_id": self.pid,
            "podman_name": self.podman_name,
            "install_dir": "",
            "config_options": getattr(self, "_Node__config_options", {}),
        }
        if self.initial_token is not None:
            values["initial_token"] = self.initial_token
        if self.remote_debug_port:
            values["remote_debug_port"] = self.remote_debug_port
        if self.data_center:
            values["data_center"] = self.data_center
        if self.rack:
            values["rack"] = self.rack
        if self.workload is not None:
            values["workload"] = self.workload
        with open(filename, "w", encoding="utf-8") as f:
            YAML().dump(values, f)

    @staticmethod
    def filter_args(args):
        """Filter command-line args for podman container compatibility.

        The incoming args list from ScyllaNode.start() begins with
        ``[launch_bin, '--options-file', options_file, ...]``.  We skip
        the launcher preamble and keep the remaining Scylla flags intact,
        except for a tiny set of known launcher-only/incompatible options.
        """
        # Work on a copy to avoid mutating the caller's list
        args = list(args)
        cleaned_args = []
        boolean_args = {"--experimental", "--disable-version-check"}
        drop_flags = {
            "--log-to-stdout",
            "--default-log-level",
            "--options-file",
        }
        if "--overprovisioned" in args or any(a.startswith("--overprovisioned=") for a in args):
            # Handle both "--overprovisioned VALUE" and "--overprovisioned=VALUE"
            for idx in range(len(args) - 1, -1, -1):
                if args[idx] == "--overprovisioned":
                    if idx + 1 < len(args) and not args[idx + 1].startswith("--"):
                        del args[idx : idx + 2]
                    else:
                        del args[idx]
                elif args[idx].startswith("--overprovisioned="):
                    del args[idx]
            args += ["--overprovisioned", "1"]

        # Find the start of flag arguments (skip the launch binary and
        # --options-file <path> preamble by looking for the first element
        # that starts with '--').
        flag_start = 0
        for i, a in enumerate(args):
            if a.startswith("--"):
                flag_start = i
                break

        i = flag_start
        while i < len(args):
            arg = args[i]
            if not arg.startswith("--"):
                i += 1
                continue
            # Handle --flag=value syntax by splitting on the first '='
            if "=" in arg:
                flag_name, value = arg.split("=", 1)
                i += 1
            elif arg in boolean_args:
                flag_name = arg
                value = ""
                i += 1
            elif i + 1 < len(args) and not args[i + 1].startswith("--"):
                flag_name = arg
                value = args[i + 1]
                i += 2
            else:
                flag_name = arg
                value = ""
                i += 1
            if flag_name == "--developer-mode" and value == "true":
                value = "1"
            if flag_name in drop_flags:
                continue
            if flag_name.startswith("--"):
                cleaned_args.append(flag_name)
                if value:
                    cleaned_args.append(value)
        return cleaned_args

    def _start_scylla(
        self,
        args,
        marks,
        update_pid,
        wait_other_notice,
        wait_normal_token_owner,
        wait_for_binary_proto,
        ext_env,
    ):
        if getattr(self.cluster, "pinning", False):
            self.cluster._refresh_cpu_assignments()

        args = self.filter_args(args)
        args = self._pinning_scylla_args(args)
        if ext_env:
            LOGGER.warning(
                "ext_env (SCYLLA_EXT_ENV) is not supported for podman clusters; "
                "environment settings will be ignored"
            )
        self.create_container(args)

        # Restart the log streamer if it was stopped (e.g. after do_stop)
        if not self.log_thread and self.pid:
            self.log_thread = PodmanLogger(
                self, os.path.join(self.get_path(), "logs", "system.log")
            )
            self.log_thread.start()

        scylla_status = self.service_status(self._scylla_service_name())
        if scylla_status and scylla_status.upper() != "RUNNING":
            self.service_start(self._scylla_service_name())

        if wait_other_notice:
            for node, mark in marks:
                node.watch_log_for_alive(self, from_mark=mark)

        # Reset cached host ID so it is re-fetched after restart, matching
        # the parent _start_scylla() behaviour (ScyllaNode line 529).
        self.node_hostid = None

        if wait_for_binary_proto:
            podman_process = PodmanProcess(self.pid)
            self.wait_for_binary_interface(
                from_mark=self.mark, process=podman_process, timeout=300
            )

        # Store the process adapter so the parent start_nodes() can pass it
        # to watch_log_for() for early death detection between sequential
        # node starts (ScyllaCluster.start_nodes line 143).
        self._process_scylla = PodmanProcess(self.pid)
        return self._process_scylla

    def do_stop(self, gently=True):
        # Stop the log streamer so it doesn't become orphaned
        if self.log_thread:
            self.log_thread.stop()
            self.log_thread = None

        if not self.pid:
            return

        if gently:
            jmx_service = self._jmx_service_name()
            if jmx_service:
                self.service_stop(jmx_service)
            self.service_stop(self._scylla_service_name())
        else:
            jmx_service = self._jmx_service_name()
            scylla_service = self._scylla_service_name()
            # Get the PID of the scylla service, then kill -9 via bash
            pid_res = run(
                [
                    "podman",
                    "exec",
                    self.pid,
                    "supervisorctl",
                    "pid",
                    scylla_service,
                ],
                stdout=PIPE,
                stderr=PIPE,
                text=True,
            )
            if pid_res.returncode == 0 and pid_res.stdout.strip():
                _pid = pid_res.stdout.strip()
                if not _pid.isdigit() or _pid == "0":
                    LOGGER.warning(
                        "Unexpected PID value from supervisorctl for %s "
                        "in %s: %r",
                        scylla_service, self.name, _pid,
                    )
                else:
                    run(
                        [
                            "podman",
                            "exec",
                            self.pid,
                            "bash",
                            "-c",
                            f"kill -9 {_pid}",
                        ],
                        stdout=PIPE,
                        stderr=PIPE,
                    )
            # Tell supervisord the service is stopped so autorestart
            # does not kick in after the kill -9.
            self.service_stop(scylla_service)
            if jmx_service:
                jmx_pid_res = run(
                    [
                        "podman",
                        "exec",
                        self.pid,
                        "supervisorctl",
                        "pid",
                        jmx_service,
                    ],
                    stdout=PIPE,
                    stderr=PIPE,
                    text=True,
                )
                if jmx_pid_res.returncode == 0 and jmx_pid_res.stdout.strip():
                    _jmx_pid = jmx_pid_res.stdout.strip()
                    if not _jmx_pid.isdigit() or _jmx_pid == "0":
                        LOGGER.warning(
                            "Unexpected PID value from supervisorctl for %s "
                            "in %s: %r",
                            jmx_service, self.name, _jmx_pid,
                        )
                    else:
                        run(
                            [
                                "podman",
                                "exec",
                                self.pid,
                                "bash",
                                "-c",
                                f"kill -9 {_jmx_pid}",
                            ],
                            stdout=PIPE,
                            stderr=PIPE,
                        )
                self.service_stop(jmx_service)

    def wait_until_stopped(self, wait_seconds=None, marks=None, dump_core=True):
        """Wait until the Scylla service inside the container is no longer running.

        Overrides the parent implementation because self.pid is a container ID
        string (not an OS pid), so os.kill() would crash.  The container itself
        stays alive — only the Scylla process inside supervisord is stopped.
        """
        marks = marks or []
        if wait_seconds is None:
            wait_seconds = 127

        if self.is_running():
            if not common.wait_for(
                func=lambda: not self.is_running(),
                timeout=wait_seconds,
                step=0.5,
            ):
                raise NodeError(f"Problem stopping node {self.name}")

        for node, mark in marks:
            if node != self:
                node.watch_log_for_death(self, from_mark=mark)

    def clear(self, *args, **kwargs):
        # Reclaim ownership of container-written files so the host user can
        # delete them.  775 is sufficient — data is about to be removed.
        _busybox_chmod(self.get_path(), "/node", "775", f"clear chmod for {self.name}")
        super(ScyllaPodmanNode, self).clear(*args, **kwargs)

    def remove(self):
        if self.log_thread:
            self.log_thread.stop()
            self.log_thread = None
        # Invalidate caches tied to the container — a new container may have
        # different supervisor programs or nodetool support.
        self._cached_supervisor_programs = None
        self._cached_nodetool_support = {}
        container_id = self.pid
        # Clear pid first so that any subsequent is_running()/service_status()
        # calls (e.g. from the parent stop() during teardown) take the early
        # return path in _update_podman_status instead of exec-ing into a removed
        # container.
        self.pid = None
        # Try to remove by container ID first, then by deterministic podman
        # name as a fallback.  Log and warn on failures — silent swallowing
        # of podman rm errors leads to leaked containers.
        targets = []
        if container_id:
            targets.append(str(container_id))
        if hasattr(self, "podman_name") and self.podman_name:
            targets.append(self.podman_name)
        removed = False
        for target in targets:
            res = run(
                ["podman", "rm", "--volumes", "-f", target],
                stdout=PIPE,
                stderr=PIPE,
                text=True,
            )
            if res.returncode == 0:
                removed = True
                break
            LOGGER.warning(
                "podman rm -f %s failed (rc=%d): %s",
                target,
                res.returncode,
                res.stderr.strip(),
            )
        if not removed and targets:
            LOGGER.error(
                "Failed to remove container for %s using targets %s",
                self.name,
                targets,
            )

    def _start_jmx(self, data):
        jmx_service = self._jmx_service_name()
        if not jmx_service:
            return
        jmx_status = self.service_status(jmx_service)
        if jmx_status and jmx_status.upper() != "RUNNING":
            self.service_start(jmx_service)

    def is_running(self):
        self._update_podman_status()
        return self.status == Status.UP or self.status == Status.DECOMMISSIONED

    def is_live(self):
        self._update_podman_status()
        return self.status == Status.UP

    def _update_podman_status(self):
        if self.pid is None:
            if self.status == Status.UP or self.status == Status.DECOMMISSIONED:
                self.status = Status.DOWN
                self._update_config()
            return

        scylla_status = self.service_status(self._scylla_service_name())
        if scylla_status and scylla_status.upper() == "RUNNING":
            new_status = Status.UP
        elif self.status == Status.DECOMMISSIONED:
            # Preserve DECOMMISSIONED — a decommissioned node whose scylla
            # process has stopped is still decommissioned, not merely DOWN.
            return
        else:
            new_status = Status.DOWN
        if new_status != self.status:
            self.status = new_status
            self._update_config()

    def _wait_java_up(self, ip_addr, jmx_port):
        return True

    def _update_pid(self, process):
        pass

    def get_tool(self, toolname):
        if self.pid is None:
            raise RuntimeError(f"Cannot run {toolname} on {self.name}: no running container")
        podman_bin = which("podman") or "podman"
        return [podman_bin, "exec", "-i", f"{self.pid}", f"{toolname}"]

    def _find_cmd(self, command_name):
        return self.get_tool(command_name)

    def get_sstables(self, *args, **kwargs):
        files = super(ScyllaPodmanNode, self).get_sstables(*args, **kwargs)
        prefix = self.get_path()
        return [
            "/usr/lib/scylla" + f[len(prefix):] if f.startswith(prefix) else f
            for f in files
        ]

    def get_env(self):
        return os.environ.copy()

    def copy_config_files(self):
        pass

    def import_config_files(self):
        self.update_yaml()

    def kill(self, __signal):
        if self.pid is None:
            return
        service_name = self._scylla_service_name()
        # Get the PID of the service first, then send the signal via bash
        # (kill is a shell builtin, not a binary in the Scylla container).
        pid_res = run(
            [
                "podman",
                "exec",
                self.pid,
                "supervisorctl",
                "pid",
                service_name,
            ],
            stdout=PIPE,
            stderr=PIPE,
            text=True,
        )
        if pid_res.returncode != 0 or not pid_res.stdout.strip():
            LOGGER.debug(
                "Failed to get pid of %s in %s: %s",
                service_name, self.name, pid_res.stderr,
            )
            return
        _pid = pid_res.stdout.strip()
        if not _pid.isdigit() or _pid == "0":
            LOGGER.warning(
                "Unexpected PID value from supervisorctl for %s "
                "in %s: %r",
                service_name, self.name, _pid,
            )
            return
        run(
            [
                "podman",
                "exec",
                self.pid,
                "bash",
                "-c",
                f"kill -{int(__signal)} {_pid}",
            ],
            stdout=PIPE,
            stderr=PIPE,
        )

    def pause(self):
        """Pause the Scylla process inside the container using SIGSTOP.

        Overrides the base Node.pause() because self.pid is a container ID
        string — not an OS-level integer PID — so os.kill() / psutil.Process()
        would crash with TypeError.
        """
        if self.pid is None:
            return
        service_name = self._scylla_service_name()
        pid_res = run(
            ["podman", "exec", self.pid, "supervisorctl", "pid", service_name],
            stdout=PIPE,
            stderr=PIPE,
            text=True,
        )
        if pid_res.returncode != 0 or not pid_res.stdout.strip():
            LOGGER.warning(
                "Cannot pause %s: failed to get Scylla PID from supervisorctl",
                self.name,
            )
            return
        _pid = pid_res.stdout.strip()
        if not _pid.isdigit() or _pid == "0":
            LOGGER.warning(
                "Cannot pause %s: unexpected PID value %r from supervisorctl",
                self.name,
                _pid,
            )
            return
        run(
            ["podman", "exec", self.pid, "bash", "-c", f"kill -STOP {_pid}"],
            stdout=PIPE,
            stderr=PIPE,
        )

    def resume(self):
        """Resume the Scylla process inside the container using SIGCONT.

        Overrides the base Node.resume() for the same reason as pause().
        """
        if self.pid is None:
            return
        service_name = self._scylla_service_name()
        pid_res = run(
            ["podman", "exec", self.pid, "supervisorctl", "pid", service_name],
            stdout=PIPE,
            stderr=PIPE,
            text=True,
        )
        if pid_res.returncode != 0 or not pid_res.stdout.strip():
            LOGGER.warning(
                "Cannot resume %s: failed to get Scylla PID from supervisorctl",
                self.name,
            )
            return
        _pid = pid_res.stdout.strip()
        if not _pid.isdigit() or _pid == "0":
            LOGGER.warning(
                "Cannot resume %s: unexpected PID value %r from supervisorctl",
                self.name,
                _pid,
            )
            return
        run(
            ["podman", "exec", self.pid, "bash", "-c", f"kill -CONT {_pid}"],
            stdout=PIPE,
            stderr=PIPE,
        )

    def unlink(self, file_path):
        if not os.path.exists(file_path):
            return
        # Mount the parent directory (not the file itself) because a bind-
        # mounted file IS the mount point — ``rm`` inside the container would
        # fail with EBUSY if we mounted the file directly.
        parent_dir = os.path.dirname(os.path.abspath(file_path))
        res = run(
            [
                "podman",
                "run",
                "--rm",
                "-v",
                f"{parent_dir}:{parent_dir}",
                BUSYBOX_IMAGE,
                "rm",
                os.path.abspath(file_path),
            ],
            stdout=DEVNULL,
            stderr=PIPE,
            text=True,
        )
        if res.returncode != 0:
            LOGGER.warning(
                "unlink %s via busybox failed (rc=%d): %s",
                file_path, res.returncode, res.stderr.strip(),
            )

    def chmod(self, file_path, permissions):
        prefix = self.get_path()
        if file_path.startswith(prefix):
            path_inside = self.base_data_path + file_path[len(prefix):]
        else:
            path_inside = file_path
        _busybox_chmod(file_path, path_inside, permissions, f"chmod {permissions} for {self.name}")

    def rmtree(self, path):
        _busybox_chmod(self.get_path(), "/node", "777", f"rmtree chmod for {self.name}")
        super(ScyllaPodmanNode, self).rmtree(path)


class PodmanLogger:
    """Streams podman container logs to a local file.

    Uses subprocess.Popen so the process can be tracked and stopped cleanly.
    """

    def __init__(self, node, target_log_file: str):
        self._node = node
        self._target_log_file = target_log_file
        self._process = None
        self._log_file = None
        self._reader_thread = None
        self._stop_event = threading.Event()

    def start(self):
        """Start streaming container logs to the target file.

        Uses a background thread that reads from ``podman logs -f`` and writes
        each line to the log file with an explicit flush.  This ensures that
        ``watch_log_for`` sees new lines promptly, regardless of OS-level
        write buffering on the pipe-to-file path.
        """
        self.stop()  # Clean up any previous process
        self._stop_event.clear()
        self._log_file = open(self._target_log_file, "a", encoding="utf-8")
        try:
            self._process = Popen(
                ["podman", "logs", "-f", self._node.pid],
                stdout=PIPE,
                stderr=STDOUT,
            )
            self._reader_thread = threading.Thread(
                target=self._reader_loop, daemon=True
            )
            self._reader_thread.start()
        except Exception:
            if self._process is not None:
                try:
                    self._process.kill()
                    self._process.wait(timeout=5)
                except Exception:
                    pass
                self._process = None
            self._log_file.close()
            self._log_file = None
            raise

    def _reader_loop(self):
        """Read lines from the podman logs process and write them with flush."""
        log_file = self._log_file
        if log_file is None:
            return
        try:
            for line in self._process.stdout:
                if self._stop_event.is_set():
                    break
                try:
                    log_file.write(line.decode("utf-8", errors="replace"))
                    log_file.flush()
                except Exception:
                    LOGGER.debug(
                        "Podman log writer stopped for %s",
                        self._node.name,
                        exc_info=True,
                    )
                    break
        except Exception:
            LOGGER.debug(
                "Podman log reader stopped for %s", self._node.name, exc_info=True
            )

    def stop(self):
        """Stop the log streaming process, join the reader thread, and close the file handle."""
        self._stop_event.set()
        if self._process is not None:
            try:
                self._process.terminate()
                self._process.wait(timeout=5)
            except Exception:
                try:
                    self._process.kill()
                    self._process.wait(timeout=5)
                except Exception:
                    pass
            self._process = None
        # Join the reader thread to ensure it has finished writing before we
        # close the file handle.  This prevents a race where start() reopens
        # the file while the old thread is still flushing.
        if self._reader_thread is not None:
            self._reader_thread.join(timeout=5)
            self._reader_thread = None
        if self._log_file is not None:
            try:
                self._log_file.close()
            except Exception:
                pass
            self._log_file = None
