"""
Container client abstraction layer for Docker and Podman support.

This module provides a unified interface for container operations,
supporting both Docker and Podman as container runtimes.
"""

import json
import logging
import os
import shutil
import threading
from abc import ABC, abstractmethod
from subprocess import run, Popen, PIPE, STDOUT, CalledProcessError, TimeoutExpired
from typing import Dict, List, Optional, Tuple

LOGGER = logging.getLogger("ccm")


class ContainerClientError(Exception):
    """Base exception for container client errors."""
    pass


class ContainerRuntimeNotFoundError(ContainerClientError):
    """Raised when the container runtime is not installed or not accessible."""
    pass


class ContainerImageNotFoundError(ContainerClientError):
    """Raised when a container image is not found and cannot be pulled."""
    pass


class ContainerStartError(ContainerClientError):
    """Raised when a container fails to start."""
    pass


class ContainerExecError(ContainerClientError):
    """Raised when executing a command in a container fails."""
    pass


class ContainerClient(ABC):
    """Abstract base class for container runtime clients."""
    
    def __init__(self):
        """Initialize the container client."""
        self.runtime_name = self._get_runtime_name()
        if not self._is_available():
            raise ContainerRuntimeNotFoundError(
                f"{self.runtime_name} is not installed or not accessible. "
                f"Please install {self.runtime_name} and ensure it's in your PATH."
            )
        # Resolved binary path for callers that exec/spawn it directly
        # (e.g. get_tool(), ContainerLogManager) instead of via _run_command().
        self.runtime_path = shutil.which(self.runtime_name) or self.runtime_name
        LOGGER.debug(f"Initialized {self.runtime_name} client")
    
    @abstractmethod
    def _get_runtime_name(self) -> str:
        """Return the name of the container runtime (e.g., 'docker', 'podman')."""
        pass
    
    def _is_available(self) -> bool:
        """Check if the container runtime is available."""
        if not shutil.which(self.runtime_name):
            return False
        try:
            result = run([self.runtime_name, 'version'], 
                        stdout=PIPE, stderr=PIPE, timeout=5)
            return result.returncode == 0
        except (FileNotFoundError, CalledProcessError, TimeoutExpired):
            return False
    
    def _run_command(
        self, cmd: List[str], check: bool = True, timeout: Optional[float] = None
    ) -> Tuple[int, str, str]:
        """
        Run a container runtime command.
        
        Args:
            cmd: Command and arguments to run
            check: If True, raise exception on non-zero return code
            timeout: Optional timeout in seconds. On expiry, returns
                     (-1, '', 'timed out') instead of raising.
            
        Returns:
            Tuple of (returncode, stdout, stderr)
        """
        LOGGER.debug(f"Running command: {' '.join(cmd)}")
        try:
            result = run(cmd, stdout=PIPE, stderr=PIPE, universal_newlines=True, timeout=timeout)
            LOGGER.debug(f"Command exit code: {result.returncode}")
            if result.stdout:
                LOGGER.debug(f"Command stdout: {result.stdout[:500]}")
            if result.stderr:
                LOGGER.debug(f"Command stderr: {result.stderr[:500]}")
            
            if check and result.returncode != 0:
                raise CalledProcessError(result.returncode, cmd, result.stdout, result.stderr)
            
            return result.returncode, result.stdout, result.stderr
        except FileNotFoundError:
            raise ContainerRuntimeNotFoundError(
                f"{self.runtime_name} command not found. Is {self.runtime_name} installed?"
            )
        except TimeoutExpired:
            LOGGER.debug(f"Command timed out after {timeout}s: {' '.join(cmd)}")
            return -1, '', f'command timed out after {timeout}s'
    
    def run_container(
        self,
        image: str,
        name: str,
        volumes: Optional[Dict[str, str]] = None,
        volumes_no_relabel: Optional[List[str]] = None,
        ports: Optional[Dict[str, str]] = None,
        env: Optional[Dict[str, str]] = None,
        network: Optional[str] = None,
        command: Optional[List[str]] = None,
        entrypoint: Optional[str] = None,
        user: Optional[str] = None,
        cap_add: Optional[List[str]] = None,
        security_opt: Optional[List[str]] = None,
        tmpfs: Optional[List[str]] = None,
        detach: bool = True,
        remove: bool = False,
        ip: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None,
        cpuset_cpus: Optional[str] = None,
        hostname: Optional[str] = None,
    ) -> str:
        """
        Run a container.

        Args:
            image: Container image to run
            name: Name for the container
            volumes: Dictionary of host_path: container_path volume mounts
            volumes_no_relabel: Host paths (subset of `volumes`' keys) to
                mount without podman's `:z` SELinux relabel suffix (e.g.
                `/tmp`, which shouldn't be relabeled).
            ports: Dictionary of host_port: container_port port mappings
            env: Dictionary of environment variables
            network: Network to connect to
            command: Command to run in container
            entrypoint: Override container entrypoint
            user: User to run as
            cap_add: List of Linux capabilities to add
            detach: Run in detached mode
            remove: Remove container when it exits
            ip: Static IP address to assign on `network` (requires `network`)
            labels: Dictionary of label_key: label_value container labels
            cpuset_cpus: CPU set to pin the container to (e.g. "0,1" or "0-3")
            hostname: Container hostname

        Returns:
            Container ID
        """
        cmd = [self.runtime_name, 'run']

        if detach:
            cmd.append('-d')

        if remove:
            cmd.append('--rm')

        if cap_add:
            for cap in cap_add:
                cmd.extend(['--cap-add', cap])

        if security_opt:
            for opt in security_opt:
                cmd.extend(['--security-opt', opt])

        if tmpfs:
            for mount in tmpfs:
                cmd.extend(['--tmpfs', mount])

        if labels:
            for key, value in labels.items():
                cmd.extend(['--label', f'{key}={value}'])

        if cpuset_cpus:
            cmd.extend(['--cpuset-cpus', cpuset_cpus])

        if hostname:
            cmd.extend(['--hostname', hostname])

        cmd.extend(['--name', name])

        if volumes:
            # Podman relabels (:z) by default, except paths in volumes_no_relabel.
            no_relabel = set(volumes_no_relabel or [])
            for host_path, container_path in volumes.items():
                relabel = self.runtime_name == 'podman' and host_path not in no_relabel
                vol_suffix = ':z' if relabel else ''
                cmd.extend(['-v', f'{host_path}:{container_path}{vol_suffix}'])

        if ports:
            for host_port, container_port in ports.items():
                cmd.extend(['-p', f'{host_port}:{container_port}'])

        if env:
            for key, value in env.items():
                cmd.extend(['-e', f'{key}={value}'])

        if network:
            cmd.extend(['--network', network])

        if ip:
            cmd.extend(['--ip', ip])

        if entrypoint:
            cmd.extend(['--entrypoint', entrypoint])

        if user:
            cmd.extend(['--user', user])

        cmd.append(image)

        if command:
            cmd.extend(command)
        
        returncode, stdout, stderr = self._run_command(cmd, check=False)

        if returncode != 0:
            raise ContainerStartError(
                f"Failed to start container '{name}': {stderr}"
            )
        
        container_id = stdout.strip()
        LOGGER.info(f"Started container '{name}' with ID: {container_id[:12]}")
        return container_id
    
    def exec_command(
        self,
        container_id: str,
        command: List[str],
        interactive: bool = False,
        tty: bool = False,
        user: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> Tuple[int, str, str]:
        """
        Execute a command in a running container.
        
        Args:
            container_id: Container ID or name
            command: Command and arguments to execute
            interactive: Keep STDIN open
            tty: Allocate a pseudo-TTY
            user: User to run command as
            timeout: Optional timeout in seconds for the exec call itself
            
        Returns:
            Tuple of (returncode, stdout, stderr)
        """
        cmd = [self.runtime_name, 'exec']
        
        if interactive:
            cmd.append('-i')
        
        if tty:
            cmd.append('-t')
        
        if user:
            cmd.extend(['-u', user])
        
        cmd.append(container_id)
        cmd.extend(command)
        
        return self._run_command(cmd, check=False, timeout=timeout)
    
    def stop_container(self, container_id: str, timeout: int = 10) -> None:
        """
        Stop a running container.
        
        Args:
            container_id: Container ID or name
            timeout: Seconds to wait before killing
        """
        cmd = [self.runtime_name, 'stop', '-t', str(timeout), container_id]
        returncode, stdout, stderr = self._run_command(cmd, check=False)
        
        if returncode != 0:
            LOGGER.warning(f"Failed to stop container '{container_id}': {stderr}")
        else:
            LOGGER.info(f"Stopped container: {container_id[:12]}")
    
    def remove_container(
        self,
        container_id: str,
        force: bool = False,
        volumes: bool = False,
        check: bool = False,
    ) -> None:
        """
        Remove a container.
        
        Args:
            container_id: Container ID or name
            force: Force removal (kill if running)
            volumes: Remove associated volumes
            check: If True, raise `ContainerExecError` on failure instead of
                   just logging a warning (for safety-critical callers).
        """
        cmd = [self.runtime_name, 'rm'] + self._force_remove_flags(force) + (['-v'] if volumes else []) + [container_id]

        returncode, stdout, stderr = self._run_command(cmd, check=False)
        
        if returncode != 0:
            if check:
                raise ContainerExecError(
                    f"Failed to remove container '{container_id}': {stderr}"
                )
            LOGGER.warning(f"Failed to remove container '{container_id}': {stderr}")
        else:
            LOGGER.info(f"Removed container: {container_id[:12]}")

    def _force_remove_flags(self, force: bool) -> List[str]:
        """Flags for an instant force-remove (kill, no grace period).

        Docker's `rm -f` is already an immediate SIGKILL. Podman's `rm -f`
        still honors the default 10s stop-grace-period before SIGKILL unless
        `--time 0` is given, which podman-specifically supports on `rm`.
        """
        if not force:
            return []
        if self.runtime_name == 'podman':
            return ['-f', '--time', '0']
        return ['-f']

    def remove_containers(
        self,
        container_ids: List[str],
        force: bool = False,
        volumes: bool = False,
        chunk_size: int = 10,
    ) -> List[str]:
        """
        Remove multiple containers, batching several IDs into each `rm`
        invocation instead of spawning one subprocess per container.

        Args:
            container_ids: Container IDs/names to remove
            force: Force removal (kill if running)
            volumes: Remove associated volumes
            chunk_size: Max container IDs per `rm` invocation

        Returns:
            The subset of container_ids whose chunk reported a failure, so
            callers can retry/fallback on just those (e.g. by name).
        """
        failed: List[str] = []
        base_cmd = [self.runtime_name, 'rm'] + self._force_remove_flags(force) + (['-v'] if volumes else [])

        for i in range(0, len(container_ids), chunk_size):
            chunk = container_ids[i:i + chunk_size]
            returncode, stdout, stderr = self._run_command(base_cmd + chunk, check=False)
            if returncode != 0:
                # A batch failure could be one bad container among several
                # good ones; punt the whole chunk back to the caller rather
                # than guessing which ones actually succeeded.
                failed.extend(chunk)
                LOGGER.warning(f"Failed to remove some containers in batch {chunk}: {stderr}")
            else:
                LOGGER.info(f"Removed {len(chunk)} containers: {', '.join(c[:12] for c in chunk)}")

        return failed
    
    def inspect_container(self, container_id: str) -> Optional[Dict]:
        """
        Inspect a container and return its metadata.
        
        Args:
            container_id: Container ID or name
            
        Returns:
            Container metadata as dictionary, or None if not found
        """
        cmd = [self.runtime_name, 'inspect', container_id]
        returncode, stdout, stderr = self._run_command(cmd, check=False)
        
        if returncode != 0 or not stdout.strip():
            return None
        
        try:
            data = json.loads(stdout)
            return data[0] if data else None
        except (json.JSONDecodeError, IndexError):
            LOGGER.error(f"Failed to parse inspect output for '{container_id}'")
            return None
    
    def get_container_ip(self, container_id: str, network: Optional[str] = None) -> Optional[str]:
        """
        Get the IP address of a container.

        Args:
            container_id: Container ID or name
            network: Network name to get IP from. When containers are on a
                     custom network, the top-level IPAddress is empty.

        Returns:
            IP address as string, or None if not found
        """
        if network:
            fmt = '{{(index .NetworkSettings.Networks "' + network + '").IPAddress}}'
        else:
            fmt = '{{.NetworkSettings.IPAddress}}'
        cmd = [
            self.runtime_name, 'inspect',
            '--format', fmt,
            container_id
        ]
        returncode, stdout, stderr = self._run_command(cmd, check=False)

        if returncode != 0:
            LOGGER.error(f"Failed to get IP for container '{container_id}': {stderr}")
            return None

        ip = stdout.strip()
        return ip if ip else None

    def get_container_pid(self, container_id: str) -> Optional[int]:
        """
        Get the host-visible PID of a container's init process, for use
        with ``nsenter`` to enter its namespaces from the host.

        Args:
            container_id: Container ID or name

        Returns:
            The host PID as an int, or None if the container isn't running
            or the PID couldn't be determined.
        """
        cmd = [self.runtime_name, 'inspect', '--format', '{{.State.Pid}}', container_id]
        returncode, stdout, stderr = self._run_command(cmd, check=False)
        if returncode != 0:
            return None
        try:
            pid = int(stdout.strip())
        except ValueError:
            LOGGER.error(
                f"Unexpected PID value in inspect output for '{container_id}': {stdout!r}"
            )
            return None
        return pid if pid > 0 else None

    def get_container_status(self, container_id: str) -> Optional[str]:
        """
        Get the lifecycle status of a container (e.g. 'running', 'exited').

        Args:
            container_id: Container ID or name

        Returns:
            The status string, or None if the container was not found.
        """
        cmd = [self.runtime_name, 'inspect', '--format', '{{.State.Status}}', container_id]
        returncode, stdout, stderr = self._run_command(cmd, check=False)
        if returncode != 0:
            return None
        status = stdout.strip()
        return status or None

    def get_container_labels(self, container_id: str) -> Dict[str, str]:
        """
        Get the labels attached to a container.

        Args:
            container_id: Container ID or name

        Returns:
            Dictionary of labels (empty if the container was not found or has
            no labels).
        """
        info = self.inspect_container(container_id)
        if info is None:
            return {}
        labels = info.get("Config", {}).get("Labels", {})
        return labels if isinstance(labels, dict) else {}

    def copy_from_container(self, container_id: str, container_path: str) -> bytes:
        """
        Copy a file or directory out of a container as a tar archive stream.

        Args:
            container_id: Container ID or name
            container_path: Path inside the container to copy (e.g. '/etc/scylla/')

        Returns:
            Raw tar archive bytes (as produced by ``<runtime> container cp -a``).

        Raises:
            ContainerExecError: if the copy failed.
        """
        cmd = [self.runtime_name, 'container', 'cp', '-a', f'{container_id}:{container_path}', '-']
        LOGGER.debug(f"Running command: {' '.join(cmd)}")
        try:
            result = run(cmd, stdout=PIPE, stderr=PIPE)
        except FileNotFoundError:
            raise ContainerRuntimeNotFoundError(
                f"{self.runtime_name} command not found. Is {self.runtime_name} installed?"
            )
        if result.returncode != 0:
            stderr_text = result.stderr.decode('utf-8', errors='replace') if isinstance(result.stderr, bytes) else result.stderr
            raise ContainerExecError(
                f"Failed to copy '{container_path}' from container '{container_id}': {stderr_text}"
            )
        return result.stdout
    
    def container_exists(self, container_name: str) -> bool:
        """
        Check if a container exists.
        
        Args:
            container_name: Container name
            
        Returns:
            True if container exists, False otherwise
        """
        cmd = [self.runtime_name, 'ps', '-a', '--filter', f'name=^{container_name}$', '--format', '{{.Names}}']
        returncode, stdout, stderr = self._run_command(cmd, check=False)
        
        return returncode == 0 and container_name in stdout
    
    def image_exists(self, image: str) -> bool:
        """
        Check if an image exists locally.
        
        Args:
            image: Image name/tag
            
        Returns:
            True if image exists, False otherwise
        """
        cmd = [self.runtime_name, 'image', 'inspect', image]
        returncode, stdout, stderr = self._run_command(cmd, check=False)
        
        return returncode == 0
    
    def pull_image(self, image: str) -> bool:
        """
        Pull a container image.
        
        Args:
            image: Image name/tag to pull
            
        Returns:
            True if successful, False otherwise
        """
        LOGGER.info(f"Pulling image: {image}")
        cmd = [self.runtime_name, 'pull', image]
        returncode, stdout, stderr = self._run_command(cmd, check=False)
        
        if returncode != 0:
            LOGGER.error(f"Failed to pull image '{image}': {stderr}")
            return False
        
        LOGGER.info(f"Successfully pulled image: {image}")
        return True
    
    def stream_logs(
        self,
        container_id: str,
        follow: bool = False,
        tail: Optional[int] = None,
    ) -> str:
        """
        Get logs from a container.
        
        Args:
            container_id: Container ID or name
            follow: Follow log output
            tail: Number of lines to show from end
            
        Returns:
            Log output as string
        """
        cmd = [self.runtime_name, 'logs']
        
        if follow:
            cmd.append('-f')
        
        if tail is not None:
            cmd.extend(['--tail', str(tail)])
        
        cmd.append(container_id)
        
        returncode, stdout, stderr = self._run_command(cmd, check=False)
        
        # Combine stdout and stderr as container logs can be in either
        return stdout + stderr
    
    def create_network(
        self,
        network_name: str,
        subnet: Optional[str] = None,
        gateway: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        Create a container network.
        
        Args:
            network_name: Name for the network
            subnet: CIDR subnet for the network (e.g. '10.89.1.0/24')
            gateway: Gateway IP for the network (requires `subnet`)
            labels: Dictionary of label_key: label_value network labels
            
        Returns:
            True if successful (or the network already exists)

        Raises:
            ContainerStartError: if `subnet`/`gateway`/`labels` was given and
                creation failed for a reason other than already existing.
                Plain `create_network(name)` keeps the legacy bool-return
                behaviour for existing callers.
        """
        cmd = [self.runtime_name, 'network', 'create']
        if labels:
            for key, value in labels.items():
                cmd.extend(['--label', f'{key}={value}'])
        if subnet:
            cmd.extend(['--subnet', subnet])
        if gateway:
            cmd.extend(['--gateway', gateway])
        cmd.append(network_name)

        returncode, stdout, stderr = self._run_command(cmd, check=False)
        
        if returncode != 0:
            # Network might already exist
            if 'already exists' in stderr.lower():
                LOGGER.debug(f"Network '{network_name}' already exists")
                return True
            if subnet or gateway or labels:
                raise ContainerStartError(
                    f"Failed to create network '{network_name}': {stderr}"
                )
            LOGGER.error(f"Failed to create network '{network_name}': {stderr}")
            return False
        
        LOGGER.info(f"Created network: {network_name}")
        return True
    
    def remove_network(self, network_name: str, force: bool = False, check: bool = False) -> None:
        """
        Remove a container network.
        
        Args:
            network_name: Name of the network to remove
            force: Force removal (disconnect any attached containers first)
            check: If True, raise `ContainerExecError` on failure instead of
                   just logging a warning.
        """
        cmd = [self.runtime_name, 'network', 'rm']
        if force:
            cmd.append('-f')
        cmd.append(network_name)
        returncode, stdout, stderr = self._run_command(cmd, check=False)
        
        if returncode != 0:
            if check:
                raise ContainerExecError(
                    f"Failed to remove network '{network_name}': {stderr}"
                )
            LOGGER.warning(f"Failed to remove network '{network_name}': {stderr}")
        else:
            LOGGER.info(f"Removed network: {network_name}")

    def inspect_network(self, network_name: str) -> Optional[Dict]:
        """
        Inspect a network and return its metadata.

        Args:
            network_name: Network name or ID

        Returns:
            Network metadata as dictionary, or None if not found
        """
        cmd = [self.runtime_name, 'network', 'inspect', network_name]
        returncode, stdout, stderr = self._run_command(cmd, check=False)

        if returncode != 0 or not stdout.strip():
            return None

        try:
            data = json.loads(stdout)
        except json.JSONDecodeError:
            LOGGER.warning(f"Failed to parse network inspect output for '{network_name}'")
            return None
        if isinstance(data, list):
            return data[0] if data else None
        return data

    def list_networks(self) -> List[Dict]:
        """
        List all networks known to the container runtime.

        Returns:
            List of network metadata dictionaries (empty list on error).
        """
        cmd = [self.runtime_name, 'network', 'ls', '--format', 'json']
        returncode, stdout, stderr = self._run_command(cmd, check=False)

        if returncode != 0:
            LOGGER.warning(f"Failed to list networks: {stderr}")
            return []

        try:
            networks = json.loads(stdout)
        except json.JSONDecodeError:
            LOGGER.warning("Failed to parse network ls output as JSON")
            return []
        return networks if isinstance(networks, list) else []


class DockerClient(ContainerClient):
    """Docker implementation of the container client."""
    
    def _get_runtime_name(self) -> str:
        return 'docker'


class PodmanClient(ContainerClient):
    """Podman implementation of the container client."""

    def _get_runtime_name(self) -> str:
        return 'podman'

    def unshare(self, command: List[str], check: bool = False) -> Tuple[int, str, str]:
        """
        Run a command via ``podman unshare`` (rootless user namespace).
        Podman-specific escape hatch for host-side chown/chmod on
        bind-mounts owned by the container's user namespace.

        Args:
            command: Command and arguments to run inside the user namespace
            check: If True, raise `ContainerExecError` on non-zero return code

        Returns:
            Tuple of (returncode, stdout, stderr)
        """
        cmd = [self.runtime_name, 'unshare'] + list(command)
        returncode, stdout, stderr = self._run_command(cmd, check=False)
        if check and returncode != 0:
            raise ContainerExecError(
                f"'podman unshare {' '.join(command)}' failed: {stderr}"
            )
        return returncode, stdout, stderr


def get_container_client(runtime: Optional[str] = None) -> ContainerClient:
    """
    Get a container client instance.
    
    Args:
        runtime: Container runtime to use ('docker' or 'podman').
                If None, checks CCM_CONTAINER_RUNTIME env var,
                then tries docker, then podman.
    
    Returns:
        ContainerClient instance
        
    Raises:
        ContainerRuntimeNotFoundError: If no container runtime is available
    """
    if runtime is None:
        runtime = os.environ.get('CCM_CONTAINER_RUNTIME', '').lower()
    
    if runtime == 'podman':
        return PodmanClient()
    elif runtime == 'docker':
        return DockerClient()
    else:
        # Auto-detect: try docker first, then podman
        try:
            return DockerClient()
        except ContainerRuntimeNotFoundError:
            try:
                return PodmanClient()
            except ContainerRuntimeNotFoundError:
                raise ContainerRuntimeNotFoundError(
                    "No container runtime found. Please install Docker or Podman."
                )


class _LogStreamState:
    """Internal state for a single container's log stream."""
    __slots__ = ("log_file_path", "stop_event", "thread", "process", "lock")

    def __init__(self, log_file_path: str):
        self.log_file_path = log_file_path
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        # Guards `process`, set by the background thread once Popen()
        # succeeds; read by stop_stream()/stop_all() to terminate it early.
        self.process: Optional[Popen] = None
        self.lock = threading.Lock()


class ContainerLogManager:
    """Streams `<runtime> logs -f <id>` to per-container log files.

    One daemon thread per container. Shared by the Docker and Podman
    cluster implementations to avoid duplicating log-capture logic.
    """

    def __init__(self, client: ContainerClient):
        self._client = client
        self._runtime_path = client.runtime_path
        self._streams: Dict[str, _LogStreamState] = {}
        self._lock = threading.Lock()

    def start_stream(self, container_id: str, log_file_path: str) -> None:
        """Begin streaming logs for *container_id* to *log_file_path*.

        A no-op if a stream for this container is already active.
        """
        with self._lock:
            if container_id in self._streams:
                return
            state = _LogStreamState(log_file_path)
            self._streams[container_id] = state
        thread = threading.Thread(
            target=self._stream_logs,
            args=(container_id, state),
            name=f"{self._client.runtime_name}-log-{container_id[:12]}",
            daemon=True,
        )
        thread.start()
        state.thread = thread

    def stop_stream(self, container_id: str) -> None:
        """Stop the log stream for *container_id*, if one is active."""
        with self._lock:
            state = self._streams.pop(container_id, None)
        if state is not None:
            self._terminate_state(state)

    def stop_all(self) -> None:
        """Stop all managed log streams."""
        with self._lock:
            states = list(self._streams.values())
            self._streams.clear()
        for state in states:
            self._terminate_state(state)

    @staticmethod
    def _terminate_state(state: "_LogStreamState") -> None:
        """Stop a stream and kill its process now (not on the next log line).

        The streaming thread only checks stop_event between reads on a
        blocking pipe, so a quiet container would otherwise linger.
        """
        state.stop_event.set()
        with state.lock:
            process = state.process
        if process is not None:
            try:
                process.terminate()
            except ProcessLookupError:
                pass

    def _stream_logs(self, container_id: str, state: _LogStreamState) -> None:
        """Background: run ``<runtime> logs -f`` and append output to the log file."""
        try:
            with open(state.log_file_path, 'a') as f:
                process = Popen(
                    [self._runtime_path, 'logs', '-f', container_id],
                    stdout=PIPE,
                    stderr=STDOUT,
                    universal_newlines=True,
                    bufsize=1,
                )
                with state.lock:
                    already_stopped = state.stop_event.is_set()
                    if not already_stopped:
                        state.process = process
                if already_stopped:
                    # Raced with stop_stream()/stop_all(); kill it now.
                    try:
                        process.terminate()
                    except ProcessLookupError:
                        pass
                    try:
                        process.wait(timeout=5)
                    except Exception:
                        pass
                    return
                try:
                    for line in process.stdout:
                        if state.stop_event.is_set():
                            break
                        f.write(line)
                        f.flush()
                except (ValueError, OSError):
                    pass
                finally:
                    try:
                        process.terminate()
                    except ProcessLookupError:
                        pass
                    try:
                        process.wait(timeout=5)
                    except Exception:
                        pass
        except Exception:
            LOGGER.warning(
                "Log stream failed for container %s", container_id, exc_info=True,
            )
        finally:
            # Drop the state if the thread exits on its own (e.g. Popen
            # failure) so start_stream() can be retried for this container.
            with self._lock:
                self._streams.pop(container_id, None)
