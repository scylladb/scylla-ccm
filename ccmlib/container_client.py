"""
Container client abstraction layer for Docker and Podman support.

This module provides a unified interface for container operations,
supporting both Docker and Podman as container runtimes.
"""

import logging
import os
import shutil
from abc import ABC, abstractmethod
from subprocess import run, PIPE, CalledProcessError
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
        except (FileNotFoundError, CalledProcessError):
            return False
    
    def _run_command(self, cmd: List[str], check: bool = True) -> Tuple[int, str, str]:
        """
        Run a container runtime command.
        
        Args:
            cmd: Command and arguments to run
            check: If True, raise exception on non-zero return code
            
        Returns:
            Tuple of (returncode, stdout, stderr)
        """
        LOGGER.debug(f"Running command: {' '.join(cmd)}")
        try:
            result = run(cmd, stdout=PIPE, stderr=PIPE, universal_newlines=True)
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
    
    def run_container(
        self,
        image: str,
        name: str,
        volumes: Optional[Dict[str, str]] = None,
        ports: Optional[Dict[str, str]] = None,
        env: Optional[Dict[str, str]] = None,
        network: Optional[str] = None,
        command: Optional[List[str]] = None,
        detach: bool = True,
        remove: bool = False,
    ) -> str:
        """
        Run a container.
        
        Args:
            image: Container image to run
            name: Name for the container
            volumes: Dictionary of host_path: container_path volume mounts
            ports: Dictionary of host_port: container_port port mappings
            env: Dictionary of environment variables
            network: Network to connect to
            command: Command to run in container
            detach: Run in detached mode
            remove: Remove container when it exits
            
        Returns:
            Container ID
        """
        cmd = [self.runtime_name, 'run']
        
        if detach:
            cmd.append('-d')
        
        if remove:
            cmd.append('--rm')
        
        cmd.extend(['--name', name])
        
        if volumes:
            for host_path, container_path in volumes.items():
                cmd.extend(['-v', f'{host_path}:{container_path}'])
        
        if ports:
            for host_port, container_port in ports.items():
                cmd.extend(['-p', f'{host_port}:{container_port}'])
        
        if env:
            for key, value in env.items():
                cmd.extend(['-e', f'{key}={value}'])
        
        if network:
            cmd.extend(['--network', network])
        
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
    ) -> Tuple[int, str, str]:
        """
        Execute a command in a running container.
        
        Args:
            container_id: Container ID or name
            command: Command and arguments to execute
            interactive: Keep STDIN open
            tty: Allocate a pseudo-TTY
            user: User to run command as
            
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
        
        return self._run_command(cmd, check=False)
    
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
    ) -> None:
        """
        Remove a container.
        
        Args:
            container_id: Container ID or name
            force: Force removal (kill if running)
            volumes: Remove associated volumes
        """
        cmd = [self.runtime_name, 'rm']
        
        if force:
            cmd.append('-f')
        
        if volumes:
            cmd.append('-v')
        
        cmd.append(container_id)
        
        returncode, stdout, stderr = self._run_command(cmd, check=False)
        
        if returncode != 0:
            LOGGER.warning(f"Failed to remove container '{container_id}': {stderr}")
        else:
            LOGGER.info(f"Removed container: {container_id[:12]}")
    
    def inspect_container(self, container_id: str) -> Optional[Dict]:
        """
        Inspect a container and return its metadata.
        
        Args:
            container_id: Container ID or name
            
        Returns:
            Container metadata as dictionary, or None if not found
        """
        import json
        
        cmd = [self.runtime_name, 'inspect', container_id]
        returncode, stdout, stderr = self._run_command(cmd, check=False)
        
        if returncode != 0:
            return None
        
        try:
            data = json.loads(stdout)
            return data[0] if data else None
        except (json.JSONDecodeError, IndexError):
            LOGGER.error(f"Failed to parse inspect output for '{container_id}'")
            return None
    
    def get_container_ip(self, container_id: str) -> Optional[str]:
        """
        Get the IP address of a container.
        
        Args:
            container_id: Container ID or name
            
        Returns:
            IP address as string, or None if not found
        """
        cmd = [
            self.runtime_name, 'inspect',
            '--format', '{{.NetworkSettings.IPAddress}}',
            container_id
        ]
        returncode, stdout, stderr = self._run_command(cmd, check=False)
        
        if returncode != 0:
            LOGGER.error(f"Failed to get IP for container '{container_id}': {stderr}")
            return None
        
        ip = stdout.strip()
        return ip if ip else None
    
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
        
        if tail:
            cmd.extend(['--tail', str(tail)])
        
        cmd.append(container_id)
        
        returncode, stdout, stderr = self._run_command(cmd, check=False)
        
        # Combine stdout and stderr as container logs can be in either
        return stdout + stderr
    
    def create_network(self, network_name: str) -> bool:
        """
        Create a container network.
        
        Args:
            network_name: Name for the network
            
        Returns:
            True if successful, False otherwise
        """
        cmd = [self.runtime_name, 'network', 'create', network_name]
        returncode, stdout, stderr = self._run_command(cmd, check=False)
        
        if returncode != 0:
            # Network might already exist
            if 'already exists' in stderr.lower():
                LOGGER.debug(f"Network '{network_name}' already exists")
                return True
            LOGGER.error(f"Failed to create network '{network_name}': {stderr}")
            return False
        
        LOGGER.info(f"Created network: {network_name}")
        return True
    
    def remove_network(self, network_name: str) -> None:
        """
        Remove a container network.
        
        Args:
            network_name: Name of the network to remove
        """
        cmd = [self.runtime_name, 'network', 'rm', network_name]
        returncode, stdout, stderr = self._run_command(cmd, check=False)
        
        if returncode != 0:
            LOGGER.warning(f"Failed to remove network '{network_name}': {stderr}")
        else:
            LOGGER.info(f"Removed network: {network_name}")


class DockerClient(ContainerClient):
    """Docker implementation of the container client."""
    
    def _get_runtime_name(self) -> str:
        return 'docker'


class PodmanClient(ContainerClient):
    """Podman implementation of the container client."""
    
    def _get_runtime_name(self) -> str:
        return 'podman'


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
