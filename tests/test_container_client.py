"""
Unit tests for the container client abstraction layer.

These tests mock the container runtime to avoid requiring Docker/Podman installation.
"""

import os
import pytest
from unittest.mock import patch, MagicMock, call
from subprocess import CalledProcessError

from ccmlib.container_client import (
    ContainerClient,
    DockerClient,
    PodmanClient,
    get_container_client,
    ContainerRuntimeNotFoundError,
    ContainerStartError,
    ContainerExecError,
)


@pytest.fixture
def mock_run():
    """Mock subprocess.run for testing."""
    with patch('ccmlib.container_client.run') as mock:
        # Default successful response
        mock.return_value = MagicMock(
            returncode=0,
            stdout='',
            stderr=''
        )
        yield mock


@pytest.fixture
def mock_which():
    """Mock shutil.which for testing."""
    with patch('ccmlib.container_client.shutil.which') as mock:
        mock.return_value = '/usr/bin/docker'
        yield mock


class TestDockerClient:
    """Test Docker client implementation."""
    
    def test_init_success(self, mock_run, mock_which):
        """Test successful Docker client initialization."""
        client = DockerClient()
        assert client.runtime_name == 'docker'
        mock_which.assert_called_with('docker')
        mock_run.assert_called_once()
    
    def test_init_not_installed(self, mock_which):
        """Test initialization fails when Docker is not installed."""
        mock_which.return_value = None
        
        with pytest.raises(ContainerRuntimeNotFoundError) as exc_info:
            DockerClient()
        
        assert 'docker is not installed' in str(exc_info.value).lower()
    
    def test_run_container_basic(self, mock_run, mock_which):
        """Test running a basic container."""
        mock_run.return_value.stdout = 'abc123container456'
        
        client = DockerClient()
        mock_run.reset_mock()
        
        container_id = client.run_container(
            image='scylladb/scylla:latest',
            name='test-node',
        )
        
        assert container_id == 'abc123container456'
        
        # Verify docker run was called
        args = mock_run.call_args[0][0]
        assert args[0] == 'docker'
        assert args[1] == 'run'
        assert '-d' in args
        assert '--name' in args
        assert 'test-node' in args
        assert 'scylladb/scylla:latest' in args
    
    def test_run_container_with_volumes(self, mock_run, mock_which):
        """Test running container with volume mounts."""
        mock_run.return_value.stdout = 'container123'
        
        client = DockerClient()
        mock_run.reset_mock()
        
        volumes = {
            '/host/data': '/container/data',
            '/host/config': '/etc/scylla'
        }
        
        container_id = client.run_container(
            image='scylladb/scylla:latest',
            name='test-node',
            volumes=volumes,
        )
        
        args = mock_run.call_args[0][0]
        
        # Check volume mounts
        assert '-v' in args
        mount_args = [args[i+1] for i, x in enumerate(args) if x == '-v']
        assert '/host/data:/container/data' in mount_args
        assert '/host/config:/etc/scylla' in mount_args
    
    def test_run_container_with_ports(self, mock_run, mock_which):
        """Test running container with port mappings."""
        mock_run.return_value.stdout = 'container123'
        
        client = DockerClient()
        mock_run.reset_mock()
        
        ports = {
            '9042': '9042',
            '7000': '7000'
        }
        
        client.run_container(
            image='scylladb/scylla:latest',
            name='test-node',
            ports=ports,
        )
        
        args = mock_run.call_args[0][0]
        
        # Check port mappings
        assert '-p' in args
        port_args = [args[i+1] for i, x in enumerate(args) if x == '-p']
        assert '9042:9042' in port_args
        assert '7000:7000' in port_args
    
    def test_run_container_with_env(self, mock_run, mock_which):
        """Test running container with environment variables."""
        mock_run.return_value.stdout = 'container123'
        
        client = DockerClient()
        mock_run.reset_mock()
        
        env = {
            'SCYLLA_ARGS': '--developer-mode 1',
            'CLUSTER_NAME': 'test-cluster'
        }
        
        client.run_container(
            image='scylladb/scylla:latest',
            name='test-node',
            env=env,
        )
        
        args = mock_run.call_args[0][0]
        
        # Check environment variables
        assert '-e' in args
        env_args = [args[i+1] for i, x in enumerate(args) if x == '-e']
        assert any('SCYLLA_ARGS' in e for e in env_args)
        assert any('CLUSTER_NAME' in e for e in env_args)
    
    def test_run_container_with_network(self, mock_run, mock_which):
        """Test running container with custom network."""
        mock_run.return_value.stdout = 'container123'
        
        client = DockerClient()
        mock_run.reset_mock()
        
        client.run_container(
            image='scylladb/scylla:latest',
            name='test-node',
            network='ccm-cluster-1',
        )
        
        args = mock_run.call_args[0][0]
        
        # Check network
        assert '--network' in args
        network_idx = args.index('--network')
        assert args[network_idx + 1] == 'ccm-cluster-1'
    
    def test_run_container_with_command(self, mock_run, mock_which):
        """Test running container with custom command."""
        mock_run.return_value.stdout = 'container123'
        
        client = DockerClient()
        mock_run.reset_mock()
        
        client.run_container(
            image='scylladb/scylla:latest',
            name='test-node',
            command=['--seeds', '127.0.0.1', '--developer-mode', '1'],
        )
        
        args = mock_run.call_args[0][0]
        
        # Check command appears after image
        assert '--seeds' in args
        assert '127.0.0.1' in args
        assert '--developer-mode' in args
    
    def test_run_container_failure(self, mock_run, mock_which):
        """Test container start failure."""
        # First call for init (version check) - succeed
        # Second call for run_container - fail
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout='', stderr=''),  # version check
            MagicMock(returncode=1, stdout='', stderr='Error: Image not found')  # run container
        ]
        
        client = DockerClient()
        
        with pytest.raises(ContainerStartError) as exc_info:
            client.run_container(
                image='bad/image:latest',
                name='test-node',
            )
        
        assert 'Image not found' in str(exc_info.value)
    
    def test_exec_command(self, mock_run, mock_which):
        """Test executing command in container."""
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.stdout = 'command output'
        
        returncode, stdout, stderr = client.exec_command(
            container_id='container123',
            command=['echo', 'hello'],
        )
        
        args = mock_run.call_args[0][0]
        assert args[0] == 'docker'
        assert args[1] == 'exec'
        assert 'container123' in args
        assert 'echo' in args
        assert 'hello' in args
        assert returncode == 0
        assert stdout == 'command output'
    
    def test_exec_command_interactive(self, mock_run, mock_which):
        """Test executing interactive command."""
        client = DockerClient()
        mock_run.reset_mock()
        
        client.exec_command(
            container_id='container123',
            command=['bash'],
            interactive=True,
            tty=True,
        )
        
        args = mock_run.call_args[0][0]
        assert '-i' in args
        assert '-t' in args
    
    def test_stop_container(self, mock_run, mock_which):
        """Test stopping a container."""
        client = DockerClient()
        mock_run.reset_mock()
        
        client.stop_container('container123', timeout=30)
        
        args = mock_run.call_args[0][0]
        assert args == ['docker', 'stop', '-t', '30', 'container123']
    
    def test_remove_container(self, mock_run, mock_which):
        """Test removing a container."""
        client = DockerClient()
        mock_run.reset_mock()
        
        client.remove_container('container123', force=True, volumes=True)
        
        args = mock_run.call_args[0][0]
        assert args[0] == 'docker'
        assert args[1] == 'rm'
        assert '-f' in args
        assert '-v' in args
        assert 'container123' in args
    
    def test_get_container_ip(self, mock_run, mock_which):
        """Test getting container IP address."""
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.stdout = '172.17.0.2\n'
        
        ip = client.get_container_ip('container123')
        
        assert ip == '172.17.0.2'
        args = mock_run.call_args[0][0]
        assert 'docker' in args
        assert 'inspect' in args
        assert '{{.NetworkSettings.IPAddress}}' in args
    
    def test_container_exists_true(self, mock_run, mock_which):
        """Test checking if container exists (true case)."""
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.stdout = 'test-node\n'
        
        exists = client.container_exists('test-node')
        
        assert exists is True
        args = mock_run.call_args[0][0]
        assert 'docker' in args
        assert 'ps' in args
        assert '-a' in args
    
    def test_container_exists_false(self, mock_run, mock_which):
        """Test checking if container exists (false case)."""
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.stdout = ''
        
        exists = client.container_exists('nonexistent')
        
        assert exists is False
    
    def test_image_exists(self, mock_run, mock_which):
        """Test checking if image exists."""
        client = DockerClient()
        mock_run.reset_mock()
        
        exists = client.image_exists('scylladb/scylla:latest')
        
        assert exists is True
        args = mock_run.call_args[0][0]
        assert 'docker' in args
        assert 'image' in args
        assert 'inspect' in args
    
    def test_pull_image(self, mock_run, mock_which):
        """Test pulling an image."""
        client = DockerClient()
        mock_run.reset_mock()
        
        result = client.pull_image('scylladb/scylla:latest')
        
        assert result is True
        args = mock_run.call_args[0][0]
        assert args == ['docker', 'pull', 'scylladb/scylla:latest']
    
    def test_create_network(self, mock_run, mock_which):
        """Test creating a network."""
        client = DockerClient()
        mock_run.reset_mock()
        
        result = client.create_network('ccm-test-network')
        
        assert result is True
        args = mock_run.call_args[0][0]
        assert args == ['docker', 'network', 'create', 'ccm-test-network']
    
    def test_remove_network(self, mock_run, mock_which):
        """Test removing a network."""
        client = DockerClient()
        mock_run.reset_mock()
        
        client.remove_network('ccm-test-network')
        
        args = mock_run.call_args[0][0]
        assert args == ['docker', 'network', 'rm', 'ccm-test-network']


class TestPodmanClient:
    """Test Podman client implementation."""
    
    def test_init_success(self, mock_run, mock_which):
        """Test successful Podman client initialization."""
        mock_which.return_value = '/usr/bin/podman'
        
        client = PodmanClient()
        assert client.runtime_name == 'podman'
        mock_which.assert_called_with('podman')
    
    def test_runtime_name(self, mock_run, mock_which):
        """Test Podman client uses correct runtime name."""
        mock_which.return_value = '/usr/bin/podman'
        
        client = PodmanClient()
        assert client.runtime_name == 'podman'


class TestGetContainerClient:
    """Test container client factory function."""
    
    def test_get_docker_explicit(self, mock_run, mock_which):
        """Test explicitly requesting Docker client."""
        mock_which.return_value = '/usr/bin/docker'
        
        client = get_container_client('docker')
        
        assert isinstance(client, DockerClient)
        assert client.runtime_name == 'docker'
    
    def test_get_podman_explicit(self, mock_run, mock_which):
        """Test explicitly requesting Podman client."""
        mock_which.return_value = '/usr/bin/podman'
        
        client = get_container_client('podman')
        
        assert isinstance(client, PodmanClient)
        assert client.runtime_name == 'podman'
    
    def test_get_from_env_docker(self, mock_run, mock_which, monkeypatch):
        """Test getting client from CCM_CONTAINER_RUNTIME env var (docker)."""
        monkeypatch.setenv('CCM_CONTAINER_RUNTIME', 'docker')
        mock_which.return_value = '/usr/bin/docker'
        
        client = get_container_client()
        
        assert isinstance(client, DockerClient)
    
    def test_get_from_env_podman(self, mock_run, mock_which, monkeypatch):
        """Test getting client from CCM_CONTAINER_RUNTIME env var (podman)."""
        monkeypatch.setenv('CCM_CONTAINER_RUNTIME', 'podman')
        mock_which.return_value = '/usr/bin/podman'
        
        client = get_container_client()
        
        assert isinstance(client, PodmanClient)
    
    def test_auto_detect_docker(self, mock_run, mock_which):
        """Test auto-detection prefers Docker."""
        mock_which.return_value = '/usr/bin/docker'
        
        client = get_container_client()
        
        assert isinstance(client, DockerClient)
    
    def test_auto_detect_podman_fallback(self, mock_run, mock_which):
        """Test auto-detection falls back to Podman if Docker not available."""
        def which_side_effect(name):
            if name == 'docker':
                return None
            elif name == 'podman':
                return '/usr/bin/podman'
            return None
        
        mock_which.side_effect = which_side_effect
        
        client = get_container_client()
        
        assert isinstance(client, PodmanClient)
    
    def test_no_runtime_available(self, mock_which):
        """Test error when no container runtime is available."""
        mock_which.return_value = None
        
        with pytest.raises(ContainerRuntimeNotFoundError) as exc_info:
            get_container_client()
        
        assert 'No container runtime found' in str(exc_info.value)


class TestContainerClientEdgeCases:
    """Test edge cases and error handling."""
    
    def test_inspect_container_invalid_json(self, mock_run, mock_which):
        """Test inspect handling invalid JSON."""
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.stdout = 'not valid json'
        
        result = client.inspect_container('container123')
        
        assert result is None
    
    def test_get_container_ip_empty_response(self, mock_run, mock_which):
        """Test getting IP when container has no IP."""
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.stdout = ''
        
        ip = client.get_container_ip('container123')
        
        assert ip is None
    
    def test_pull_image_failure(self, mock_run, mock_which):
        """Test image pull failure."""
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.returncode = 1
        mock_run.return_value.stderr = 'manifest not found'
        
        result = client.pull_image('nonexistent/image:latest')
        
        assert result is False
    
    def test_create_network_already_exists(self, mock_run, mock_which):
        """Test creating network that already exists."""
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.returncode = 1
        mock_run.return_value.stderr = 'network already exists'
        
        # Should still return True
        result = client.create_network('existing-network')
        
        assert result is True
