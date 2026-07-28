"""
Unit tests for the container client abstraction layer.

These tests mock the container runtime to avoid requiring Docker/Podman installation.
"""

import pytest
import threading
import time
from unittest.mock import patch, MagicMock

from ccmlib.container_client import (
    ContainerLogManager,
    DockerClient,
    PodmanClient,
    get_container_client,
    ContainerExecError,
    ContainerRuntimeNotFoundError,
    ContainerStartError,
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

    def test_init_version_check_timeout_treated_as_not_available(self, mock_which):
        """A hung `docker version` must be treated as unavailable, not raise
        TimeoutExpired out of __init__."""
        from subprocess import TimeoutExpired

        with patch('ccmlib.container_client.run', side_effect=TimeoutExpired(cmd=['docker', 'version'], timeout=5)):
            with pytest.raises(ContainerRuntimeNotFoundError):
                DockerClient()
    
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
        
        client.run_container(
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

    def test_remove_container_check_raises_on_failure(self, mock_run, mock_which):
        """check=True surfaces failures via exception for safety-critical callers."""
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.returncode = 1
        mock_run.return_value.stderr = 'no such container'

        with pytest.raises(ContainerExecError):
            client.remove_container('container123', force=True, check=True)

    def test_remove_container_no_check_logs_warning_on_failure(self, mock_run, mock_which):
        """Default (check=False) preserves the legacy warn-and-continue behaviour."""
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.returncode = 1
        mock_run.return_value.stderr = 'no such container'

        client.remove_container('container123', force=True)  # should not raise

    def test_remove_container_force_no_time_flag_for_docker(self, mock_run, mock_which):
        """Docker's `rm -f` is already an instant SIGKILL and has no --time flag."""
        client = DockerClient()
        mock_run.reset_mock()

        client.remove_container('container123', force=True)

        args = mock_run.call_args[0][0]
        assert args == ['docker', 'rm', '-f', 'container123']
        assert '--time' not in args

    def test_remove_containers_batches_into_one_call(self, mock_run, mock_which):
        """Multiple IDs fit in one chunk should be removed with a single `rm` call."""
        client = DockerClient()
        mock_run.reset_mock()

        failed = client.remove_containers(['c1', 'c2', 'c3'], force=True, volumes=True)

        assert failed == []
        assert mock_run.call_count == 1
        args = mock_run.call_args[0][0]
        assert args == ['docker', 'rm', '-f', '-v', 'c1', 'c2', 'c3']

    def test_remove_containers_chunks_large_lists(self, mock_run, mock_which):
        """More IDs than chunk_size should be split across multiple `rm` calls."""
        client = DockerClient()
        mock_run.reset_mock()

        ids = [f'c{i}' for i in range(25)]
        failed = client.remove_containers(ids, force=True, chunk_size=10)

        assert failed == []
        assert mock_run.call_count == 3
        chunk_lens = [len(call.args[0]) - 3 for call in mock_run.call_args_list]  # minus [runtime, rm, -f]
        assert chunk_lens == [10, 10, 5]

    def test_remove_containers_empty_list_is_noop(self, mock_run, mock_which):
        """No container IDs should mean no subprocess calls at all."""
        client = DockerClient()
        mock_run.reset_mock()

        failed = client.remove_containers([], force=True)

        assert failed == []
        mock_run.assert_not_called()

    def test_remove_containers_reports_failed_chunk(self, mock_run, mock_which):
        """A chunk that fails should have all its IDs reported back to the caller."""
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value = MagicMock(returncode=1, stdout='', stderr='boom')

        failed = client.remove_containers(['c1', 'c2'], force=True)

        assert failed == ['c1', 'c2']

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

    def test_remove_container_force_adds_time_zero(self, mock_run, mock_which):
        """Plain `podman rm -f` still waits podman's default 10s stop-grace
        before SIGKILL; --time 0 makes force-removal actually instant."""
        mock_which.return_value = '/usr/bin/podman'
        client = PodmanClient()
        mock_run.reset_mock()

        client.remove_container('container123', force=True, volumes=True)

        args = mock_run.call_args[0][0]
        assert args == ['podman', 'rm', '-f', '--time', '0', '-v', 'container123']

    def test_remove_container_non_force_no_time_flag(self, mock_run, mock_which):
        """--time is only relevant to force removal; don't add it otherwise."""
        mock_which.return_value = '/usr/bin/podman'
        client = PodmanClient()
        mock_run.reset_mock()

        client.remove_container('container123', force=False)

        args = mock_run.call_args[0][0]
        assert args == ['podman', 'rm', 'container123']

    def test_remove_containers_batch_adds_time_zero(self, mock_run, mock_which):
        """Batched podman removal should also skip the stop-grace period."""
        mock_which.return_value = '/usr/bin/podman'
        client = PodmanClient()
        mock_run.reset_mock()

        failed = client.remove_containers(['c1', 'c2'], force=True, volumes=True)

        assert failed == []
        args = mock_run.call_args[0][0]
        assert args == ['podman', 'rm', '-f', '--time', '0', '-v', 'c1', 'c2']


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

    def test_inspect_container_empty_stdout_returns_none_without_logging_error(self, mock_run, mock_which, caplog):
        """returncode=0 with empty stdout (e.g. a lenient test double) is treated
        as 'not found', same as inspect_network(), instead of attempting to
        json.loads('') and logging a parse-error."""
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.stdout = ''

        result = client.inspect_container('container123')

        assert result is None
        assert not any('Failed to parse' in r.message for r in caplog.records)
    
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


class TestRunContainerExtendedArgs:
    """Test the extended run_container()/create_network()/exec_command() parameters
    needed by ScyllaPodmanCluster (static IP, labels, CPU pinning, timeouts)."""

    def test_run_container_podman_relabels_volumes_by_default(self, mock_run, mock_which):
        """On podman, volumes get the :z SELinux relabel suffix by default."""
        mock_which.return_value = '/usr/bin/podman'
        client = PodmanClient()
        mock_run.reset_mock()
        mock_run.return_value.stdout = 'container123'

        client.run_container(
            image='img', name='n',
            volumes={'/host/conf': '/etc/scylla'},
        )

        args = mock_run.call_args[0][0]
        mount_args = [args[i + 1] for i, x in enumerate(args) if x == '-v']
        assert mount_args == ['/host/conf:/etc/scylla:z']

    def test_run_container_docker_never_relabels_volumes(self, mock_run, mock_which):
        """Docker has no :z relabel concept; volumes are never suffixed."""
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.stdout = 'container123'

        client.run_container(
            image='img', name='n',
            volumes={'/host/conf': '/etc/scylla', '/tmp': '/tmp'},
            volumes_no_relabel=['/tmp'],
        )

        args = mock_run.call_args[0][0]
        mount_args = [args[i + 1] for i, x in enumerate(args) if x == '-v']
        assert '/host/conf:/etc/scylla' in mount_args
        assert '/tmp:/tmp' in mount_args

    def test_run_container_volumes_no_relabel_excludes_specific_paths(self, mock_run, mock_which):
        """volumes_no_relabel opts specific host paths out of the :z suffix
        on podman (e.g. /tmp, which some SELinux policies refuse to relabel
        and which shouldn't be relabeled since it's a shared system path)."""
        mock_which.return_value = '/usr/bin/podman'
        client = PodmanClient()
        mock_run.reset_mock()
        mock_run.return_value.stdout = 'container123'

        client.run_container(
            image='img', name='n',
            volumes={'/host/conf': '/etc/scylla', '/tmp': '/tmp'},
            volumes_no_relabel=['/tmp'],
        )

        args = mock_run.call_args[0][0]
        mount_args = [args[i + 1] for i, x in enumerate(args) if x == '-v']
        assert '/host/conf:/etc/scylla:z' in mount_args
        assert '/tmp:/tmp' in mount_args
        assert '/tmp:/tmp:z' not in mount_args

    def test_run_container_with_ip(self, mock_run, mock_which):
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.stdout = 'container123'

        client.run_container(image='img', name='n', network='net1', ip='10.89.1.5')

        args = mock_run.call_args[0][0]
        assert '--ip' in args
        assert args[args.index('--ip') + 1] == '10.89.1.5'

    def test_run_container_with_labels(self, mock_run, mock_which):
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.stdout = 'container123'

        client.run_container(image='img', name='n', labels={'owner': '123', 'kind': 'ccm'})

        args = mock_run.call_args[0][0]
        label_args = [args[i + 1] for i, x in enumerate(args) if x == '--label']
        assert 'owner=123' in label_args
        assert 'kind=ccm' in label_args

    def test_run_container_with_cpuset_cpus(self, mock_run, mock_which):
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.stdout = 'container123'

        client.run_container(image='img', name='n', cpuset_cpus='0,1,2')

        args = mock_run.call_args[0][0]
        assert '--cpuset-cpus' in args
        assert args[args.index('--cpuset-cpus') + 1] == '0,1,2'

    def test_run_container_with_hostname(self, mock_run, mock_which):
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.stdout = 'container123'

        client.run_container(image='img', name='n', hostname='node1')

        args = mock_run.call_args[0][0]
        assert '--hostname' in args
        assert args[args.index('--hostname') + 1] == 'node1'

    def test_exec_command_timeout_passed_through(self, mock_run, mock_which):
        client = DockerClient()
        mock_run.reset_mock()

        client.exec_command('container123', ['echo', 'hi'], timeout=5)

        assert mock_run.call_args.kwargs.get('timeout') == 5

    def test_run_command_timeout_expired_returns_failure_tuple(self, mock_which):
        """A timed-out command should surface as a normal failure, not raise."""
        from subprocess import TimeoutExpired
        with patch('ccmlib.container_client.run', side_effect=[
            MagicMock(returncode=0, stdout='', stderr=''),  # init version check
            TimeoutExpired(cmd=['docker'], timeout=5),  # the actual call
        ]):
            client = DockerClient()
            returncode, stdout, stderr = client._run_command(['docker', 'exec', 'x'], timeout=5)

        assert returncode == -1
        assert 'timed out' in stderr

    def test_create_network_with_subnet_and_gateway(self, mock_run, mock_which):
        client = DockerClient()
        mock_run.reset_mock()

        client.create_network('ccm-rack1', subnet='10.89.1.0/24', gateway='10.89.1.254',
                              labels={'owner': '42'})

        args = mock_run.call_args[0][0]
        assert '--subnet' in args
        assert args[args.index('--subnet') + 1] == '10.89.1.0/24'
        assert '--gateway' in args
        assert args[args.index('--gateway') + 1] == '10.89.1.254'
        assert '--label' in args
        assert args[args.index('--label') + 1] == 'owner=42'

    def test_create_network_extended_raises_on_failure(self, mock_run, mock_which):
        """Extended form (subnet given) surfaces failures via exception, not a bool,
        so callers can inspect stderr (e.g. to detect subnet conflicts and retry)."""
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.returncode = 1
        mock_run.return_value.stderr = 'subnet 10.89.1.0/24 overlaps with existing network'

        with pytest.raises(ContainerStartError) as exc_info:
            client.create_network('ccm-rack1', subnet='10.89.1.0/24')

        assert 'overlaps' in str(exc_info.value)

    def test_create_network_simple_form_still_returns_bool_on_failure(self, mock_run, mock_which):
        """Legacy callers (ScyllaDockerCluster) that don't pass subnet/gateway/labels
        keep the original return-False-and-log behaviour."""
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.returncode = 1
        mock_run.return_value.stderr = 'some other error'

        result = client.create_network('plain-network')

        assert result is False

    def test_remove_network_force(self, mock_run, mock_which):
        client = DockerClient()
        mock_run.reset_mock()

        client.remove_network('ccm-rack1', force=True)

        args = mock_run.call_args[0][0]
        assert args == ['docker', 'network', 'rm', '-f', 'ccm-rack1']

    def test_remove_network_check_raises_on_failure(self, mock_run, mock_which):
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.returncode = 1
        mock_run.return_value.stderr = 'network in use'

        with pytest.raises(ContainerExecError):
            client.remove_network('ccm-rack1', force=True, check=True)

    def test_inspect_network_found(self, mock_run, mock_which):
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.stdout = '[{"name": "ccm-rack1", "subnets": [{"subnet": "10.89.1.0/24"}]}]'

        info = client.inspect_network('ccm-rack1')

        assert info['name'] == 'ccm-rack1'
        args = mock_run.call_args[0][0]
        assert args == ['docker', 'network', 'inspect', 'ccm-rack1']

    def test_inspect_network_not_found(self, mock_run, mock_which):
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.returncode = 1
        mock_run.return_value.stdout = ''

        assert client.inspect_network('missing') is None

    def test_list_networks(self, mock_run, mock_which):
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.stdout = '[{"name": "a"}, {"name": "b"}]'

        networks = client.list_networks()

        assert [n['name'] for n in networks] == ['a', 'b']
        args = mock_run.call_args[0][0]
        assert args == ['docker', 'network', 'ls', '--format', 'json']

    def test_list_networks_on_error_returns_empty(self, mock_run, mock_which):
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.returncode = 1
        mock_run.return_value.stderr = 'boom'

        assert client.list_networks() == []

    def test_get_container_pid(self, mock_run, mock_which):
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.stdout = '12345\n'

        assert client.get_container_pid('c1') == 12345
        args = mock_run.call_args[0][0]
        assert args == ['docker', 'inspect', '--format', '{{.State.Pid}}', 'c1']

    def test_get_container_pid_not_running(self, mock_run, mock_which):
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.stdout = '0\n'

        assert client.get_container_pid('c1') is None

    def test_get_container_pid_not_found(self, mock_run, mock_which):
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.returncode = 1
        mock_run.return_value.stdout = ''
        mock_run.return_value.stderr = 'no such container'

        assert client.get_container_pid('c1') is None

    def test_get_container_pid_unparseable_output_logs_and_returns_none(self, mock_run, mock_which):
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.stdout = '<no value>\n'

        assert client.get_container_pid('c1') is None

    def test_get_container_status(self, mock_run, mock_which):
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.stdout = 'running\n'

        assert client.get_container_status('c1') == 'running'
        args = mock_run.call_args[0][0]
        assert args == ['docker', 'inspect', '--format', '{{.State.Status}}', 'c1']

    def test_get_container_status_not_found(self, mock_run, mock_which):
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.returncode = 1
        mock_run.return_value.stdout = ''

        assert client.get_container_status('c1') is None

    def test_get_container_labels(self, mock_run, mock_which):
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.stdout = '[{"Config": {"Labels": {"owner": "42"}}}]'

        assert client.get_container_labels('c1') == {'owner': '42'}

    def test_get_container_labels_missing_returns_empty_dict(self, mock_run, mock_which):
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.returncode = 1
        mock_run.return_value.stdout = ''

        assert client.get_container_labels('c1') == {}

    def test_copy_from_container(self, mock_which):
        client_run_patch = patch('ccmlib.container_client.run')
        with client_run_patch as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=b'', stderr=b'')
            client = DockerClient()
            mock_run.reset_mock()
            mock_run.return_value = MagicMock(returncode=0, stdout=b'tarball-bytes', stderr=b'')

            data = client.copy_from_container('c1', '/etc/scylla/')

        assert data == b'tarball-bytes'
        args = mock_run.call_args[0][0]
        assert args == ['docker', 'container', 'cp', '-a', 'c1:/etc/scylla/', '-']

    def test_copy_from_container_failure_raises(self, mock_which):
        with patch('ccmlib.container_client.run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=b'', stderr=b'')
            client = DockerClient()
            mock_run.reset_mock()
            mock_run.return_value = MagicMock(returncode=1, stdout=b'', stderr=b'no such container')

            with pytest.raises(ContainerExecError):
                client.copy_from_container('missing', '/etc/scylla/')


class TestStreamLogs:
    """Test the one-shot (non-follow) log-fetching helper."""

    def test_stream_logs_basic(self, mock_run, mock_which):
        client = DockerClient()
        mock_run.reset_mock()
        mock_run.return_value.stdout = 'log line 1\n'
        mock_run.return_value.stderr = ''

        output = client.stream_logs('container123')

        assert output == 'log line 1\n'
        args = mock_run.call_args[0][0]
        assert args == ['docker', 'logs', 'container123']

    def test_stream_logs_with_tail(self, mock_run, mock_which):
        client = DockerClient()
        mock_run.reset_mock()

        client.stream_logs('container123', tail=50)

        args = mock_run.call_args[0][0]
        assert '--tail' in args
        assert args[args.index('--tail') + 1] == '50'

    def test_stream_logs_tail_zero_is_not_silently_dropped(self, mock_run, mock_which):
        """tail=0 must still pass --tail 0; `if tail:` would drop it as falsy."""
        client = DockerClient()
        mock_run.reset_mock()

        client.stream_logs('container123', tail=0)

        args = mock_run.call_args[0][0]
        assert '--tail' in args
        assert args[args.index('--tail') + 1] == '0'

    def test_stream_logs_no_tail_omits_flag(self, mock_run, mock_which):
        client = DockerClient()
        mock_run.reset_mock()

        client.stream_logs('container123')

        args = mock_run.call_args[0][0]
        assert '--tail' not in args


class TestPodmanUnshare:
    """Test the Podman-only `unshare` escape hatch."""

    def test_unshare_success(self, mock_run, mock_which):
        mock_which.return_value = '/usr/bin/podman'
        client = PodmanClient()
        mock_run.reset_mock()
        mock_run.return_value.stdout = 'ok'

        returncode, stdout, stderr = client.unshare(['chown', '-R', '999:999', '/some/path'])

        assert returncode == 0
        args = mock_run.call_args[0][0]
        assert args == ['podman', 'unshare', 'chown', '-R', '999:999', '/some/path']

    def test_unshare_check_raises_on_failure(self, mock_run, mock_which):
        mock_which.return_value = '/usr/bin/podman'
        client = PodmanClient()
        mock_run.reset_mock()
        mock_run.return_value.returncode = 1
        mock_run.return_value.stderr = 'permission denied'

        with pytest.raises(ContainerExecError):
            client.unshare(['chown', '-R', '999:999', '/some/path'], check=True)

    def test_unshare_not_check_returns_tuple_on_failure(self, mock_run, mock_which):
        mock_which.return_value = '/usr/bin/podman'
        client = PodmanClient()
        mock_run.reset_mock()
        mock_run.return_value.returncode = 1
        mock_run.return_value.stderr = 'permission denied'

        returncode, stdout, stderr = client.unshare(['chown', '-R', '999:999', '/some/path'])

        assert returncode == 1
        assert stderr == 'permission denied'

    def test_docker_client_has_no_unshare(self, mock_run, mock_which):
        client = DockerClient()
        assert not hasattr(client, 'unshare')


class TestContainerLogManager:
    """Test the shared log-streaming manager used by both Docker and Podman clusters."""

    def _make_client(self, mock_which):
        mock_which.return_value = '/usr/bin/podman'
        with patch('ccmlib.container_client.run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout='', stderr='')
            return PodmanClient()

    def test_start_stream_spawns_thread_and_writes_lines(self, mock_which, tmp_path):
        client = self._make_client(mock_which)
        manager = ContainerLogManager(client)
        log_file = tmp_path / "system.log"

        fake_process = MagicMock()
        fake_process.stdout = iter(["line1\n", "line2\n"])
        with patch('ccmlib.container_client.Popen', return_value=fake_process) as mock_popen:
            manager.start_stream('container123', str(log_file))
            # Wait for the daemon thread to finish consuming the fake iterator.
            state = manager._streams.get('container123')
            if state and state.thread:
                state.thread.join(timeout=5)

        assert mock_popen.call_args[0][0][0] in ('podman', '/usr/bin/podman')
        assert mock_popen.call_args[0][0][1:3] == ['logs', '-f']
        assert log_file.read_text() == "line1\nline2\n"

    def test_start_stream_is_idempotent(self, mock_which, tmp_path):
        client = self._make_client(mock_which)
        manager = ContainerLogManager(client)
        log_file = tmp_path / "system.log"

        fake_process = MagicMock()
        fake_process.stdout = iter([])
        with patch('ccmlib.container_client.Popen', return_value=fake_process) as mock_popen:
            manager.start_stream('container123', str(log_file))
            state = manager._streams.get('container123')
            manager.start_stream('container123', str(log_file))
            if state and state.thread:
                state.thread.join(timeout=5)

        # Second call should be a no-op since the stream is already registered.
        assert mock_popen.call_count == 1

    def test_stop_stream_sets_stop_event(self, mock_which, tmp_path):
        client = self._make_client(mock_which)
        manager = ContainerLogManager(client)
        log_file = tmp_path / "system.log"

        fake_process = MagicMock()
        fake_process.stdout = iter([])
        with patch('ccmlib.container_client.Popen', return_value=fake_process):
            manager.start_stream('container123', str(log_file))
            state = manager._streams['container123']
            manager.stop_stream('container123')

        assert state.stop_event.is_set()
        assert 'container123' not in manager._streams

    def test_stop_all_clears_all_streams(self, mock_which, tmp_path):
        client = self._make_client(mock_which)
        manager = ContainerLogManager(client)

        fake_process = MagicMock()
        fake_process.stdout = iter([])
        with patch('ccmlib.container_client.Popen', return_value=fake_process):
            manager.start_stream('c1', str(tmp_path / "1.log"))
            manager.start_stream('c2', str(tmp_path / "2.log"))
            manager.stop_all()

        assert manager._streams == {}

    def test_stop_stream_terminates_process_immediately_when_quiescent(self, mock_which, tmp_path):
        """A quiet container's stream must not linger after stop_stream() --
        the blocking read on process.stdout won't return on its own."""
        client = self._make_client(mock_which)
        manager = ContainerLogManager(client)
        log_file = tmp_path / "system.log"
        terminated = threading.Event()

        class BlockingStdout:
            """Simulates `logs -f` on a quiet container: blocks until terminated."""

            def __iter__(self):
                return self

            def __next__(self):
                if not terminated.wait(timeout=10):
                    raise AssertionError("process was not terminated within 10s")
                raise StopIteration

        fake_process = MagicMock()
        fake_process.stdout = BlockingStdout()
        fake_process.terminate.side_effect = terminated.set

        with patch('ccmlib.container_client.Popen', return_value=fake_process):
            manager.start_stream('container123', str(log_file))
            state = manager._streams['container123']
            # Wait for the background thread to publish state.process
            # (i.e. reach the blocking read) before stopping it.
            for _ in range(200):
                with state.lock:
                    if state.process is not None:
                        break
                time.sleep(0.01)
            else:
                pytest.fail("background thread never published state.process")

            manager.stop_stream('container123')
            state.thread.join(timeout=5)

        assert not state.thread.is_alive()
        fake_process.terminate.assert_called()

    def test_stop_stream_before_process_published_terminates_on_publish(self, mock_which, tmp_path):
        """If stop_stream() races start_stream() before Popen() runs, the
        thread must still notice stop_event and terminate the process."""
        client = self._make_client(mock_which)
        manager = ContainerLogManager(client)
        log_file = tmp_path / "system.log"

        fake_process = MagicMock()
        fake_process.stdout = iter([])

        with patch('ccmlib.container_client.Popen', return_value=fake_process):
            # Directly construct the race: register a state and mark it
            # stopped *before* the streaming thread runs at all.
            from ccmlib.container_client import _LogStreamState
            state = _LogStreamState(str(log_file))
            with manager._lock:
                manager._streams['container123'] = state
            state.stop_event.set()

            thread = threading.Thread(
                target=manager._stream_logs, args=('container123', state), daemon=True
            )
            thread.start()
            thread.join(timeout=5)

        assert not thread.is_alive()
        fake_process.terminate.assert_called()


class TestPodmanClientSingletonRace:
    """Regression test for the double-checked-locking fix on the module-level
    PodmanClient singleton in scylla_podman_cluster.py."""

    def test_concurrent_get_podman_client_constructs_exactly_once(self, monkeypatch):
        import ccmlib.scylla_podman_cluster as podman_mod

        monkeypatch.setattr(podman_mod, "_PODMAN_CLIENT", None)

        construct_count = {"n": 0}
        real_init = podman_mod.PodmanClient.__init__

        def counting_init(self):
            # Simulate a slow `podman version` call so races overlap.
            time.sleep(0.05)
            construct_count["n"] += 1
            self.runtime_name = "podman"
            self.runtime_path = "podman"

        monkeypatch.setattr(podman_mod.PodmanClient, "__init__", counting_init)

        results = []
        barrier = threading.Barrier(8)

        def worker():
            barrier.wait(timeout=5)
            results.append(podman_mod._get_podman_client())

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        assert construct_count["n"] == 1
        assert len(results) == 8
        assert all(r is results[0] for r in results)

        monkeypatch.setattr(podman_mod.PodmanClient, "__init__", real_init)
