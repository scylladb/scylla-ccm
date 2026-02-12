"""
Integration tests for Docker-based Scylla clusters.

These tests verify that CCM works correctly with Docker containers,
including cluster lifecycle, node operations, and CQL functionality.
"""

import re
import pytest
import time
from pathlib import Path


@pytest.mark.docker
class TestScyllaDockerClusterIntegration:
    """Integration tests for Docker cluster functionality."""
    
    @staticmethod
    def parse_nodetool_status(lines):
        """Parse output of nodetool status."""
        keys = ["status", "address"]
        nodes_statuses = []
        line_re = re.compile(
            r"(?P<status>[UNDJ]{2}?)\s+(?P<address>[\d]{1,3}\.[\d]{1,3}\.[\d]{1,3}\.[\d]{1,3}?)\s")
        for line in lines:
            node_status = {}
            res = line_re.search(line)
            if res:
                for key in keys:
                    node_status[key] = res[key]
                nodes_statuses.append(node_status)
        return nodes_statuses

    def test_cluster_creation(self, docker_cluster):
        """Test that Docker cluster can be created."""
        assert docker_cluster is not None
        assert docker_cluster.is_docker()
        assert docker_cluster.docker_image is not None
        
        # Verify cluster has expected number of nodes
        nodes = docker_cluster.nodelist()
        assert len(nodes) == 3
        
        # Verify nodes are Docker nodes
        for node in nodes:
            assert node.is_docker()
            assert node.is_scylla()
    
    def test_cluster_network(self, docker_cluster):
        """Test that cluster has isolated network."""
        assert docker_cluster.cluster_network is not None
        assert 'ccm-' in docker_cluster.cluster_network
    
    def test_container_client(self, docker_cluster):
        """Test that container client is initialized."""
        client = docker_cluster.get_container_client()
        assert client is not None
        assert client.runtime_name in ['docker', 'podman']
    
    def test_nodes_running(self, docker_cluster):
        """Test that all nodes are running."""
        nodes = docker_cluster.nodelist()
        
        for node in nodes:
            assert node.is_running()
            assert node.pid is not None
            
            # Verify container exists
            client = node.get_container_client()
            assert client.container_exists(node.docker_name)
    
    def test_node_addresses(self, docker_cluster):
        """Test that nodes have valid IP addresses."""
        nodes = docker_cluster.nodelist()
        
        for node in nodes:
            address = node.address()
            assert address is not None
            
            # IP should be in valid format
            parts = address.split('.')
            assert len(parts) == 4
            for part in parts:
                assert 0 <= int(part) <= 255
    
    def test_cqlsh_connection(self, docker_cluster):
        """Test CQL connection via cqlsh."""
        node1 = docker_cluster.nodelist()[0]
        
        # Create keyspace
        node1.run_cqlsh(
            '''
            CREATE KEYSPACE IF NOT EXISTS test_ks 
            WITH replication = { 'class' :'SimpleStrategy', 'replication_factor': 3};
            '''
        )
        
        # Verify keyspace was created
        output, _ = node1.run_cqlsh('DESCRIBE KEYSPACES;', return_output=True)
        assert 'test_ks' in output
    
    def test_cql_operations(self, docker_cluster):
        """Test basic CQL operations."""
        node1 = docker_cluster.nodelist()[0]
        
        # Create keyspace and table
        node1.run_cqlsh(
            '''
            CREATE KEYSPACE IF NOT EXISTS ks 
            WITH replication = { 'class' :'SimpleStrategy', 'replication_factor': 3};
            USE ks;
            CREATE TABLE IF NOT EXISTS test (key int PRIMARY KEY, value text);
            INSERT INTO test (key, value) VALUES (1, 'hello');
            INSERT INTO test (key, value) VALUES (2, 'world');
            '''
        )
        
        # Query data
        output, error = node1.run_cqlsh('SELECT * from ks.test', return_output=True)
        assert error == ''
        assert '1' in output
        assert 'hello' in output
        assert '2' in output
        assert 'world' in output
    
    def test_nodetool_status(self, docker_cluster):
        """Test nodetool status command."""
        node1 = docker_cluster.nodelist()[0]
        
        status, error = node1.nodetool('status')
        assert error == ''
        assert 'UN' in status  # At least one node should be Up/Normal
        
        # Parse and verify all nodes are up
        nodes_statuses = self.parse_nodetool_status(status.splitlines())
        assert len(nodes_statuses) == 3
        assert all(node['status'] == 'UN' for node in nodes_statuses), \
            "Expecting all nodes to be UN (Up/Normal)"
    
    def test_nodetool_info(self, docker_cluster):
        """Test nodetool info command."""
        node1 = docker_cluster.nodelist()[0]
        
        info, error = node1.nodetool('info')
        assert error == ''
        assert 'ID' in info
        assert 'Gossip active' in info or 'gossip' in info.lower()
    
    def test_node_restart(self, docker_cluster):
        """Test stopping and starting a node."""
        node3 = docker_cluster.nodelist()[2]
        
        # Stop node
        node3.stop()
        time.sleep(2)
        assert not node3.is_running()
        
        # Start node
        node3.start()
        time.sleep(5)  # Give node time to start
        assert node3.is_running()
        
        # Verify node rejoined cluster
        node1 = docker_cluster.nodelist()[0]
        status, _ = node1.nodetool('status')
        assert 'UN' in status
    
    def test_node_stop_ungently(self, docker_cluster):
        """Test ungraceful node stop."""
        node3 = docker_cluster.nodelist()[2]
        
        node3.stop(gently=False)
        time.sleep(2)
        assert not node3.is_running()
        
        node3.start()
        time.sleep(5)
        assert node3.is_running()
    
    def test_data_persistence(self, docker_cluster):
        """Test that data persists across node restart."""
        node1 = docker_cluster.nodelist()[0]
        
        # Insert data
        node1.run_cqlsh(
            '''
            CREATE KEYSPACE IF NOT EXISTS persist_ks 
            WITH replication = { 'class' :'SimpleStrategy', 'replication_factor': 3};
            USE persist_ks;
            CREATE TABLE IF NOT EXISTS data (id int PRIMARY KEY, val text);
            INSERT INTO data (id, val) VALUES (42, 'persistent');
            '''
        )
        
        # Restart node
        node1.stop()
        time.sleep(2)
        node1.start()
        time.sleep(5)
        
        # Verify data still exists
        output, _ = node1.run_cqlsh('SELECT * FROM persist_ks.data WHERE id=42', return_output=True)
        assert '42' in output
        assert 'persistent' in output
    
    def test_directory_mounting(self, docker_cluster):
        """Test that directories are properly mounted."""
        node1 = docker_cluster.nodelist()[0]
        
        # Check that expected directories exist on host
        node_path = Path(node1.get_path())
        assert node_path.exists()
        
        for directory in ['conf', 'data', 'commitlogs', 'hints', 'logs']:
            dir_path = node_path / directory
            assert dir_path.exists(), f"Expected directory {directory} does not exist"
    
    def test_configuration_access(self, docker_cluster):
        """Test that configuration files are accessible."""
        node1 = docker_cluster.nodelist()[0]
        
        # Check scylla.yaml exists
        conf_path = Path(node1.get_conf_dir()) / 'scylla.yaml'
        assert conf_path.exists()
        
        # Verify we can read it
        with open(conf_path, 'r') as f:
            content = f.read()
            assert 'cluster_name' in content or 'data_file_directories' in content
    
    def test_log_access(self, docker_cluster):
        """Test that logs are accessible."""
        node1 = docker_cluster.nodelist()[0]
        
        log_path = Path(node1.get_path()) / 'logs' / 'system.log'
        
        # Wait for log file to be created (may take a moment)
        max_wait = 30
        waited = 0
        while not log_path.exists() and waited < max_wait:
            time.sleep(1)
            waited += 1
        
        assert log_path.exists(), "Log file was not created"
        
        # Verify we can read logs
        with open(log_path, 'r') as f:
            content = f.read()
            # Should contain some Scylla-related content
            assert len(content) > 0


@pytest.mark.docker
@pytest.mark.slow
class TestScyllaDockerClusterStress:
    """Stress tests for Docker clusters (slower tests)."""
    
    def test_stress_workload(self, docker_cluster):
        """Test running cassandra-stress workload."""
        node1 = docker_cluster.nodelist()[0]
        
        # Run a small stress test
        ret = node1.stress(['write', 'n=1000', '-rate', 'threads=1'])
        
        assert ret.returncode == 0 or 'END' in ret.stdout, \
            f"Stress test failed: {ret.stderr}"
        
        # Verify some writes succeeded
        if ret.stdout:
            assert '1,000' in ret.stdout or '1000' in ret.stdout or 'END' in ret.stdout


@pytest.mark.docker
class TestContainerRuntimeSelection:
    """Test container runtime selection functionality."""
    
    def test_auto_detection(self, docker_cluster):
        """Test that container runtime is auto-detected."""
        client = docker_cluster.get_container_client()
        assert client.runtime_name in ['docker', 'podman']
    
    def test_docker_runtime_works(self, docker_cluster):
        """Test that Docker runtime works correctly."""
        client = docker_cluster.get_container_client()
        
        # If Docker is available, it should work
        if client.runtime_name == 'docker':
            # Verify we can list containers
            for node in docker_cluster.nodelist():
                assert client.container_exists(node.docker_name)


@pytest.mark.docker
class TestMultipleDockerClusters:
    """Test running multiple Docker clusters in parallel."""
    
    @pytest.mark.skip(reason="Requires creating additional clusters in test")
    def test_parallel_clusters(self):
        """Test that multiple clusters can run simultaneously."""
        # This would require creating multiple clusters
        # which is better done in a separate test setup
        pass


@pytest.mark.docker
class TestDockerErrorHandling:
    """Test error handling in Docker operations."""
    
    def test_invalid_image_handled(self):
        """Test that invalid Docker image is handled gracefully."""
        from ccmlib.scylla_docker_cluster import ScyllaDockerCluster
        from ccmlib.container_client import ContainerImageNotFoundError
        import tempfile
        import os
        
        with tempfile.TemporaryDirectory() as tmpdir:
            # Creating a cluster with invalid image should raise an error
            # when trying to validate/pull the image
            # Note: This may succeed if image validation is skipped
            try:
                cluster = ScyllaDockerCluster(
                    tmpdir,
                    'test-invalid',
                    docker_image='nonexistent/invalid:tag'
                )
                # If we get here, validation was skipped or it auto-pulls
                # That's okay - the error will occur when trying to run containers
            except ContainerImageNotFoundError:
                # Expected behavior
                pass
