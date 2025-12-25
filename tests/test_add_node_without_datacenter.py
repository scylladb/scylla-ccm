"""Test for adding nodes without explicitly specifying data_center.

This test reproduces the issue where adding a new node to a cluster without
explicitly specifying the data_center parameter results in improperly configured
cassandra-rackdc.properties file.
"""
import os
import tempfile
import shutil
import pytest
from unittest.mock import Mock, patch
from ccmlib.cluster import Cluster
from ccmlib.node import Node


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    tmp_dir = tempfile.mkdtemp(prefix='ccm_test_')
    yield tmp_dir
    # Clean up the temporary directory
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)


@pytest.fixture
def mock_cluster(temp_dir):
    """Create a minimal cluster instance with mocked methods."""
    cluster = object.__new__(Cluster)
    cluster.name = 'test_cluster'
    cluster.path = temp_dir
    cluster.nodes = {}
    cluster.seeds = []
    cluster.snitch = 'org.apache.cassandra.locator.GossipingPropertyFileSnitch'
    cluster._Cluster__log_level = 'INFO'
    cluster._debug = []
    cluster._trace = []
    
    # Mock methods
    cluster._update_config = Mock()
    cluster.debug = Mock()
    
    return cluster


@pytest.fixture
def mock_node():
    """Create a mock node."""
    def _create_mock_node(name, data_center=None, rack=None):
        node = Mock(spec=Node)
        node.name = name
        node.data_center = data_center
        node.rack = rack
        node._save = Mock()
        node.set_log_level = Mock()
        return node
    return _create_mock_node


def test_add_node_without_datacenter_infers_from_existing_nodes(mock_cluster, mock_node):
    """
    Test that adding a node without specifying data_center infers it from existing nodes.
    
    This reproduces the issue described in:
    "Can't add new node to the cluster if I don't specify data-center explicitly."
    
    The fix makes the add() method look at existing nodes and use their datacenter
    when data_center is not specified, ensuring __update_topology_files() is called.
    """
    # First, add an existing node with datacenter
    existing_node = mock_node('node1', data_center='dc1', rack='RAC1')
    
    # Add the first node
    with patch.object(mock_cluster, '_Cluster__update_topology_files'):
        mock_cluster.add(existing_node, is_seed=True, data_center='dc1', rack='RAC1')
    
    # Now add a second node WITHOUT specifying data_center
    new_node = mock_node('node2')
    
    # Mock __update_topology_files to track if it's called
    with patch.object(mock_cluster, '_Cluster__update_topology_files') as mock_update_topology:
        # Add node without specifying data_center (data_center=None)
        mock_cluster.add(new_node, is_seed=True, data_center=None)
        
        # The new node should have inherited dc1 from the existing node
        assert new_node.data_center == 'dc1', \
            f"Expected node to inherit dc1, got {new_node.data_center}"
        assert new_node.rack == 'RAC1', \
            f"Expected node to inherit RAC1, got {new_node.rack}"
        
        # __update_topology_files() should be called since datacenter was inferred
        mock_update_topology.assert_called_once()


def test_add_first_node_without_datacenter(mock_cluster, mock_node):
    """
    Test that adding the first node without specifying data_center doesn't call
    __update_topology_files() since there's no datacenter to infer.
    
    This is expected behavior - the first node needs an explicit datacenter,
    or it will be handled by __update_topology_using_rackdc_properties() 
    which defaults to dc1.
    """
    # Create the first node without datacenter
    node = mock_node('node1')
    
    # Mock __update_topology_files to track if it's called
    with patch.object(mock_cluster, '_Cluster__update_topology_files') as mock_update_topology:
        # Add first node without specifying data_center
        mock_cluster.add(node, is_seed=True, data_center=None)
        
        # The node should still have None datacenter (no nodes to infer from)
        assert node.data_center is None
        
        # __update_topology_files() should NOT be called for None datacenter
        mock_update_topology.assert_not_called()


def test_add_node_with_explicit_datacenter_calls_update_topology(mock_cluster, mock_node):
    """
    Test that adding a node WITH specifying data_center calls __update_topology_files().
    This is the workaround mentioned in the issue that currently works.
    """
    # Create a mock node
    node = mock_node('node1', data_center='dc1', rack='RAC1')
    
    # Mock __update_topology_files to track if it's called
    with patch.object(mock_cluster, '_Cluster__update_topology_files') as mock_update_topology:
        # Add node WITH specifying data_center
        mock_cluster.add(node, is_seed=True, data_center='dc1')
        
        # This should call __update_topology_files() - this currently works
        mock_update_topology.assert_called_once()


