"""Test for adding nodes without explicitly specifying data_center.

This test reproduces the issue where adding a new node to a cluster without
explicitly specifying the data_center parameter results in improperly configured
cassandra-rackdc.properties file.
"""
import os
import tempfile
import shutil
import pytest
from unittest.mock import Mock, patch, MagicMock, call
from ccmlib.cluster import Cluster
from ccmlib.node import Node


class TestAddNodeWithoutDatacenter:
    """Test adding nodes without explicitly specifying datacenter."""

    def test_add_node_without_datacenter_infers_from_existing_nodes(self):
        """
        Test that adding a node without specifying data_center infers it from existing nodes.
        
        This reproduces the issue described in:
        "Can't add new node to the cluster if I don't specify data-center explicitly."
        
        The fix makes the add() method look at existing nodes and use their datacenter
        when data_center is not specified, ensuring __update_topology_files() is called.
        """
        # Create a minimal cluster instance with mocked methods
        temp_dir = tempfile.mkdtemp(prefix='ccm_test_')
        cluster_name = 'test_cluster'
        
        try:
            # Create a cluster with minimal initialization
            cluster = object.__new__(Cluster)
            cluster.name = cluster_name
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
            
            # First, add an existing node with datacenter
            existing_node = Mock(spec=Node)
            existing_node.name = 'node1'
            existing_node.data_center = 'dc1'
            existing_node.rack = 'RAC1'
            existing_node._save = Mock()
            existing_node.set_log_level = Mock()
            
            # Add the first node
            with patch.object(cluster, '_Cluster__update_topology_files'):
                cluster.add(existing_node, is_seed=True, data_center='dc1', rack='RAC1')
            
            # Now add a second node WITHOUT specifying data_center
            new_node = Mock(spec=Node)
            new_node.name = 'node2'
            new_node.data_center = None
            new_node.rack = None
            new_node._save = Mock()
            new_node.set_log_level = Mock()
            
            # Mock __update_topology_files to track if it's called
            with patch.object(cluster, '_Cluster__update_topology_files') as mock_update_topology:
                # Add node without specifying data_center (data_center=None)
                cluster.add(new_node, is_seed=True, data_center=None)
                
                # The new node should have inherited dc1 from the existing node
                assert new_node.data_center == 'dc1', \
                    f"Expected node to inherit dc1, got {new_node.data_center}"
                assert new_node.rack == 'RAC1', \
                    f"Expected node to inherit RAC1, got {new_node.rack}"
                
                # __update_topology_files() should be called since datacenter was inferred
                mock_update_topology.assert_called_once()
                
        finally:
            # Clean up the temporary directory
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

    def test_add_first_node_without_datacenter(self):
        """
        Test that adding the first node without specifying data_center doesn't call
        __update_topology_files() since there's no datacenter to infer.
        
        This is expected behavior - the first node needs an explicit datacenter,
        or it will be handled by __update_topology_using_rackdc_properties() 
        which defaults to dc1.
        """
        # Create a minimal cluster instance
        temp_dir = tempfile.mkdtemp(prefix='ccm_test_')
        cluster_name = 'test_cluster'
        
        try:
            # Create a cluster with minimal initialization
            cluster = object.__new__(Cluster)
            cluster.name = cluster_name
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
            
            # Create the first node without datacenter
            node = Mock(spec=Node)
            node.name = 'node1'
            node.data_center = None
            node.rack = None
            node._save = Mock()
            node.set_log_level = Mock()
            
            # Mock __update_topology_files to track if it's called
            with patch.object(cluster, '_Cluster__update_topology_files') as mock_update_topology:
                # Add first node without specifying data_center
                cluster.add(node, is_seed=True, data_center=None)
                
                # The node should still have None datacenter (no nodes to infer from)
                assert node.data_center is None
                
                # __update_topology_files() should NOT be called for None datacenter
                mock_update_topology.assert_not_called()
                
        finally:
            # Clean up the temporary directory
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

    def test_add_node_with_explicit_datacenter_calls_update_topology(self):
        """
        Test that adding a node WITH specifying data_center calls __update_topology_files().
        This is the workaround mentioned in the issue that currently works.
        """
        # Create a minimal cluster instance
        temp_dir = tempfile.mkdtemp(prefix='ccm_test_')
        cluster_name = 'test_cluster'
        
        try:
            # Create a cluster with minimal initialization
            cluster = object.__new__(Cluster)
            cluster.name = cluster_name
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
            
            # Create a mock node
            node = Mock(spec=Node)
            node.name = 'node1'
            node.data_center = 'dc1'
            node.rack = 'RAC1'
            node._save = Mock()
            node.set_log_level = Mock()
            
            # Mock __update_topology_files to track if it's called
            with patch.object(cluster, '_Cluster__update_topology_files') as mock_update_topology:
                # Add node WITH specifying data_center
                cluster.add(node, is_seed=True, data_center='dc1')
                
                # This should call __update_topology_files() - this currently works
                mock_update_topology.assert_called_once()
                
        finally:
            # Clean up the temporary directory
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)


