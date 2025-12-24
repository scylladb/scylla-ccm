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

    def test_add_node_without_datacenter_calls_update_topology(self):
        """
        Test that adding a node without specifying data_center still calls
        __update_topology_files() to create proper topology configuration.
        
        This reproduces the issue described in:
        "Can't add new node to the cluster if I don't specify data-center explicitly."
        
        The bug is in cluster.py:add() method at line 244:
        
        ```python
        if data_center is not None:
            self.debug(...)
            self.__update_topology_files()
        ```
        
        This means when data_center=None, __update_topology_files() is never called,
        so the cassandra-rackdc.properties file doesn't get written with default values.
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
            
            # Create a mock node
            node = Mock(spec=Node)
            node.name = 'node1'
            node.data_center = None
            node.rack = None
            node._save = Mock()
            node.set_log_level = Mock()
            
            # Mock __update_topology_files to track if it's called
            with patch.object(cluster, '_Cluster__update_topology_files') as mock_update_topology:
                # Add node without specifying data_center (data_center=None)
                cluster.add(node, is_seed=True, data_center=None)
                
                # The issue is that __update_topology_files() is NOT called when data_center=None
                # This test will FAIL initially, confirming the bug exists
                # After the fix, it should PASS
                mock_update_topology.assert_called_once()
                
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


