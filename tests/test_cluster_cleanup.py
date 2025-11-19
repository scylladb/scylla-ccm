"""Tests for cluster_cleanup function."""
import pytest
from unittest.mock import Mock, MagicMock, patch
from ccmlib.cluster import Cluster


class TestClusterCleanup:
    """Test suite for the cluster_cleanup method."""

    def test_cluster_cleanup_exists(self):
        """Test that cluster_cleanup method exists on Cluster class."""
        assert hasattr(Cluster, 'cluster_cleanup')
        assert callable(getattr(Cluster, 'cluster_cleanup'))

    def test_cluster_cleanup_calls_nodetool_on_single_node(self):
        """Test that cluster_cleanup calls 'cluster cleanup' on a single running node."""
        # Create a mock cluster
        with patch.object(Cluster, '__init__', lambda x, *args, **kwargs: None):
            cluster = Cluster(None, None)
            cluster.nodes = {}
            
            # Create mock nodes
            mock_node1 = Mock()
            mock_node1.is_running.return_value = True
            mock_node1.nodetool = Mock()
            
            mock_node2 = Mock()
            mock_node2.is_running.return_value = True
            mock_node2.nodetool = Mock()
            
            mock_node3 = Mock()
            mock_node3.is_running.return_value = False
            mock_node3.nodetool = Mock()
            
            cluster.nodes = {
                'node1': mock_node1,
                'node2': mock_node2,
                'node3': mock_node3,
            }
            
            # Call cluster_cleanup
            cluster.cluster_cleanup()
            
            # Verify that nodetool was called with "cluster cleanup" on exactly one node
            total_calls = (mock_node1.nodetool.call_count + 
                          mock_node2.nodetool.call_count + 
                          mock_node3.nodetool.call_count)
            assert total_calls == 1, f"Expected exactly 1 nodetool call, got {total_calls}"
            
            # Verify it was called on a running node
            if mock_node1.nodetool.called:
                mock_node1.nodetool.assert_called_once_with("cluster cleanup")
            elif mock_node2.nodetool.called:
                mock_node2.nodetool.assert_called_once_with("cluster cleanup")
            
            # Verify the stopped node was not called
            assert not mock_node3.nodetool.called

    def test_cluster_cleanup_with_no_running_nodes(self):
        """Test that cluster_cleanup does nothing when no nodes are running."""
        # Create a mock cluster
        with patch.object(Cluster, '__init__', lambda x, *args, **kwargs: None):
            cluster = Cluster(None, None)
            cluster.nodes = {}
            
            # Create mock stopped nodes
            mock_node1 = Mock()
            mock_node1.is_running.return_value = False
            mock_node1.nodetool = Mock()
            
            mock_node2 = Mock()
            mock_node2.is_running.return_value = False
            mock_node2.nodetool = Mock()
            
            cluster.nodes = {
                'node1': mock_node1,
                'node2': mock_node2,
            }
            
            # Call cluster_cleanup
            cluster.cluster_cleanup()
            
            # Verify that nodetool was not called on any node
            assert not mock_node1.nodetool.called
            assert not mock_node2.nodetool.called

    def test_cluster_cleanup_with_empty_cluster(self):
        """Test that cluster_cleanup handles an empty cluster gracefully."""
        # Create a mock cluster with no nodes
        with patch.object(Cluster, '__init__', lambda x, *args, **kwargs: None):
            cluster = Cluster(None, None)
            cluster.nodes = {}
            
            # Call cluster_cleanup - should not raise an exception
            cluster.cluster_cleanup()
