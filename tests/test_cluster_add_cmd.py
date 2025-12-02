"""Tests for ClusterAddCmd JMX port conflict check."""
import pytest
from unittest.mock import MagicMock, patch


class TestClusterAddCmdJmxPortConflict:
    """Tests for the JMX port conflict check in ClusterAddCmd.validate()."""

    @pytest.fixture
    def mock_cluster(self):
        """Create a mock cluster with two nodes using the same JMX port."""
        cluster = MagicMock()
        cluster.get_node_ip.return_value = "127.0.3.1"
        cluster.get_node_jmx_port.return_value = 7199  # Same JMX port for all nodes (Scylla default)
        
        # Create mock nodes with JMX port 7199
        node1 = MagicMock()
        node1.jmx_port = 7199
        node1.network_interfaces = {
            'binary': ('127.0.1.1', 9042),
            'storage': ('127.0.1.1', 7000),
        }
        
        node2 = MagicMock()
        node2.jmx_port = 7199
        node2.network_interfaces = {
            'binary': ('127.0.2.1', 9042),
            'storage': ('127.0.2.1', 7000),
        }
        
        cluster.nodelist.return_value = [node1, node2]
        return cluster

    @pytest.fixture
    def mock_options(self):
        """Create mock options for the add command."""
        options = MagicMock()
        options.config_dir = "/tmp/test_ccm"
        options.itfs = "127.0.3.1"
        options.storage_itf = None
        options.binary_itf = None
        options.jmx_port = None  # Will be auto-assigned
        options.remote_debug_port = "2000"
        options.initial_token = None
        options.scylla_node = False
        options.dse_node = False
        options.bootstrap = False
        options.is_seed = False
        options.data_center = None
        options.rack = None
        return options

    def test_cassandra_node_jmx_port_conflict_exits(self, mock_cluster, mock_options):
        """
        Test that adding a Cassandra node with conflicting JMX port fails.
        
        For Cassandra nodes, JMX ports must be unique per node since all nodes
        typically run on the same host. When a conflicting port is detected,
        the command should exit with an error.
        """
        from ccmlib.cmds.cluster_cmds import ClusterAddCmd
        
        cmd = ClusterAddCmd()
        cmd.cluster = mock_cluster
        cmd.path = "/tmp/test_ccm"
        cmd.name = "node3"
        
        # For Cassandra node (scylla_node=False), JMX port conflict should cause exit
        mock_options.scylla_node = False
        
        parser = MagicMock()
        
        with patch('ccmlib.cmds.cluster_cmds.common.current_cluster_name', return_value='test_cluster'):
            with patch('ccmlib.cmds.cluster_cmds.ClusterFactory.load', return_value=mock_cluster):
                with pytest.raises(SystemExit) as exc_info:
                    cmd.validate(parser, mock_options, ['node3'])
                
                assert exc_info.value.code == 1

    def test_scylla_node_jmx_port_conflict_allowed(self, mock_cluster, mock_options):
        """
        Test that adding a Scylla node with same JMX port succeeds.
        
        For Scylla nodes, each node runs on a unique IP address (e.g., 127.0.1.1,
        127.0.2.1, 127.0.3.1), so they can all use the same JMX port (7199)
        without conflict. The JMX port conflict check should be skipped.
        
        This is the bug fix for: "Cannot add more than 1 additional node due to
        'This JMX port is already in use.'"
        """
        from ccmlib.cmds.cluster_cmds import ClusterAddCmd
        
        cmd = ClusterAddCmd()
        cmd.cluster = mock_cluster
        cmd.path = "/tmp/test_ccm"
        cmd.name = "node3"
        
        # For Scylla node (scylla_node=True), JMX port conflict check should be skipped
        mock_options.scylla_node = True
        
        parser = MagicMock()
        
        with patch('ccmlib.cmds.cluster_cmds.common.current_cluster_name', return_value='test_cluster'):
            with patch('ccmlib.cmds.cluster_cmds.ClusterFactory.load', return_value=mock_cluster):
                # Should NOT raise SystemExit - the port conflict check is skipped
                cmd.validate(parser, mock_options, ['node3'])
                
                # Verify that the JMX port was assigned (7199 from get_node_jmx_port)
                assert cmd.jmx_port == 7199

    def test_scylla_node_explicit_jmx_port_allowed(self, mock_cluster, mock_options):
        """
        Test that Scylla node with explicitly specified conflicting JMX port succeeds.
        
        Even when a user explicitly specifies a JMX port that conflicts with
        existing nodes, it should be allowed for Scylla nodes.
        """
        from ccmlib.cmds.cluster_cmds import ClusterAddCmd
        
        cmd = ClusterAddCmd()
        cmd.cluster = mock_cluster
        cmd.path = "/tmp/test_ccm"
        cmd.name = "node3"
        
        # Set explicit JMX port that conflicts
        mock_options.jmx_port = 7199
        mock_options.scylla_node = True
        
        parser = MagicMock()
        
        with patch('ccmlib.cmds.cluster_cmds.common.current_cluster_name', return_value='test_cluster'):
            with patch('ccmlib.cmds.cluster_cmds.ClusterFactory.load', return_value=mock_cluster):
                # Should NOT raise SystemExit
                cmd.validate(parser, mock_options, ['node3'])
                
                assert cmd.jmx_port == 7199
