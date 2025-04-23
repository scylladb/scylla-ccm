import os
import re

import pytest
from ccmlib import common

from tests.test_scylla_cmds import cluster_params, copy_cluster_data


@cluster_params
class TestCCMTopology:
    start_params = ["--wait-other-notice", "--wait-for-binary-proto"]

    @staticmethod
    @pytest.fixture(autouse=True)
    def base_setup(request, cluster_under_test):
        try:
            yield
        finally:
            cluster_under_test.run_command(cluster_under_test.get_stop_cmd())
            cluster_under_test.process.wait()
            copy_cluster_data(request)
            cluster_under_test.run_command(cluster_under_test.get_remove_cmd())
            cluster_under_test.process.wait()
            if os.path.exists(cluster_under_test.cluster_dir):
                common.rmdirs(cluster_under_test.cluster_dir)

    def test_topology_single_dc(self, cluster_under_test):
        cluster_under_test.run_command(cluster_under_test.get_create_cmd(["-n", "1"]))
        cluster_under_test.validate_command_result()
        cluster_under_test.run_command(cluster_under_test.get_start_cmd("--wait-other-notice"))
        cluster_under_test.validate_command_result()
        assert_topology(
            cluster_under_test,
            {
                'node1': {'dc': 'dc1', 'rack': 'rac1'}
            })
        cluster_under_test.run_command(cluster_under_test.get_add_cmd("node2", "-j", "7501"))
        cluster_under_test.validate_command_result()
        cluster_under_test.run_command(cluster_under_test.get_node_start_cmd("node2", *self.start_params))
        cluster_under_test.validate_command_result()
        # No DC is provided node is provisioned to the same DC
        # Cluster stays as single DC cluster, `conf/cassandra-rackdc.properties` should be empty for both nodes
        assert_topology(
            cluster_under_test,
            {
                'node1': {'dc': 'dc1', 'rack': 'rac1'},
                'node2': {'dc': 'dc1', 'rack': 'rac1'}
            })

    def test_topology_multi_dc(self, cluster_under_test):
        cluster_under_test.run_command(cluster_under_test.get_create_cmd(["-n", "1:0"]))
        cluster_under_test.validate_command_result()
        cluster_under_test.run_command(cluster_under_test.get_start_cmd("--wait-other-notice"))
        cluster_under_test.validate_command_result()
        # Since it is single DC cluster `conf/cassandra-rackdc.properties` should be empty
        assert_topology(
            cluster_under_test,
            {
                'node1': {'dc': 'dc1', 'rack': 'rac1'}
            })
        cluster_under_test.run_command(cluster_under_test.get_add_cmd("node2", "--rack", "rac2", "-j", "7501"))
        cluster_under_test.validate_command_result()
        cluster_under_test.run_command(cluster_under_test.get_node_start_cmd("node2", *self.start_params))
        cluster_under_test.validate_command_result()
        # No DC is provided node is provisioned to the same DC
        # Cluster stays as single DC cluster, `conf/cassandra-rackdc.properties` should be empty for both nodes
        assert_topology(
            cluster_under_test,
            {
                'node1': {'dc': 'dc1', 'rack': 'rac1'},
                'node2': {'dc': 'dc1', 'rack': 'rac2'}
            })
        cluster_under_test.run_command(cluster_under_test.get_add_cmd("node3", "--data-center", "dc2", "-j", "7502"))
        cluster_under_test.validate_command_result()
        cluster_under_test.run_command(cluster_under_test.get_node_start_cmd("node3", *self.start_params))
        cluster_under_test.validate_command_result()
        # DC2 is specifically targeted
        # Cluster switches to multi DC cluster, `conf/cassandra-rackdc.properties` should be populated with RACK and DC
        assert_topology(
            cluster_under_test,
            {
                'node1': {'dc': 'dc1', 'rack': 'rac1'},
                'node2': {'dc': 'dc1', 'rack': 'rac2'},
                'node3': {'dc': 'dc2', 'rack': 'rac1'}
            })
        cluster_under_test.run_command(cluster_under_test.get_add_cmd("node4", "--data-center", "dc2", "--rack", "rac2", "-j", "7503"))
        cluster_under_test.validate_command_result()
        cluster_under_test.run_command(cluster_under_test.get_node_start_cmd("node4", *self.start_params))
        cluster_under_test.validate_command_result()
        # DC2 is specifically targeted
        # Cluster switches to multi DC cluster, `conf/cassandra-rackdc.properties` should be populated with RACK and DC
        assert_topology(
            cluster_under_test,
            {
                'node1': {'dc': 'dc1', 'rack': 'rac1'},
                'node2': {'dc': 'dc1', 'rack': 'rac2'},
                'node3': {'dc': 'dc2', 'rack': 'rac1'},
                'node4': {'dc': 'dc2', 'rack': 'rac2'}
            })

    def test_topology_no_populate_multi_dc(self, cluster_under_test):
        cluster_under_test.run_command(cluster_under_test.get_create_cmd())
        cluster_under_test.validate_command_result()
        # Since it is single DC cluster `conf/cassandra-rackdc.properties` should be empty
        cluster_under_test.run_command(cluster_under_test.get_add_cmd("node1"))
        cluster_under_test.validate_command_result()
        cluster_under_test.run_command(cluster_under_test.get_add_cmd("node2", "--rack", "rac2", "-j", "7501"))
        cluster_under_test.validate_command_result()
        cluster_under_test.run_command(cluster_under_test.get_add_cmd("node3", "--data-center", "dc2", "-j", "7502"))
        cluster_under_test.validate_command_result()
        cluster_under_test.run_command(cluster_under_test.get_add_cmd("node4", "--data-center", "dc2", "--rack", "rac2", "-j", "7503"))
        cluster_under_test.validate_command_result()
        cluster_under_test.run_command(cluster_under_test.get_start_cmd("--wait-other-notice"))
        cluster_under_test.validate_command_result()
        # DC2 is specifically targeted
        # Cluster switches to multi DC cluster, `conf/cassandra-rackdc.properties` should be populated with RACK and DC
        assert_topology(
            cluster_under_test,
            {
                'node1': {'dc': 'dc1', 'rack': 'rac1'},
                'node2': {'dc': 'dc1', 'rack': 'rac2'},
                'node3': {'dc': 'dc2', 'rack': 'rac1'},
                'node4': {'dc': 'dc2', 'rack': 'rac2'}
            })


def get_all_cassandra_rackdc_properties(cluster):
    res = {}
    for node_name in cluster.get_cluster_config().get("nodes"):
        res[node_name] = cluster.get_nodes_cassandra_rackdc_properties(node_name)
    return res

def get_node_cassandra_topology_properties(ccm_cluster, node_name):
    address_to_name = {}
    for node in ccm_cluster.nodelist():
        address_to_name[node.address()] = node.name

    topology_file_name = os.path.join(ccm_cluster.get_path(), node_name, "conf", "cassandra-topology.properties")
    if not os.path.exists(topology_file_name):
        return []
    with open(topology_file_name) as f:
        res = {}
        for line in f.readlines():
            if line.startswith("#"):
                continue
            chunks = line.strip().split("=", 2)
            if len(chunks) == 2:
                address, dc_rack = chunks
                if address.strip() == "default":
                    continue
                chunks = dc_rack.split(":")
                if len(chunks) == 2:
                    dc, rack = dc_rack.split(":")
                    host_name = address_to_name.get(address)
                    if not host_name:
                        continue
                    res[host_name] = {
                        'dc': dc,
                        'rack': rack,
                    }
        return res


def assert_cassandra_topology_properties(ccm_cluster, expected_topology):
    for node in ccm_cluster.nodelist():
        assert get_node_cassandra_topology_properties(ccm_cluster, node.name) == expected_topology, \
            f"cassandra-topology.properties on {node.name} returned different topology"


def run_cqlsh_and_return(node, query):
    result = []
    is_result = False
    for line in node.run_cqlsh(query, return_output=True)[0].split('\n'):
        if not line:
            continue
        if line.startswith('----'):
            is_result = True
            continue
        if is_result:
            if line.startswith('(') and line.endswith('rows)'):
                break
            result.append([chunk.strip() for chunk in line.split(' | ')])
    return result


def assert_topology_on_cqlsh(ccm_cluster, expected_topology):
    node_id_to_name = {}
    for node in ccm_cluster.nodelist():
        node_id_to_name[node.hostid()] = node.name

    for node in ccm_cluster.nodelist():
        if node.name in expected_topology:
            expected_local = expected_topology.get(node.name)
            row = run_cqlsh_and_return(node, 'select data_center, rack from system.local')
            if not row or not row[0]:
                raise ValueError("query 'select data_center, rack from system.local' returned no results")
            assert {
                "dc": row[0][0],
                "rack": row[0][1],
            } == expected_local, f"node {node.name} `system.local` returned different topology"
        row = run_cqlsh_and_return(node, 'select host_id, data_center, rack from system.peers')
        for rec in row:
            node_name = node_id_to_name.get(rec[0])
            if not node_name:
                raise ValueError(f"node id {rec[0]} is unknown")
            expected_node_topology = expected_topology.get(node_name)
            if expected_node_topology is None:
                continue
            assert {
                "dc": rec[1],
                "rack": rec[2],
            } == expected_node_topology, f"node {node.name} `system.peers` returned different topology"


def get_nodetool_topology(node, node_id_to_name):
    topology = {}
    out = node.nodetool("status")
    is_rows = False
    dc = None
    rack_header_id = None
    host_header_id = None
    for line in out[0].split('\n'):
        if line.startswith('Datacenter:'):
            chunks = line.split()
            dc = chunks[1] if len(chunks) > 1 else None
        if line.startswith('--'):
            # Reverse line, pattern breaks in some nodetool versions, but ends stays similar
            line = rev(line.lower())
            is_rows = True
            header_id = 0
            rack_header_id = None
            host_header_id = None
            for header in [x.group() for x in re.finditer(r'(di tsoh|\w+)', line)]:
                header = rev(header.strip())
                if header.lower() == 'rack':
                    rack_header_id = header_id
                if header.lower() == 'host id':
                    host_header_id = header_id
                header_id += 1
            if rack_header_id is None:
                raise ValueError(f"rack header not found in header line: {line}")
            if host_header_id is None:
                raise ValueError(f"host id not found in header line: {line}")
            continue
        if not is_rows:
            continue
        if not line.strip():
            dc = None
            is_rows = False
            continue
        chunks = rev(line).split()
        if len(chunks) < 3:
            continue
        host_id, rack = rev(chunks[host_header_id]), rev(chunks[rack_header_id])
        node_name = node_id_to_name.get(host_id)
        if not node_name:
            raise ValueError(f"node id {host_id} is unknown")
        topology[node_name] = {
            "dc": dc,
            "rack": rack,
        }
    return topology


def assert_topology_on_nodetool(ccm_cluster, expected_topology):
    node_id_to_name = {}
    for node in ccm_cluster.nodelist():
        node_id_to_name[node.hostid()] = node.name

    for node in ccm_cluster.nodelist():
        assert get_nodetool_topology(node, node_id_to_name) == expected_topology, f"nodetool status on {node.name} returned different topology"


def assert_topology(cluster, expected_topology):
    ccm_cluster = cluster.get_ccm_cluster()
    if ccm_cluster.snitch == 'org.apache.cassandra.locator.PropertyFileSnitch':
        assert_cassandra_topology_properties(ccm_cluster, expected_topology)
    elif ccm_cluster.snitch == 'org.apache.cassandra.locator.GossipingPropertyFileSnitch':
        assert get_all_cassandra_rackdc_properties(
            cluster) == expected_topology, "cassandra-rackdc.properties contains different topology"
    if cluster.use_scylla:
        # Cassandra 3.x cqlsh is broken on modern python
        assert_topology_on_cqlsh(ccm_cluster, expected_topology)
    assert_topology_on_nodetool(ccm_cluster, expected_topology)

def rev(line):
    return ''.join(reversed(line))