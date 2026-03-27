import re
import pytest


@pytest.mark.docker
class TestScyllaDockerCluster:
    @staticmethod
    def parse_nodetool_status(lines):
        """parse output of nodetool status

        Nodetool status output:
        Datacenter: eu-west
        ===================
        Status=Up/Down
        |/ State=Normal/Leaving/Joining/Moving
        --  Address      Load       Tokens       Owns    Host ID                               Rack
        UN  10.0.15.114  36.53 GB   256          ?       98429fc3-1e89-4029-ac1c-325179752142  1a
        UN  10.0.126.57  88.17 GB   256          ?       aea7e0f2-c2c3-4dc6-8ffd-8eda27f4ab8e  1a
        UN  10.0.74.155  90.62 GB   256          ?       f2df2267-b8d1-4a1b-a5d8-a6c57a289f44  1a
        UN  10.0.65.254  101.39 GB  256          ?       20eca592-3eda-478b-b9c8-03266879b8ba  1a

        Parsed result:
        [
            {"status": "UN", "address": "10.0.15.114", size: "36.53", dimension: KB|MB|GB},
            {"status": "UN", "address": "10.0.126.57", size: "88.17", dimension: KB|MB|GB},
            {"status": "UN", "address": "10.0.74.155", size: "90.62", dimension: KB|MB|GB},
            {"status": "UN", "address": "10.0.65.254", size: "101.39", dimension: KB|MB|GB}

        ]
        """
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

    def test_01_cqlsh(self, docker_cluster):
        [node1, node2, node3] = docker_cluster.nodelist()

        node1.run_cqlsh(
            '''
            CREATE KEYSPACE ks WITH replication = { 'class' :'SimpleStrategy', 'replication_factor': 3};
            USE ks;
            CREATE TABLE test (key int PRIMARY KEY);
            INSERT INTO test (key) VALUES (1);
            ''')
        rv = node1.run_cqlsh('SELECT * from ks.test', return_output=True)
        for s in ['(1 rows)', 'key', '1']:
            assert s in rv[0]
        assert rv[1] == ''

    def test_02_nodetool_status(self, docker_cluster):
        [node1, node2, node3] = docker_cluster.nodelist()
        status, error = node1.nodetool('status')
        assert error == ''
        nodes_statuses = TestScyllaDockerCluster.parse_nodetool_status(status.splitlines())
        assert all(node['status'] == 'UN' for node in nodes_statuses), "Expecting all nodes to be UN, and one or more were not"

    def test_03_stop_ungently(self, docker_cluster):
        [node1, node2, node3] = docker_cluster.nodelist()

        node3.stop(gently=False)
        node3.start()

    def test_node_stress(self, docker_cluster):
        node1, *_ = docker_cluster.nodelist()
        ret = node1.stress(['write', 'n=1000'])
        assert '1,000 [WRITE: 1,000]' in ret.stdout
        assert 'END' in ret.stdout
