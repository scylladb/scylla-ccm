import os.path
from subprocess import run
from unittest import TestCase

from ccmlib.scylla_docker_cluster import ScyllaDockerCluster


class TestScyllaDockerCluster(TestCase):
    def test_01(self):
        test_path = os.path.expanduser("~/.test")
        run(["bash", "-c", f"rm -rf {test_path}/test_cluster01"])
        cluster = ScyllaDockerCluster(test_path, name='test_cluster01',
                                      docker_image='scylladb/scylla-nightly:666.development-0.20201015.8068272b466')
        self.addCleanup(lambda: cluster.clear())
        cluster.populate(3)
        cluster.start(wait_for_binary_proto=True)
        [node1, node2, node3] = cluster.nodelist()

        node1.run_cqlsh(
            '''
            CREATE KEYSPACE ks WITH replication = { 'class' :'SimpleStrategy', 'replication_factor': 3};
            USE ks;
            CREATE TABLE test (key int PRIMARY KEY);
            INSERT INTO test (key) VALUES (1);
            ''')
        rv = node1.run_cqlsh('SELECT * from ks.test', return_output=True)
        for s in ['(1 rows)', 'key', '1']:
            self.assertIn(s, rv[0])
        self.assertEqual(rv[1], '')
