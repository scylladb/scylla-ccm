from subprocess import run
from unittest import TestCase


class TestScyllaDockerCluster(TestCase):
    def test_01(self):
        run(["bash", "-c", "rm -rf /home/fruch/.test/fruch01"])
        from ccmlib.scylla_docker_cluster import ScyllaDockerCluster
        cluster = ScyllaDockerCluster('/home/fruch/.test', name='fruch01', docker_image='scylladb/scylla-nightly:666.development-0.20201015.8068272b466')
        cluster.populate(1)
        cluster.start(no_wait=True)
        node1 = cluster.nodes