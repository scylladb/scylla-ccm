import os.path
from subprocess import run

import pytest

from ccmlib.scylla_docker_cluster import ScyllaDockerCluster


@pytest.fixture(scope="module")
def test_path():
    p = os.path.expanduser("~/.test")
    yield p
    run(["bash", "-c", f"rm -rf {p}/test_cluster01"])


@pytest.fixture(scope="module")
def docker_cluster(test_path):
    cluster = ScyllaDockerCluster(test_path, name='test_cluster01',
                                  docker_image='scylladb/scylla-nightly:666.development-0.20201015.8068272b466')
    yield cluster

    cluster.clear()


def test_01_cqlsh(docker_cluster):

    docker_cluster.populate(3)
    docker_cluster.start(wait_for_binary_proto=True)
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

