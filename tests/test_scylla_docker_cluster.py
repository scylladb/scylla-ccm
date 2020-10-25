class TestScyllaDockerCluster:
    def test_01(self, docker_cluster):
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
