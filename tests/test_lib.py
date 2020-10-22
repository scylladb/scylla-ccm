import sys
sys.path = [".."] + sys.path


class TestCCMLib:
    def test_run_simple_load(self, simple_cluster):
        node1, node2, node3 = simple_cluster.nodelist()

        node1.stress(['write', 'n=1000000'])

        simple_cluster.flush()

    def test_restart(self, simple_cluster):

        simple_cluster.stop(wait_other_notice=True)
        simple_cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        simple_cluster.show(True)

    def test_multi_dc(self, multi_dc_cluster):
        dc_list_before_restart = [node.data_center for node in multi_dc_cluster.nodelist()]
        multi_dc_cluster.set_configuration_options(None, None)

        multi_dc_cluster.stop()
        multi_dc_cluster.start()

        dc_list_after_restart = [node.data_center for node in multi_dc_cluster.nodelist()]
        assert dc_list_before_restart == dc_list_after_restart, \
            f"The list of DC'c changed after changed after restart:\nbefore:{dc_list_before_restart}\n" \
            f"after:{dc_list_after_restart}"


class TestNodetool:
    def test_nodetool_cleanup(self, simple_cluster):
        simple_cluster.cleanup()

    def test_nodetool_repair(self, simple_cluster):
        simple_cluster.repair()

    def test_nodetool_flush(self, simple_cluster):
        simple_cluster.flush()

    def test_nodetool_compact(self, simple_cluster):
        simple_cluster.compact()

    def test_nodetool_drain(self, disposable_cluster):
        disposable_cluster.drain()

    def test_remove_nodes(self, disposable_cluster):
        node_list_pre_removal = disposable_cluster.nodelist()

        class FakeNode:
            name = "non-existing node"

        disposable_cluster.remove(FakeNode())
        node_list_post_fake_removal = disposable_cluster.nodelist()
        assert node_list_post_fake_removal == node_list_pre_removal, \
            f"The list of nodes in the cluster changed after removing a nonexistent node:" \
            f"\npre-removal: {node_list_post_fake_removal}\npost-removal: {node_list_post_fake_removal}"

        node_to_be_removed = node_list_pre_removal[0]
        disposable_cluster.remove(node_to_be_removed)
        node_list_post_removal = disposable_cluster.nodelist()
        assert set(node_list_pre_removal) - set(node_list_post_removal) == {node_to_be_removed}, \
            f"The specified node was not removed from the cluster as expected:\n List of nodes pre-removal: " \
            f"{node_list_pre_removal}\nList of nodes post-removal: {node_list_post_removal}"


class TestRunCqlsh:
    def test_run_cqlsh(self, simple_cluster):
        """run_cqlsh works with a simple example input"""
        node = simple_cluster.nodelist()[0]
        node.run_cqlsh(
            '''
            CREATE KEYSPACE ks WITH replication = { 'class' :'SimpleStrategy', 'replication_factor': 1};
            USE ks;
            CREATE TABLE test (key int PRIMARY KEY);
            INSERT INTO test (key) VALUES (1);
            ''')
        rv = node.run_cqlsh('SELECT * from ks.test', return_output=True)
        for string in ['(1 rows)', 'key', '1']:
            assert string in rv[0], f'String {string} not found in cqlsh output: "{rv}"'
        assert not rv[1], f"Second row of cqlsh output is not empty: {rv[1]}"
