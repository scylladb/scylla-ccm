# ccm clusters
import os
import time

from ccmlib import common
from ccmlib.cluster import Cluster
from ccmlib.scylla_node import ScyllaNode
from ccmlib.node import NodeError

SNITCH = 'org.apache.cassandra.locator.GossipingPropertyFileSnitch'


class ScyllaCluster(Cluster):

    def __init__(self, path, name, partitioner=None, install_dir=None,
                 create_directory=True, version=None, verbose=False, **kwargs):
        install_func = common.scylla_extract_install_dir_and_mode
        install_dir, self.scylla_mode = install_func(install_dir)
        super(ScyllaCluster, self).__init__(path, name, partitioner,
                                            install_dir, create_directory,
                                            version, verbose,
                                            snitch=SNITCH)

    def load_from_repository(self, version, verbose):
        raise NotImplementedError('ScyllaCluster.load_from_repository')

    def create_node(self, name, auto_bootstrap, thrift_interface,
                    storage_interface, jmx_port, remote_debug_port,
                    initial_token, save=True, binary_interface=None):
        return ScyllaNode(name, self, auto_bootstrap, thrift_interface,
                          storage_interface, jmx_port, remote_debug_port,
                          initial_token, save, binary_interface)

    # copy from cluster
    def __update_pids(self, started):
        for node, p, _ in started:
            node._update_pid(p)

    # override cluster
    def start(self, no_wait=False, verbose=False, wait_for_binary_proto=False,
              wait_other_notice=False, jvm_args=None, profile_options=None,
              quiet_start=False):
        p = None
        if jvm_args is None:
            jvm_args = []

        marks = []
        if wait_other_notice:
            marks = [(node, node.mark_log()) for node in self.nodes.values()]

        started = []
        for node in self.nodes.values():
            if not node.is_running():
                mark = 0
                if os.path.exists(node.logfilename()):
                    mark = node.mark_log()

                p = node.start(update_pid=False, jvm_args=jvm_args,
                               profile_options=profile_options)
                started.append((node, p, mark))

        if no_wait and not verbose:
            # waiting 2 seconds to check for early errors and for the
            # pid to be set
            time.sleep(2)
        else:
            for node, p, mark in started:
                start_message = "Starting listening for CQL clients"
                try:
                    # updated code, scylla starts CQL only by default
                    # process should not be checked for scylla as the
                    # process is a boot script (that ends after boot)
                    node.watch_log_for(start_message, timeout=300,
                                       verbose=verbose, from_mark=mark)
                except RuntimeError:
                    raise Exception("Not able to find start "
                                    "message '%s' in Node '%s'" %
                                    (start_message, node.name))

        self.__update_pids(started)

        for node, p, _ in started:
            if not node.is_running():
                raise NodeError("Error starting {0}.".format(node.name), p)

        if not no_wait and self.cassandra_version() >= "0.8":
            # 0.7 gossip messages seems less predictable that from 0.8
            # onwards and I don't care enough
            for node, _, mark in started:
                for other_node, _, _ in started:
                    if other_node is not node:
                        node.watch_log_for_alive(other_node, from_mark=mark)

        if wait_other_notice:
            for old_node, mark in marks:
                for node, _, _ in started:
                    if old_node is not node:
                        old_node.watch_log_for_alive(node, from_mark=mark)

        if wait_for_binary_proto and self.version() >= '1.2':
            for node, _, mark in started:
                node.watch_log_for("Starting listening for CQL clients",
                                   verbose=verbose, from_mark=mark)
            time.sleep(0.2)

        return started

    def cassandra_version(self):
        # TODO: Handle versioning
        return '2.1'

    def get_scylla_mode(self):
        return self.scylla_mode
