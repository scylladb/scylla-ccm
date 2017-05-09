# ccm clusters
import os
import shutil
import time

from ccmlib import common
from ccmlib.cluster import Cluster
from ccmlib.scylla_node import ScyllaNode
from ccmlib.node import NodeError

SNITCH = 'org.apache.cassandra.locator.GossipingPropertyFileSnitch'


class ScyllaCluster(Cluster):

    def __init__(self, path, name, partitioner=None, install_dir=None,
                 create_directory=True, version=None, verbose=False,
                 force_wait_for_cluster_start=False, **kwargs):
        install_func = common.scylla_extract_install_dir_and_mode
        install_dir, self.scylla_mode = install_func(install_dir)
        self.started = False
        self.force_wait_for_cluster_start = force_wait_for_cluster_start
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
        if not self.started and self.force_wait_for_cluster_start:
            wait_other_notice=True
            wait_for_binary_proto=True
        self.started=True

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
                # Let's ensure the nodes start at different times to avoid
                # race conditions while creating system tables
                time.sleep(1)
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

    def version(self):
        return self.cassandra_version()

    def cassandra_version(self):
        # TODO: Handle versioning
        # Return 2.2 as it changes some option values for tools. 
        # Our tools are actually at 3.x level (-ish), but otoh
        # the server is more or less 2.1-2.2
        return '3.0'

    def get_scylla_mode(self):
        return self.scylla_mode

    def enable_internode_ssl(self, node_ssl_path):
        shutil.copyfile(os.path.join(node_ssl_path, 'trust.pem'), os.path.join(self.get_path(), 'internode-trust.pem'))
        shutil.copyfile(os.path.join(node_ssl_path, 'ccm_node.pem'), os.path.join(self.get_path(), 'internode-ccm_node.pem'))
        shutil.copyfile(os.path.join(node_ssl_path, 'ccm_node.key'), os.path.join(self.get_path(), 'internode-ccm_node.key'))
        node_ssl_options = {
            'internode_encryption': 'all',
            'certificate': os.path.join(self.get_path(), 'internode-ccm_node.pem'),
            'keyfile': os.path.join(self.get_path(), 'internode-ccm_node.key'),
            'truststore': os.path.join(self.get_path(), 'internode-trust.pem'),
        }

        self._config_options['server_encryption_options'] = node_ssl_options
        self._update_config()
