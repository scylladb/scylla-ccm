# ccm clusters
import os
import shutil
import subprocess
import signal
import time

from six import iteritems
from ccmlib import repository
from ccmlib.cluster import Cluster
from ccmlib.urchin_node import UrchinNode
from ccmlib import common

class UrchinCluster(Cluster):
    def __init__(self, path, name, partitioner=None, install_dir=None, create_directory=True, version=None, verbose=False, **kwargs):
        install_dir, self.urchin_mode=common.urchin_extract_install_dir_and_mode(install_dir)
        super(UrchinCluster, self).__init__(path, name, partitioner, install_dir, create_directory, version, verbose)

    def load_from_repository(self, version, verbose):
        raise Exception("no impl");
#        return repository.setup_dse(version, self.dse_username, self.dse_password, verbose)

    def create_node(self, name, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save=True, binary_interface=None):
        return UrchinNode(name, self, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save, binary_interface)

    # copy from cluster
    def __update_pids(self, started):
        for node, p, _ in started:
            node._update_pid(p)
    
    # overidr cluster 
    def start(self, no_wait=False, verbose=False, wait_for_binary_proto=False, wait_other_notice=False, jvm_args=[], profile_options=None):
        if wait_other_notice:
            marks = [(node, node.mark_log()) for node in list(self.nodes.values())]

        started = []
        for node in list(self.nodes.values()):
            if not node.is_running():
                mark = 0
                if os.path.exists(node.logfilename()):
                    mark = node.mark_log()

                p = node.start(update_pid=False, jvm_args=jvm_args, profile_options=profile_options)
                started.append((node, p, mark))

        if no_wait and not verbose:
            time.sleep(2) # waiting 2 seconds to check for early errors and for the pid to be set
        else:
            for node, p, mark in started:
                try:
                    # updated code, urchin starts CQL only by default
                    # process should not be checked for urchin as the process is a bootup script (that ends after boot)
                    start_message = "Starting listening for CQL clients"
                    node.watch_log_for(start_message, timeout=300, verbose=verbose, from_mark=mark)
                except RuntimeError:
                    raise Exception("node %s not able to find start message %s" % (node.name, start_message))

        self.__update_pids(started)

        for node, p, _ in started:
            if not node.is_running():
                raise NodeError("Error starting {0}.".format(node.name), p)

        if not no_wait and self.cassandra_version() >= "0.8":
            # 0.7 gossip messages seems less predictible that from 0.8 onwards and
            # I don't care enough
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
                node.watch_log_for("Starting listening for CQL clients", process=p, verbose=verbose, from_mark=mark)
            time.sleep(0.2)

        return started


    def cassandra_version(self):
        # FIXME
        return '2.1'

    def get_urchin_mode(self):
        return self.urchin_mode;
