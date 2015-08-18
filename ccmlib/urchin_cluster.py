# ccm clusters
import os
import shutil
import subprocess
import signal

from six import iteritems
from ccmlib import repository
from ccmlib.cluster import Cluster
from ccmlib.urchin_node import UrchinNode
from ccmlib import common

class UrchinCluster(Cluster):
    def __init__(self, path, name, partitioner=None, install_dir=None, create_directory=True, version=None, verbose=False):
        install_dir, self.urchin_mode=common.urchin_extract_install_dir_and_mode(install_dir)
        super(UrchinCluster, self).__init__(path, name, partitioner, install_dir, create_directory, version, verbose)

    def load_from_repository(self, version, verbose):
        raise Exception("no impl");
#        return repository.setup_dse(version, self.dse_username, self.dse_password, verbose)

    def create_node(self, name, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save=True, binary_interface=None):
        return UrchinNode(name, self, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save, binary_interface)

    def start(self, no_wait=False, verbose=False, wait_for_binary_proto=False, wait_other_notice=False, jvm_args=[], profile_options=None):
        no_wait=True
        started = super(UrchinCluster, self).start(no_wait, verbose, wait_for_binary_proto, wait_other_notice, jvm_args, profile_options)
        return started

    def cassandra_version(self):
        # FIXME
        return '2.1'

    def get_urchin_mode(self):
        return self.urchin_mode;
