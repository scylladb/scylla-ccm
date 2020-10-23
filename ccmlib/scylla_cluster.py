# ccm clusters
import os
import shutil

from ccmlib import common
from ccmlib.cluster import Cluster
from ccmlib.scylla_node import ScyllaNode
from ccmlib.node import NodeError
from ccmlib.scylla_manager import ScyllaManager, ScyllaManagerDocker
from ccmlib import scylla_repository

SNITCH = 'org.apache.cassandra.locator.GossipingPropertyFileSnitch'


class ScyllaCluster(Cluster):

    def __init__(self, path, name, partitioner=None, install_dir=None,
                 create_directory=True, version=None, verbose=False,
                 force_wait_for_cluster_start=False, manager_install_dir=None, **kwargs):
        install_func = common.scylla_extract_install_dir_and_mode

        cassandra_version = kwargs.get('cassandra_version', version)
        if cassandra_version:
            self.scylla_reloc = True
            self.scylla_mode = None
        else:
            self.scylla_reloc = False
            install_dir, self.scylla_mode = install_func(install_dir)

        self.started = False
        self.force_wait_for_cluster_start = (force_wait_for_cluster_start != False)
        super(ScyllaCluster, self).__init__(path, name, partitioner,
                                            install_dir, create_directory,
                                            version, verbose,
                                            snitch=SNITCH, cassandra_version=cassandra_version)

        docker_cluster = kwargs.get("docker_cluster", False)

        if self._scylla_manager is None:
            if not manager_install_dir:
                scylla_ext_opts = os.getenv('SCYLLA_EXT_OPTS', "").split()
                opts_i = 0
                while opts_i < len(scylla_ext_opts):
                    if scylla_ext_opts[opts_i].startswith("--scylla-manager="):
                        manager_install_dir = scylla_ext_opts[opts_i].split('=')[1]
                    opts_i += 1

            if os.path.exists(os.path.join(self.get_path(), common.SCYLLAMANAGER_DIR)):
                if docker_cluster:
                    self._scylla_manager = ScyllaManagerDocker(self)
                else:
                    self._scylla_manager = ScyllaManager(self)
            elif manager_install_dir:
                if docker_cluster:
                    self._scylla_manager = ScyllaManagerDocker(self, manager_install_dir)
                else:
                    self._scylla_manager = ScyllaManager(self, manager_install_dir)

    def load_from_repository(self, version, verbose):
        install_dir, version = scylla_repository.setup(version, verbose)
        install_dir, self.scylla_mode = common.scylla_extract_install_dir_and_mode(install_dir)
        return install_dir, version

    def create_node(self, name, auto_bootstrap, thrift_interface,
                    storage_interface, jmx_port, remote_debug_port,
                    initial_token, save=True, binary_interface=None):
        return ScyllaNode(name, self, auto_bootstrap, thrift_interface,
                          storage_interface, jmx_port, remote_debug_port,
                          initial_token, save, binary_interface, scylla_manager=self._scylla_manager)

    # copy from cluster
    def __update_pids(self, started):
        for node, p, _ in started:
            node._update_pid(p)

    def start_nodes(self, nodes=None, no_wait=True, verbose=False, wait_for_binary_proto=None,
              wait_other_notice=None, jvm_args=None, profile_options=None,
              quiet_start=False):
        if wait_for_binary_proto is None:
            wait_for_binary_proto = self.force_wait_for_cluster_start
        if wait_other_notice is None:
            wait_other_notice = self.force_wait_for_cluster_start
        self.started=True

        p = None
        if jvm_args is None:
            jvm_args = []

        marks = []
        if wait_other_notice:
            marks = [(node, node.mark_log()) for node in self.nodes.values() if node.is_running()]

        if nodes is None:
            nodes = self.nodes.values()
        elif isinstance(nodes, ScyllaNode):
            nodes = [nodes]

        started = []
        for node in nodes:
            if not node.is_running():
                if started:
                    last_node, _, last_mark = started[-1]
                    last_node.watch_log_for("node is now in normal status|Starting listening for CQL clients",
                                            verbose=verbose, from_mark=last_mark)
                mark = 0
                if os.path.exists(node.logfilename()):
                    mark = node.mark_log()

                p = node.start(update_pid=False, jvm_args=jvm_args,
                               profile_options=profile_options, no_wait=no_wait)
                started.append((node, p, mark))

        self.__update_pids(started)

        for node, p, _ in started:
            if not node.is_running():
                raise NodeError("Error starting {0}.".format(node.name), p)

        if wait_for_binary_proto:
            for node, _, mark in started:
                node.watch_log_for("Starting listening for CQL clients",
                                   verbose=verbose, from_mark=mark)

        if wait_other_notice:
            for old_node, mark in marks:
                for node, _, _ in started:
                    if old_node is not node:
                        old_node.watch_log_for_alive(node, from_mark=mark)

        return started

    # override cluster
    def start(self, no_wait=True, verbose=False, wait_for_binary_proto=None,
              wait_other_notice=None, jvm_args=None, profile_options=None,
              quiet_start=False):
        args = locals()
        del args['self']
        started = self.start_nodes(**args)
        if self._scylla_manager:
            self._scylla_manager.start()

        return started

    def stop_nodes(self, nodes=None, wait=True, gently=True, wait_other_notice=False, other_nodes=None, wait_seconds=127):
        if nodes is None:
            nodes = self.nodes.values()
        elif isinstance(nodes, ScyllaNode):
            nodes = [nodes]

        marks = []
        if wait_other_notice:
            if not other_nodes:
                other_nodes = [node for node in self.nodes.values() if not node in nodes]
            marks = [(node, node.mark_log()) for node in other_nodes if node.is_live()]

        # stop all nodes in parallel
        stopped = [node for node in nodes if node.is_running()]
        for node in stopped:
            node.do_stop(gently=gently)

        # wait for stopped nodes is needed
        if wait or wait_other_notice:
            for node in stopped:
                node.wait_until_stopped(wait_seconds, marks)

        return [node for node in nodes if not node.is_running()]

    def stop(self, wait=True, gently=True, wait_other_notice=False, other_nodes=None, wait_seconds=127):
        if self._scylla_manager:
            self._scylla_manager.stop(gently)
        args = locals()
        del args['self']
        return self.stop_nodes(**args)

    def get_scylla_mode(self):
        return self.scylla_mode

    def is_scylla_reloc(self):
        return self.scylla_reloc

    def enable_internode_ssl(self, node_ssl_path, internode_encryption='all'):
        shutil.copyfile(os.path.join(node_ssl_path, 'trust.pem'), os.path.join(self.get_path(), 'internode-trust.pem'))
        shutil.copyfile(os.path.join(node_ssl_path, 'ccm_node.pem'), os.path.join(self.get_path(), 'internode-ccm_node.pem'))
        shutil.copyfile(os.path.join(node_ssl_path, 'ccm_node.key'), os.path.join(self.get_path(), 'internode-ccm_node.key'))
        node_ssl_options = {
            'internode_encryption': internode_encryption,
            'certificate': os.path.join(self.get_path(), 'internode-ccm_node.pem'),
            'keyfile': os.path.join(self.get_path(), 'internode-ccm_node.key'),
            'truststore': os.path.join(self.get_path(), 'internode-trust.pem'),
        }

        self._config_options['server_encryption_options'] = node_ssl_options
        self._update_config()

    def sctool(self, cmd):
        if self._scylla_manager == None:
            raise Exception("scylla manager not enabled - sctool command cannot be executed")
        return self._scylla_manager.sctool(cmd)

    def start_scylla_manager(self):
        if not self._scylla_manager:
            return
        self._scylla_manager.start()

    def stop_scylla_manager(self, gently=True):
        if not self._scylla_manager:
            return
        self._scylla_manager.stop(gently)


