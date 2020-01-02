# ccm clusters
import os
import shutil
import time
import subprocess
import signal
import yaml
import uuid
import datetime

from six import print_

from ccmlib import common
from ccmlib.cluster import Cluster
from ccmlib.scylla_node import ScyllaNode
from ccmlib.node import NodeError
from ccmlib import scylla_repository

SNITCH = 'org.apache.cassandra.locator.GossipingPropertyFileSnitch'


class ScyllaCluster(Cluster):

    def __init__(self, path, name, partitioner=None, install_dir=None,
                 create_directory=True, version=None, verbose=False,
                 force_wait_for_cluster_start=False, manager=None, **kwargs):
        install_func = common.scylla_extract_install_dir_and_mode

        cassandra_version = kwargs.get('cassandra_version', version)
        if cassandra_version:
            self.scylla_mode = 'reloc'
        else:
            install_dir, self.scylla_mode = install_func(install_dir)
            
        self.started = False
        self.force_wait_for_cluster_start = force_wait_for_cluster_start
        super(ScyllaCluster, self).__init__(path, name, partitioner,
                                            install_dir, create_directory,
                                            version, verbose,
                                            snitch=SNITCH, cassandra_version=cassandra_version)

        self._scylla_manager=None
        if not manager:
            scylla_ext_opts = os.getenv('SCYLLA_EXT_OPTS', "").split()
            opts_i = 0
            while opts_i < len(scylla_ext_opts):
                if scylla_ext_opts[opts_i].startswith("--scylla-manager="):
                   manager = scylla_ext_opts[opts_i].split('=')[1]
                opts_i += 1

        if os.path.exists(os.path.join(self.get_path(), common.SCYLLAMANAGER_DIR)):
            self._scylla_manager = ScyllaManager(self)
        elif manager:
            self._scylla_manager = ScyllaManager(self,manager)

    def load_from_repository(self, version, verbose):
        return scylla_repository.setup(version, verbose)

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
                    node.watch_log_for(start_message, timeout=600,
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

        if self._scylla_manager:
            self._scylla_manager.start()

        return started

    def stop(self, wait=True, gently=True, wait_other_notice=False, other_nodes=None, wait_seconds=127):
        if self._scylla_manager:
            self._scylla_manager.stop(gently)
        Cluster.stop(self,wait,gently,wait_seconds=wait_seconds, wait_other_notice=wait_other_notice, other_nodes=other_nodes)

    def get_scylla_mode(self):
        return self.scylla_mode

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


class ScyllaManager:
    def __init__(self, scylla_cluster, install_dir=None):
        self.scylla_cluster = scylla_cluster
        self._process_scylla_manager = None
        self._pid = None
        self.auth_token = str(uuid.uuid4())

        if install_dir:
            if not os.path.exists(self._get_path()):
                os.mkdir(self._get_path())
            self._install(install_dir)
        else:
            self._update_pid()

    def _install(self, install_dir):
        self._copy_config_files(install_dir)
        self._copy_bin_files(install_dir)
        self._update_config(install_dir)

    def _get_api_address(self):
        return "%s:9090" % self.scylla_cluster.get_node_ip(1)

    def _update_config(self, install_dir=None):
        conf_file = os.path.join(self._get_path(), common.SCYLLAMANAGER_CONF)
        with open(conf_file, 'r') as f:
            data = yaml.safe_load(f)
        data['http'] = self._get_api_address()
        if not 'database' in data:
            data['database'] = {}
        data['database']['hosts'] = [self.scylla_cluster.get_node_ip(1)]
        data['database']['replication_factor'] = 3
        if install_dir:
            data['database']['keyspace_tpl_file'] = os.path.join(install_dir, 'dist', 'etc', 'create_keyspace.cql.tpl')
            data['database']['migrate_dir'] = os.path.join(install_dir, 'schema', 'cql')
        if 'https' in data:
            del data['https']
        if 'tls_cert_file' in data:
            del data['tls_cert_file']
        if 'tls_key_file' in data:
            del data['tls_key_file']
        if not 'logger' in data:
            data['logger'] = {}
        data['logger']['mode'] = 'stderr'
        data['prometheus'] = "{}:56091".format(self.scylla_cluster.get_node_ip(1))
        # Changing port to 56091 since the manager and the first node share the same ip and 56090 is already in use
        # by the first node's manager agent
        if 'ssh' in data:
            del data['ssh']
        with open(conf_file, 'w') as f:
            yaml.safe_dump(data, f, default_flow_style=False)

    def _copy_config_files(self, install_dir):
        conf_dir = os.path.join(install_dir, 'dist', 'etc')
        if not os.path.exists(conf_dir):
            raise Exception("%s is not a valid scylla-manager install dir" % install_dir)
        for name in os.listdir(conf_dir):
            filename = os.path.join(conf_dir, name)
            if os.path.isfile(filename):
                shutil.copy(filename, self._get_path())
        agent_conf = os.path.join(install_dir, 'etc/scylla-manager-agent/scylla-manager-agent.yaml')
        if os.path.exists(agent_conf):
            shutil.copy(agent_conf, self._get_path())

    def _copy_bin_files(self, install_dir):
        os.mkdir(os.path.join(self._get_path(), 'bin'))
        files = ['scylla-manager', 'sctool']
        for name in files:
            src = os.path.join(install_dir, 'usr', 'bin',name)
            if not os.path.exists(src):
               raise Exception("%s not found in scylla-manager install dir" % src)
            shutil.copy(src,
                        os.path.join(self._get_path(), 'bin', name))

        agent_bin = os.path.join(install_dir, 'usr', 'bin', 'scylla-manager-agent')
        if os.path.exists(agent_bin):
            shutil.copy(agent_bin, os.path.join(self._get_path(), 'bin', 'scylla-manager-agent'))

    @property
    def is_agent_available(self):
        return os.path.exists(os.path.join(self._get_path(), 'bin', 'scylla-manager-agent'))

    def _get_path(self):
        return os.path.join(self.scylla_cluster.get_path(), common.SCYLLAMANAGER_DIR)

    def _get_pid_file(self):
        return os.path.join(self._get_path(), "scylla-manager.pid")

    def _update_pid(self):
        if not os.path.isfile(self._get_pid_file()):
            return

        start = time.time()
        pidfile = self._get_pid_file()
        while not (os.path.isfile(pidfile) and os.stat(pidfile).st_size > 0):
            if time.time() - start > 30.0:
                print_("Timed out waiting for pidfile {} to be filled (current time is %s): File {} size={}".format(
                        pidfile,
                        datetime.now(),
                        'exists' if os.path.isfile(pidfile) else 'does not exist' if not os.path.exists(pidfile) else 'is not a file',
                        os.stat(pidfile).st_size if os.path.exists(pidfile) else -1))
                break
            else:
                time.sleep(0.1)

        try:
            with open(self._get_pid_file(), 'r') as f:
                self._pid = int(f.readline().strip())
        except IOError as e:
            raise NodeError('Problem starting scylla-manager due to %s' %
                            (e))

    def start(self):
        # some configurations are set post initialisation (cluster id) so
        # we are forced to update the config prior to calling start
        self._update_config()
        # check process is not running
        if self._pid:
            try:
                os.kill(self._pid, 0)
                return
            except OSError as err:
                pass

        log_file = os.path.join(self._get_path(),'scylla-manager.log')
        scylla_log = open(log_file, 'a')

        if os.path.isfile(self._get_pid_file()):
            os.remove(self._get_pid_file())

        args = [os.path.join(self._get_path(), 'bin', 'scylla-manager'),
                '--config-file', os.path.join(self._get_path(), 'scylla-manager.yaml')]
        self._process_scylla_manager = subprocess.Popen(args, stdout=scylla_log,
                                                stderr=scylla_log,
                                                close_fds=True)
        self._process_scylla_manager.poll()
        with open(self._get_pid_file(), 'w') as pid_file:
            pid_file.write(str(self._process_scylla_manager.pid))

        api_interface = common.parse_interface(self._get_api_address(), 9090)
        if not common.check_socket_listening(api_interface,timeout=180):
            raise Exception("scylla manager interface %s:%s is not listening after 180 seconds, scylla manager may have failed to start."
                          % (api_interface[0], api_interface[1]))

        return self._process_scylla_manager

    def stop(self, gently):
        if self._process_scylla_manager:
            if gently:
                try:
                    self._process_scylla_manager.terminate()
                except OSError as e:
                    pass
            else:
                try:
                    self._process_scylla_manager.kill()
                except OSError as e:
                    pass
        else:
            if self._pid:
                signal_mapping = {True: signal.SIGTERM, False: signal.SIGKILL}
                try:
                    os.kill(self._pid, signal_mapping[gently])
                except OSError:
                    pass

    def sctool(self, cmd):
        sctool = os.path.join(self._get_path(), 'bin', 'sctool')
        args = [sctool, '--api-url', "http://%s/api/v1" % self._get_api_address()]
        args += cmd
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        stdout, stderr = p.communicate()
        exit_status = p.wait()
        if exit_status != 0:
            raise Exception(" ".join(args), exit_status, stdout, stderr)
        return stdout, stderr
