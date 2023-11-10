# ccm clusters
import os
import shutil
import time
import subprocess
import signal
import yaml
import uuid
import datetime

from distutils.version import LooseVersion

from ccmlib import common
from ccmlib.cluster import Cluster
from ccmlib.scylla_node import ScyllaNode
from ccmlib.node import NodeError
from ccmlib import scylla_repository
from ccmlib.utils.sni_proxy import stop_sni_proxy

SNITCH = 'org.apache.cassandra.locator.GossipingPropertyFileSnitch'


class ScyllaCluster(Cluster):

    def __init__(self, path, name, partitioner=None, install_dir=None,
                 create_directory=True, version=None, verbose=False,
                 force_wait_for_cluster_start=False, manager=None, skip_manager_server=False, **kwargs):
        install_func = common.scylla_extract_install_dir_and_mode

        cassandra_version = kwargs.get('cassandra_version', version)
        docker_image = kwargs.get('docker_image')

        if cassandra_version:
            self.scylla_reloc = True
            self.scylla_mode = None
        elif docker_image:
            self.scylla_reloc = False
            self.scylla_mode = None
        else:
            self.scylla_reloc = False
            install_dir, self.scylla_mode = install_func(install_dir)

        self.started = False
        self.force_wait_for_cluster_start = (force_wait_for_cluster_start != False)
        self.__set_default_timeouts()
        self._scylla_manager = None
        self.skip_manager_server = skip_manager_server
        self.scylla_version = cassandra_version

        self.debug(f"ScyllaCluster: cassandra_version={cassandra_version} docker_image={docker_image} install_dir={install_dir} scylla_mode={self.scylla_mode} default_wait_other_notice_timeout={self.default_wait_other_notice_timeout} default_wait_for_binary_proto={self.default_wait_for_binary_proto}")

        super(ScyllaCluster, self).__init__(path, name, partitioner,
                                            install_dir, create_directory,
                                            version, verbose,
                                            snitch=SNITCH, cassandra_version=cassandra_version,
                                            docker_image=docker_image)

        if not manager:
            scylla_ext_opts = os.getenv('SCYLLA_EXT_OPTS', "").split()
            opts_i = 0
            while opts_i < len(scylla_ext_opts):
                if scylla_ext_opts[opts_i].startswith("--scylla-manager="):
                   manager = scylla_ext_opts[opts_i].split('=')[1]
                opts_i += 1

        if manager:
            self._scylla_manager = ScyllaManager(self, manager)

        if os.path.exists(os.path.join(self.get_path(), common.SCYLLAMANAGER_DIR)):
            self._scylla_manager = ScyllaManager(self)

    def load_from_repository(self, version, verbose):
        install_dir, version = scylla_repository.setup(version, verbose)
        install_dir, self.scylla_mode = common.scylla_extract_install_dir_and_mode(install_dir)
        assert self.scylla_mode is not None
        self.__set_default_timeouts()
        self.debug(f"ScyllaCluster: load_from_repository: install_dir={install_dir} scylla_mode={self.scylla_mode} default_wait_other_notice_timeout={self.default_wait_other_notice_timeout} default_wait_for_binary_proto={self.default_wait_for_binary_proto}")
        return install_dir, version

    def __set_default_timeouts(self):
        self.default_wait_other_notice_timeout = 120 if self.scylla_mode != 'debug' else 600
        self.default_wait_for_binary_proto = 420 if self.scylla_mode != 'debug' else 900

    # override get_node_jmx_port for scylla-jmx
    # scylla-jmx listens on the unique node address (127.0.<cluster.id><node.id>)
    # so there's no need to listen on a different port for every jmx instance
    def get_node_jmx_port(self, nodeid):
        return 7199

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

    def start_nodes(self, nodes=None, no_wait=False, verbose=False, wait_for_binary_proto=None,
              wait_other_notice=None, wait_normal_token_owner=None, jvm_args=None, profile_options=None,
              quiet_start=False):
        if wait_for_binary_proto is None:
            wait_for_binary_proto = self.force_wait_for_cluster_start
        if wait_other_notice is None:
            wait_other_notice = self.force_wait_for_cluster_start
        if wait_normal_token_owner is None and wait_other_notice:
            wait_normal_token_owner = True
        self.debug(f"start_nodes: no_wait={no_wait} wait_for_binary_proto={wait_for_binary_proto} wait_other_notice={wait_other_notice} wait_normal_token_owner={wait_normal_token_owner} force_wait_for_cluster_start={self.force_wait_for_cluster_start}")
        self.started=True

        p = None
        if jvm_args is None:
            jvm_args = []

        marks = []
        if wait_other_notice:
            marks = [(node, node.mark_log()) for node in list(self.nodes.values()) if node.is_running()]

        if nodes is None:
            nodes = list(self.nodes.values())
        elif isinstance(nodes, ScyllaNode):
            nodes = [nodes]

        started = []
        for node in nodes:
            if not node.is_running():
                if started:
                    last_node, _, last_mark = started[-1]
                    last_node.watch_log_for("node is now in normal status|Starting listening for CQL clients",
                                            verbose=verbose, from_mark=last_mark,
                                            process=last_node._process_scylla)
                mark = 0
                if os.path.exists(node.logfilename()):
                    mark = node.mark_log()

                p = node.start(update_pid=False, jvm_args=jvm_args,
                               profile_options=profile_options, no_wait=no_wait,
                               wait_for_binary_proto=wait_for_binary_proto,
                               wait_other_notice=wait_other_notice,
                               wait_normal_token_owner=False)
                started.append((node, p, mark))
                marks.append((node, mark))

        self.__update_pids(started)

        for node, p, _ in started:
            if not node.is_running():
                raise NodeError(f"Error starting {node.name}.", p)

        if wait_for_binary_proto:
            for node, _, mark in started:
                node.watch_log_for("Starting listening for CQL clients",
                                   verbose=verbose, from_mark=mark)

        if wait_other_notice:
            for old_node, _ in marks:
                for node, _, _ in started:
                    if old_node is not node:
                        old_node.watch_rest_for_alive(node, timeout=self.default_wait_other_notice_timeout,
                                                      wait_normal_token_owner=wait_normal_token_owner)

        return started

    # override cluster
    def start(self, no_wait=False, verbose=False, wait_for_binary_proto=None,
              wait_other_notice=None, jvm_args=None, profile_options=None,
              quiet_start=False):
        kwargs = dict(**locals())
        del kwargs['self']
        started = self.start_nodes(**kwargs)
        if self._scylla_manager and not self.skip_manager_server:
            self._scylla_manager.start()

        return started

    def stop_nodes(self, nodes=None, wait=True, gently=True, wait_other_notice=False, other_nodes=None, wait_seconds=None):
        if nodes is None:
            nodes = list(self.nodes.values())
        elif isinstance(nodes, ScyllaNode):
            nodes = [nodes]

        marks = []
        if wait_other_notice:
            if not other_nodes:
                other_nodes = [node for node in list(self.nodes.values()) if not node in nodes]
            marks = [(node, node.mark_log()) for node in other_nodes if node.is_live()]

        # stop all nodes in parallel
        for node in nodes:
            node.do_stop(gently=gently)

        # wait for stopped nodes is needed
        if wait or wait_other_notice:
            for node in nodes:
                node.wait_until_stopped(wait_seconds, marks, dump_core=gently)

        return [node for node in nodes if not node.is_running()]

    def stop(self, wait=True, gently=True, wait_other_notice=False, other_nodes=None, wait_seconds=None):
        if getattr(self, 'sni_proxy_docker_ids', None):
            for sni_proxy_docker_id in self.sni_proxy_docker_ids:
                stop_sni_proxy(sni_proxy_docker_id)
            self.sni_proxy_docker_ids = []

        if self._scylla_manager and not self.skip_manager_server:
            self._scylla_manager.stop(gently)
        kwargs = dict(**locals())
        del kwargs['self']
        if 'sni_proxy_docker_id' in kwargs:
            del kwargs['sni_proxy_docker_id']
        return self.stop_nodes(**kwargs)

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

    def _update_config(self, install_dir=None):
        """
        add scylla specific item to the cluster.conf
        :return: None
        """
        super(ScyllaCluster, self)._update_config()

        filename = os.path.join(self.get_path(), 'cluster.conf')

        with open(filename, 'r') as f:
            data = yaml.safe_load(f)

        if self.is_scylla_reloc():
            data['scylla_version'] = self.scylla_version

        if self._scylla_manager and self._scylla_manager.install_dir:
            data['scylla_manager_install_path'] = self._scylla_manager.install_dir

        with open(filename, 'w') as f:
            yaml.safe_dump(data, f)

    def sctool(self, cmd):
        if self._scylla_manager == None:
            raise Exception("scylla manager not enabled - sctool command cannot be executed")
        return self._scylla_manager.sctool(cmd)

    def start_scylla_manager(self):
        if not self._scylla_manager or self.skip_manager_server:
            return
        self._scylla_manager.start()

    def stop_scylla_manager(self, gently=True):
        if not self._scylla_manager or self.skip_manager_server:
            return
        self._scylla_manager.stop(gently)

    def upgrade_cluster(self, upgrade_version):
        """
        Support when uses relocatable packages only
        :param upgrade_version: relocatables name. Example: unstable/master:2020-11-18T08:57:53Z
        """
        for node in self.nodelist():
            node.upgrade(upgrade_version)

        self._update_config(install_dir=self.nodelist()[0].node_install_dir)


class ScyllaManager:
    def __init__(self, scylla_cluster, install_dir=None):
        self.scylla_cluster = scylla_cluster
        self._process_scylla_manager = None
        self._pid = None
        self.auth_token = str(uuid.uuid4())
        self.install_dir = install_dir
        if install_dir:
            if not os.path.exists(self._get_path()):
                os.mkdir(self._get_path())
            self._install(install_dir)
        else:
            self._update_pid()

    @property
    def version(self):
        stdout, _ = self.sctool(["version"], ignore_exit_status=True)
        version_string = stdout[stdout.find(": ") + 2:].strip()  # Removing unnecessary information
        version_code = LooseVersion(version_string)
        return version_code

    def _install(self, install_dir):
        self._copy_config_files(install_dir)
        self._copy_bin_files(install_dir)
        self._update_config(install_dir)

    def _get_api_address(self):
        return f"{self.scylla_cluster.get_node_ip(1)}:5080"

    def _update_config(self, install_dir=None):
        conf_file = os.path.join(self._get_path(), common.SCYLLAMANAGER_CONF)
        with open(conf_file, 'r') as f:
            data = yaml.safe_load(f)
        data['http'] = self._get_api_address()
        if not 'database' in data:
            data['database'] = {}
        data['database']['hosts'] = [self.scylla_cluster.get_node_ip(1)]
        data['database']['replication_factor'] = 3
        if install_dir and (self.version < LooseVersion("2.5") or
                            LooseVersion('666') < self.version < LooseVersion('666.dev-0.20210430.2217cc84')):
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
        if not 'repair' in data:
            data['repair'] = {}
        if self.version < LooseVersion("2.2"):
            data['repair']['segments_per_repair'] = 16
        data['prometheus'] = f"{self.scylla_cluster.get_node_ip(1)}:56091"
        # Changing port to 56091 since the manager and the first node share the same ip and 56090 is already in use
        # by the first node's manager agent
        data["debug"] = f"{self.scylla_cluster.get_node_ip(1)}:5611"
        # Since both the manager server and the first node use the same address, the manager can't use port
        # 56112, as node 1's agent already seized it
        if 'ssh' in data:
            del data['ssh']
        keys_to_delete = []
        for key in data:
            if not data[key]:
                keys_to_delete.append(key)  # can't delete from dict during loop
        for key in keys_to_delete:
            del data[key]
        with open(conf_file, 'w') as f:
            yaml.safe_dump(data, f, default_flow_style=False)

    def _copy_config_files(self, install_dir):
        conf_dir = os.path.join(install_dir, 'etc')
        if not os.path.exists(conf_dir):
            raise Exception(f"{install_dir} is not a valid scylla-manager install dir")
        for name in os.listdir(conf_dir):
            filename = os.path.join(conf_dir, name)
            if os.path.isfile(filename):
                shutil.copy(filename, self._get_path())

    def _copy_bin_files(self, install_dir):
        os.mkdir(os.path.join(self._get_path(), 'bin'))
        files = ['sctool', 'scylla-manager', 'scylla-manager-agent']
        for name in files:
            src = os.path.join(install_dir, name)
            if not os.path.exists(src):
                raise Exception(f"{src} not found in scylla-manager install dir")
            shutil.copy(src, os.path.join(self._get_path(), 'bin', name))

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
                print("Timed out waiting for pidfile {} to be filled (current time is %s): File {} size={}".format(
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
            raise NodeError(f'Problem starting scylla-manager due to {e}')

    def start(self):
        # some configurations are set post initialisation (cluster id) so
        # we are forced to update the config prior to calling start
        self._update_config(self.install_dir)
        # check process is not running
        if self._pid:
            try:
                os.kill(self._pid, 0)
                return
            except OSError as err:
                pass

        log_file = os.path.join(self._get_path(), 'scylla-manager.log')
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

        api_interface = common.parse_interface(self._get_api_address(), 5080)
        if not common.check_socket_listening(api_interface, timeout=180):
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

    def sctool(self, cmd, ignore_exit_status=False):
        sctool = os.path.join(self._get_path(), 'bin', 'sctool')
        args = [sctool, '--api-url', f"http://{self._get_api_address()}/api/v1"]
        args += cmd
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        stdout, stderr = p.communicate()
        exit_status = p.wait()
        if exit_status != 0 and not ignore_exit_status:
            raise Exception(" ".join(args), exit_status, stdout, stderr)
        return stdout, stderr

    def agent_check_location(self, location_list, extra_config_file_list=None):
        agent_bin = os.path.join(self._get_path(), 'bin', 'scylla-manager-agent')
        locations_names = ','.join(location_list)
        agent_conf = os.path.join(self.scylla_cluster.get_path(), 'node1/conf/scylla-manager-agent.yaml')
        args = [agent_bin, "check-location", "-L", str(locations_names)]
        if extra_config_file_list:
            for config_file in extra_config_file_list:
                args.extend(["-c", config_file])
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        stdout, stderr = p.communicate()
        exit_status = p.wait()
        if exit_status != 0:
            raise Exception(" ".join(args), exit_status, stdout, stderr)
        return stdout, stderr

    def agent_download_files(self, node, location_list, snapshot_tag, keyspace_filter_list=None, dry_run=False):
        """
        The function receives a node object, bucket location list and a snapshot tag
        and afterwards uses the agent's download-files command to download the snapshot
        files to the node's upload directory.
        optional:
            - keyspace_filter_list: list of glob strings. only the files of keyspaces
            that match the glob filters will be downloaded.
            - dry_run: The requested files will not be downloaded, but instead the agent
            will print out the names of the backed up tables and the size of their snapshots
        """
        node_id = node.hostid()
        agent_config_file = os.path.join(node.get_path(), "conf/scylla-manager-agent.yaml")
        agent_bin = os.path.join(self._get_path(), 'bin', 'scylla-manager-agent')
        locations_names = ','.join(location_list)
        args = [agent_bin, "download-files", "-c", agent_config_file, "-n",  node_id, "-L", str(locations_names),
                "--mode", "upload", "-T", snapshot_tag, "-d", os.path.join(node.get_path(), "data")]
        if keyspace_filter_list:
            args.extend(["-K", ",".join(keyspace_filter_list)])
        if dry_run:
            args.append("--dry-run")

        print(f"Issuing: {args}")
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        stdout, stderr = p.communicate()
        exit_status = p.wait()
        if exit_status != 0:
            raise Exception(" ".join(args), exit_status, stdout, stderr)
        return stdout, stderr
