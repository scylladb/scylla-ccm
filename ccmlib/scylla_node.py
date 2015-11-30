# ccm node
from __future__ import with_statement

import errno
import os
import shutil
import socket
import stat
import subprocess
import sys
import time

import psutil
import yaml
from six import print_

from ccmlib import common
from ccmlib.node import Node
from ccmlib.node import NodeError


class ScyllaNode(Node):

    """
    Provides interactions to a Scylla node.
    """

    def __init__(self, name, cluster, auto_bootstrap, thrift_interface,
                 storage_interface, jmx_port, remote_debug_port, initial_token,
                 save=True, binary_interface=None):
        super(ScyllaNode, self).__init__(name, cluster, auto_bootstrap,
                                         thrift_interface, storage_interface,
                                         jmx_port, remote_debug_port,
                                         initial_token, save, binary_interface)
        self.get_cassandra_version()

    def get_install_cassandra_root(self):
        return os.path.join(self.get_install_dir(), 'resources', 'cassandra')

    def get_node_cassandra_root(self):
        return os.path.join(self.get_path())

    def get_conf_dir(self):
        """
        Returns the path to the directory where Cassandra config are located
        """
        return os.path.join(self.get_path(), 'conf')

    def get_tool(self, toolname):
        return common.join_bin(os.path.join(self.get_install_dir(),
                                            'resources', 'cassandra'),
                               'bin', toolname)

    def get_tool_args(self, toolname):
        raise NotImplementedError('ScyllaNode.get_tool_args')

    def get_env(self):
        raise NotImplementedError('ScyllaNode.get_env')

    def get_cassandra_version(self):
        # TODO: Handle versioning
        return '2.1'

    def set_log_level(self, new_level, class_name=None):
        # TODO: overwritting node.py
        return self

    def set_workload(self, workload):
        raise NotImplementedError('ScyllaNode.set_workload')

    def cpuset(self, id, count):
        # leaving one core for other executables to run
        allocated_cpus = psutil.cpu_count() - 1
        start_id = id * count % allocated_cpus
        cpuset = []
        for cpuid in xrange(start_id, start_id + count):
            cpuset.append(str(cpuid % allocated_cpus))
        return cpuset

    # Scylla Overload start
    def start(self, join_ring=True, no_wait=False, verbose=False,
              update_pid=True, wait_other_notice=False, replace_token=None,
              replace_address=None, jvm_args=None, wait_for_binary_proto=False,
              profile_options=None, use_jna=False, quiet_start=False):
        """
        Start the node. Options includes:
          - join_ring: if false, start the node with -Dcassandra.join_ring=False
          - no_wait: by default, this method returns when the node is started
            and listening to clients.
            If no_wait=True, the method returns sooner.
          - wait_other_notice: if True, this method returns only when all other
            live node of the cluster
            have marked this node UP.
          - replace_token: start the node with the -Dcassandra.replace_token
            option.
          - replace_address: start the node with the
            -Dcassandra.replace_address option.
        """
        if jvm_args is None:
            jvm_args = []

        if self.is_running():
            raise NodeError("%s is already running" % self.name)

        for itf in list(self.network_interfaces.values()):
            if itf is not None and replace_address is None:
                common.check_socket_available(itf)

        marks = []
        if wait_other_notice:
            marks = [(node, node.mark_log()) for node in
                     list(self.cluster.nodes.values()) if node.is_running()]

        self.mark = self.mark_log()

        launch_bin = common.join_bin(self.get_path(), 'bin', 'run.sh')

        os.chmod(launch_bin, os.stat(launch_bin).st_mode | stat.S_IEXEC)

        # TODO: we do not support forcing specific settings
        # TODO: workaround for api-address as we do not load it
        # from config file scylla#59
        conf_file = os.path.join(self.get_conf_dir(), common.SCYLLA_CONF)
        with open(conf_file, 'r') as f:
            data = yaml.load(f)
        jvm_args = jvm_args + ['--api-address', data['api_address']]
        jvm_args = jvm_args + ['--collectd-hostname',
                               '%s.%s' % (socket.gethostname(), self.name)]

        args = [launch_bin, self.get_path()] + jvm_args
        if '--smp' not in args:
            args += ['--smp', '1']
        if '--memory' not in args:
            args += ['--memory', '512M']
        if '--default-log-level' not in args:
            args += ['--default-log-level', 'info']
        if '--collectd' not in args:
            args += ['--collectd', '0']
        if '--cpuset' not in args:
            smp = int(args[args.index('--smp') + 1])
            id = int(data['listen_address'].split('.')[3]) - 1
            cpuset = self.cpuset(id, smp)
            args += ['--cpuset', ','.join(cpuset)]

        # In case we are restarting a node
        # we risk reading the old cassandra.pid file
        self._delete_old_pid()

        FNULL = open(os.devnull, 'w')
        if common.is_win():
            # clean up any old dirty_pid files from prior runs
            if os.path.isfile(self.get_path() + "/dirty_pid.tmp"):
                os.remove(self.get_path() + "/dirty_pid.tmp")
            # TODO: restore proper environment handling on Windows
            env = os.environment()
            process = subprocess.Popen(args, cwd=self.get_bin_dir(), env=env,
                                       stdout=FNULL, stderr=subprocess.PIPE)
        else:
            # TODO: Support same cmdline options
            process = subprocess.Popen(args, stdout=FNULL, stderr=FNULL,
                                       close_fds=True)
            # TODO: workaround create pid file
            pidfile = os.path.join(self.get_path(), 'cassandra.pid')
            f = open(pidfile, "w")
            # we are waiting for the run script to have time to
            # run scylla process
            time.sleep(1)
            p = psutil.Process(process.pid)
            child_p = p.children()
            if child_p[0].name() != 'scylla':
                raise NodeError("Error starting scylla node")
            f.write(str(child_p[0].pid))
            f.flush()
            os.fsync(f)
            f.close
            os.fsync(f)

            # we are waiting to make sure the java process is up
            # by connecting to the port
            time.sleep(2)
            java_up = False
            iteration = 0
            while not java_up and iteration < 10:
                iteration += 1
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    s.settimeout(1.0)
                    s.connect((data['listen_address'], int(self.jmx_port)))
                    java_up = True
                except:
                    java_up = False
                try:
                    s.close()
                except:
                    pass
            time.sleep(1)

        # Our modified batch file writes a dirty output with more than
        # just the pid - clean it to get in parity
        # with *nix operation here.
        if common.is_win():
            self.__clean_win_pid()
            self._update_pid(process)
            print_("Started: {0} with pid: {1}".format(self.name, self.pid),
                   file=sys.stderr, flush=True)
        elif update_pid:
            self._update_pid(process)
            if not self.is_running():
                raise NodeError("Error starting node %s" % self.name, process)

        if wait_other_notice:
            for node, mark in marks:
                node.watch_log_for_alive(self, from_mark=mark)

        if wait_for_binary_proto and self.cluster.version() >= '1.2':
            self.watch_log_for("Starting listening for CQL clients",
                               from_mark=self.mark)
            # we're probably fine at that point but just wait some tiny bit
            # more because the msg is logged just before starting the binary
            # protocol server
            time.sleep(0.2)

        self.is_running()
        return process

    def start_dse(self,
                  join_ring=True,
                  no_wait=False,
                  verbose=False,
                  update_pid=True,
                  wait_other_notice=False,
                  replace_token=None,
                  replace_address=None,
                  jvm_args=None,
                  wait_for_binary_proto=False,
                  profile_options=None,
                  use_jna=False):
        """
        Start the node. Options includes:
          - join_ring: if false, start the node with -Dcassandra.join_ring=False
          - no_wait: by default, this method returns when the node is started
            and listening to clients.
            If no_wait=True, the method returns sooner.
          - wait_other_notice: if True, this method returns only when all other
            live node of the cluster  have marked this node UP.
          - replace_token: start the node with the -Dcassandra.replace_token
            option.
          - replace_address: start the node with the
            -Dcassandra.replace_address option.
        """
        if jvm_args is None:
            jvm_args = []
        raise NotImplementedError('ScyllaNode.start_dse')

    def import_config_files(self):
        # TODO: override node - enable logging
        self._update_config()
        self.copy_config_files()
        self.__update_yaml()

    def import_dse_config_files(self):
        raise NotImplementedError('ScyllaNode.import_dse_config_files')

    def copy_config_files_dse(self):
        raise NotImplementedError('ScyllaNode.copy_config_files_dse')

    def hard_link_or_copy(self, src, dst):
        try:
            os.link(src, dst)
        except OSError as oserror:
            if oserror.errno == errno.EXDEV or oserror.errno == errno.EMLINK:
                shutil.copy(src, dst)
            else:
                raise oserror

    def import_bin_files(self):
        # selectively copying files to reduce risk of using unintended items
        files = ['cassandra.in.sh', 'nodetool']
        os.makedirs(os.path.join(self.get_path(), 'resources', 'cassandra',
                                 'bin'))
        for name in files:
            self.hard_link_or_copy(os.path.join(self.get_install_dir(),
                                                'resources', 'cassandra',
                                                'bin', name),
                                   os.path.join(self.get_path(),
                                                'resources', 'cassandra',
                                                'bin', name))

        # selectively copying files to reduce risk of using unintended items
        files = ['sstable2json']
        os.makedirs(os.path.join(self.get_path(), 'resources', 'cassandra',
                                 'tools', 'bin'))
        for name in files:
            self.hard_link_or_copy(os.path.join(self.get_install_dir(),
                                                'resources', 'cassandra',
                                                'tools', 'bin', name),
                                   os.path.join(self.get_path(),
                                                'resources', 'cassandra',
                                                'tools', 'bin', name))

        # TODO: - currently no scripts only executable - copying exec
        scylla_mode = self.cluster.get_scylla_mode()
        self.hard_link_or_copy(os.path.join(self.get_install_dir(),
                                            'build', scylla_mode, 'scylla'),
                               os.path.join(self.get_bin_dir(), 'scylla'))
        self.hard_link_or_copy(os.path.join(self.get_install_dir(),
                                            '..', 'scylla-jmx', 'target',
                                            'urchin-mbean-1.0.jar'),
                               os.path.join(self.get_bin_dir(),
                                            'urchin-mbean-1.0.jar'))

        parent_dir = os.path.dirname(os.path.realpath(__file__))
        resources_bin_dir = os.path.join(parent_dir, '..', 'resources', 'bin')
        for name in os.listdir(resources_bin_dir):
            filename = os.path.join(resources_bin_dir, name)
            if os.path.isfile(filename):
                shutil.copy(filename, self.get_bin_dir())
                common.add_exec_permission(self.get_bin_dir(), name)

    def _save(self):
        # TODO: - overwrite node
        self.__update_yaml()
        self._update_config()

    def __update_yaml(self):
        # TODO: copied from node.py
        conf_file = os.path.join(self.get_conf_dir(), common.SCYLLA_CONF)
        with open(conf_file, 'r') as f:
            data = yaml.load(f)

        data['cluster_name'] = self.cluster.name
        data['auto_bootstrap'] = self.auto_bootstrap
        data['initial_token'] = self.initial_token
        if (not self.cluster.use_vnodes and
                self.get_base_cassandra_version() >= 1.2):
            data['num_tokens'] = 1
        if 'seeds' in data:
            # cassandra 0.7
            data['seeds'] = self.cluster.get_seeds()
        else:
            # cassandra 0.8
            data['seed_provider'][0]['parameters'][0]['seeds'] = (
                ','.join(self.cluster.get_seeds()))
        data['listen_address'], data['storage_port'] = (
            self.network_interfaces['storage'])
        data['rpc_address'], data['rpc_port'] = (
            self.network_interfaces['thrift'])
        if (self.network_interfaces['binary'] is not None and
                self.get_base_cassandra_version() >= 1.2):
            _, data['native_transport_port'] = self.network_interfaces['binary']

        data['data_file_directories'] = [os.path.join(self.get_path(), 'data')]
        data['commitlog_directory'] = os.path.join(self.get_path(),
                                                   'commitlogs')
        data['saved_caches_directory'] = os.path.join(self.get_path(),
                                                      'saved_caches')

        if self.cluster.partitioner:
            data['partitioner'] = self.cluster.partitioner

        # TODO: add scylla options
        data['api_address'] = data['listen_address']
        # last win and we want node options to win
        full_options = dict(self.cluster._config_options.items() +
                            self.get_config_options().items())
        for name in full_options:
            value = full_options[name]
            if value is None:
                try:
                    del data[name]
                except KeyError:
                    # it is fine to remove a key not there:w
                    pass
            else:
                try:
                    if isinstance(data[name], dict):
                        for option in full_options[name]:
                            data[name][option] = full_options[name][option]
                    else:
                        data[name] = full_options[name]
                except KeyError:
                    data[name] = full_options[name]

        with open(conf_file, 'w') as f:
            yaml.safe_dump(data, f, default_flow_style=False)

        # TODO: - for now create a cassandra conf file leaving only
        # cassandra config items - this should be removed once tools are
        # updated to remove scylla conf and use a shrunk version
        cassandra_conf_file = os.path.join(self.get_conf_dir(),
                                           common.CASSANDRA_CONF)
        cassandra_conf_items = {'authenticator': 0,
                                'authorizer': 0,
                                'auto_snapshot': 0,
                                'batch_size_warn_threshold_in_kb': 0,
                                'batchlog_replay_throttle_in_kb': 0,
                                'broadcast_address': 0,
                                'broadcast_rpc_address': 0,
                                'cas_contention_timeout_in_ms': 0,
                                'client_encryption_options': 0,
                                'cluster_name': 0,
                                'column_index_size_in_kb': 0,
                                'commit_failure_policy': 0,
                                'commitlog_directory': 0,
                                'commitlog_segment_size_in_mb': 0,
                                'commitlog_sync': 0,
                                'commitlog_sync_batch_window_in_ms': 0,
                                'commitlog_sync_period_in_ms': 0,
                                'commitlog_total_space_in_mb': 0,
                                'compaction_large_partition_warning_threshold_mb': 0,
                                'compaction_throughput_mb_per_sec': 0,
                                'concurrent_compactors': 0,
                                'concurrent_counter_writes': 0,
                                'concurrent_reads': 0,
                                'concurrent_writes': 0,
                                'counter_cache_keys_to_save': 0,
                                'counter_cache_save_period': 0,
                                'counter_cache_size_in_mb': 0,
                                'counter_write_request_timeout_in_ms': 0,
                                'cross_node_timeout': 0,
                                'data_file_directories': 0,
                                'disk_failure_policy': 0,
                                'dynamic_snitch_badness_threshold': 0,
                                'dynamic_snitch_reset_interval_in_ms': 0,
                                'dynamic_snitch_update_interval_in_ms': 0,
                                'endpoint_snitch': 0,
                                'file_cache_size_in_mb': 0,
                                'hinted_handoff_enabled': 0,
                                'hinted_handoff_throttle_in_kb': 0,
                                'incremental_backups': 0,
                                'index_summary_capacity_in_mb': 0,
                                'index_summary_resize_interval_in_minutes': 0,
                                'inter_dc_stream_throughput_outbound_megabits_per_sec': 0,
                                'inter_dc_tcp_nodelay': 0,
                                'internode_authenticator': 0,
                                'internode_compression': 0,
                                'key_cache_keys_to_save': 0,
                                'key_cache_save_period': 0,
                                'key_cache_size_in_mb': 0,
                                'listen_address': 0,
                                'listen_interface': 0,
                                'listen_interface_prefer_ipv6': 0,
                                'max_hint_window_in_ms': 0,
                                'max_hints_delivery_threads': 0,
                                'memory_allocator': 0,
                                'memtable_allocation_type': 0,
                                'memtable_cleanup_threshold': 0,
                                'memtable_flush_writers': 0,
                                'memtable_heap_space_in_mb': 0,
                                'memtable_offheap_space_in_mb': 0,
                                'native_transport_max_concurrent_connections': 0,
                                'native_transport_max_concurrent_connections_per_ip': 0,
                                'native_transport_max_frame_size_in_mb': 0,
                                'native_transport_max_threads': 0,
                                'native_transport_port': 0,
                                'num_tokens': 0,
                                'partitioner': 0,
                                'permissions_validity_in_ms': 0,
                                'phi_convict_threshold': 0,
                                'range_request_timeout_in_ms': 0,
                                'read_request_timeout_in_ms': 0,
                                'request_scheduler': 0,
                                'request_scheduler_id': 0,
                                'request_scheduler_options': 0,
                                'request_timeout_in_ms': 0,
                                'row_cache_keys_to_save': 0,
                                'row_cache_save_period': 0,
                                'row_cache_size_in_mb': 0,
                                'rpc_address': 0,
                                'rpc_interface': 0,
                                'rpc_interface_prefer_ipv6': 0,
                                'rpc_keepalive': 0,
                                'rpc_max_threads': 0,
                                'rpc_min_threads': 0,
                                'rpc_port': 0,
                                'rpc_recv_buff_size_in_bytes': 0,
                                'rpc_send_buff_size_in_bytes': 0,
                                'rpc_server_type': 0,
                                'seed_provider': 0,
                                'server_encryption_options': 0,
                                'snapshot_before_compaction': 0,
                                'ssl_storage_port': 0,
                                'sstable_preemptive_open_interval_in_mb': 0,
                                'start_native_transport': 0,
                                'start_rpc': 0,
                                'storage_port': 0,
                                'stream_throughput_outbound_megabits_per_sec': 0,
                                'streaming_socket_timeout_in_ms': 0,
                                'thrift_framed_transport_size_in_mb': 0,
                                'tombstone_failure_threshold': 0,
                                'tombstone_warn_threshold': 0,
                                'trickle_fsync': 0,
                                'trickle_fsync_interval_in_kb': 0,
                                'truncate_request_timeout_in_ms': 0,
                                'write_request_timeout_in_ms': 0}
        cassandra_data = {}
        for key in data:
            if key in cassandra_conf_items:
                cassandra_data[key] = data[key]

        with open(cassandra_conf_file, 'w') as f:
            yaml.safe_dump(cassandra_data, f, default_flow_style=False)

    def __update_yaml_dse(self):
        raise NotImplementedError('ScyllaNode.__update_yaml_dse')

    def _update_log4j(self):
        raise NotImplementedError('ScyllaNode._update_log4j')

    def __generate_server_xml(self):
        raise NotImplementedError('ScyllaNode.__generate_server_xml')

    def _get_directories(self):
        dirs = {}
        for i in ['data', 'commitlogs', 'bin', 'conf', 'logs']:
            dirs[i] = os.path.join(self.get_path(), i)
        return dirs

    def _copy_agent(self):
        raise NotImplementedError('ScyllaNode._copy_agent')

    def _start_agent(self):
        raise NotImplementedError('ScyllaNode._start_agent')

    def _stop_agent(self):
        raise NotImplementedError('ScyllaNode._stop_agent')

    def _write_agent_address_yaml(self, agent_dir):
        raise NotImplementedError('ScyllaNode._write_agent_address_yaml')

    def _write_agent_log4j_properties(self, agent_dir):
        raise NotImplementedError('ScyllaNode._write_agent_log4j_properties')

    # TODO: - scylla flush is async - it returns immediately
    def flush(self):
        self.nodetool("flush")
        time.sleep(2)
