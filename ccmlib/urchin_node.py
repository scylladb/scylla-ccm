# ccm node
from __future__ import with_statement

from six import print_

import os
import shutil
import stat
import subprocess
import time
import yaml
import signal
import psutil
import socket

from ccmlib.node import Node
from ccmlib.node import NodeError
from ccmlib import common

class UrchinNode(Node):
    """
    Provides interactions to a Urchin node.
    """

    def __init__(self, name, cluster, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save=True, binary_interface=None):
        super(UrchinNode, self).__init__(name, cluster, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save, binary_interface)
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
        return common.join_bin(os.path.join(self.get_install_dir(), 'resources', 'cassandra'), 'bin', toolname)

    def get_tool_args(self, toolname):
        raise Exception ("no impl")
        return [common.join_bin(os.path.join(self.get_install_dir(), 'resources', 'cassandra'), 'bin', 'dse'), toolname]

    def get_env(self):
        raise Exception ("no impl")
        return common.make_dse_env(self.get_install_dir(), self.get_path())

    def get_cassandra_version(self):
        # FIXME
        return '2.1'

    def set_log_level(self, new_level, class_name=None):
        # FIXME overwritting node.py
        return self

    def set_workload(self, workload):
        raise Exception ("no impl")
        self.workload = workload
        self._update_config()
        if workload == 'solr':
            self.__generate_server_xml()

    # Urchin Overload start
    def start(self,
              join_ring=True,
              no_wait=False,
              verbose=False,
              update_pid=True,
              wait_other_notice=False,
              replace_token=None,
              replace_address=None,
              jvm_args=[],
              wait_for_binary_proto=False,
              profile_options=None,
              use_jna=False):
        """
        Start the node. Options includes:
          - join_ring: if false, start the node with -Dcassandra.join_ring=False
          - no_wait: by default, this method returns when the node is started and listening to clients.
            If no_wait=True, the method returns sooner.
          - wait_other_notice: if True, this method returns only when all other live node of the cluster
            have marked this node UP.
          - replace_token: start the node with the -Dcassandra.replace_token option.
          - replace_address: start the node with the -Dcassandra.replace_address option.
        """
        # Validate Windows env
        #if common.is_win() and not common.is_ps_unrestricted() and self.cluster.version() >= '2.1':
        #    raise NodeError("PS Execution Policy must be unrestricted when running C* 2.1+")

        if self.is_running():
            raise NodeError("%s is already running" % self.name)

        for itf in list(self.network_interfaces.values()):
            if itf is not None and replace_address is None:
                common.check_socket_available(itf)

        if wait_other_notice:
            marks = [(node, node.mark_log()) for node in list(self.cluster.nodes.values()) if node.is_running()]

        self.mark = self.mark_log()

        cdir = self.get_install_dir()
        #launch_bin = common.join_bin(cdir, 'bin', 'scylla')
        ## Copy back the cassandra scripts since profiling may have modified it the previous time
        #shutil.copy(launch_bin, self.get_bin_dir())
        launch_bin = common.join_bin(self.get_path(), 'bin', 'run.sh')

        # If Windows, change entries in .bat file to split conf from binaries
        #if common.is_win():
        #    self.__clean_bat()

        #if profile_options is not None:
        #    config = common.get_config()
        #    if 'yourkit_agent' not in config:
        #        raise NodeError("Cannot enable profile. You need to set 'yourkit_agent' to the path of your agent in a ~/.ccm/config")
        #    cmd = '-agentpath:%s' % config['yourkit_agent']
        #    if 'options' in profile_options:
        #        cmd = cmd + '=' + profile_options['options']
        #    print_(cmd)
        #    # Yes, it's fragile as shit
        #    pattern = r'cassandra_parms="-Dlog4j.configuration=log4j-server.properties -Dlog4j.defaultInitOverride=true'
        #    common.replace_in_file(launch_bin, pattern, '    ' + pattern + ' ' + cmd + '"')

        os.chmod(launch_bin, os.stat(launch_bin).st_mode | stat.S_IEXEC)

        #env = common.make_cassandra_env(cdir, self.get_path())

        #if common.is_win():
        #    self._clean_win_jmx()

        pidfile = os.path.join(self.get_path(), 'cassandra.pid')
        # FIXME we do not support this forcing specific settings

        # FIXME workaround for api-address as we do not load it from config file urchin#59
        conf_file = os.path.join(self.get_conf_dir(),common.URCHIN_CONF)
        with open(conf_file, 'r') as f:
             data = yaml.load(f)
        jvm_args = jvm_args + ['--api-address',data['api_address']]
        jvm_args = jvm_args + ['--collectd-hostname',socket.gethostname()+"."+self.name]

        args = [launch_bin, self.get_path()] + jvm_args
        if '--smp' not in args:
           args += ['--smp', '1']
        if '--memory' not in args:
           args += ['--memory','512M']
        if '--default-log-level' not in args:
           args += ['--default-log-level','info']
        if '--collectd' not in args:
           args += ['--collectd','0']
        #args = [launch_bin, '-p', pidfile, '-Dcassandra.join_ring=%s' % str(join_ring)]
        #if replace_token is not None:
        #    args.append('-Dcassandra.replace_token=%s' % str(replace_token))
        #if replace_address is not None:
        #    args.append('-Dcassandra.replace_address=%s' % str(replace_address))
        #if use_jna is False:
        #    args.append('-Dcassandra.boot_without_jna=true')
        #env['JVM_EXTRA_OPTS'] = env.get('JVM_EXTRA_OPTS', "") + " ".join(jvm_args)

        #In case we are restarting a node
        #we risk reading the old cassandra.pid file
        self._delete_old_pid()

        process = None
        FNULL = open(os.devnull, 'w')
        if common.is_win():
            # clean up any old dirty_pid files from prior runs
            if (os.path.isfile(self.get_path() + "/dirty_pid.tmp")):
                os.remove(self.get_path() + "/dirty_pid.tmp")
            process = subprocess.Popen(args, cwd=self.get_bin_dir(), env=env, stdout=FNULL, stderr=subprocess.PIPE)
        else:
            # FIXME
            process = subprocess.Popen(args,stdout=FNULL,stderr=FNULL,close_fds=True)
            #process = subprocess.Popen(args, stdout=FNULL, stderr=subprocess.PIPE)
            # FIXME workaround create pid file
            pidfile = os.path.join(self.get_path(), 'cassandra.pid')
            f = open(pidfile,"w")
            # we are waiting for the run script to have time to run scylla process
            time.sleep(1)
            p = psutil.Process(process.pid)
            child_p = p.children()
            if child_p[0].name() != 'scylla':
               raise NodeError("Error starting urchin node");
            f.write(str(child_p[0].pid))
            f.flush()
            os.fsync(f)
            f.close
            os.fsync(f)

            # we are waiting to make sure the java process is up  by connecting to the port
            time.sleep(2)
            java_up = False
            iteration = 0
            while (java_up != True and iteration < 10):
               iteration = iteration + 1
               s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
               try:
                   s.settimeout(1.0)
                   s.connect((data['listen_address'],int(self.jmx_port)))
                   java_up = True
               except:
                   java_up = False
               try:
                  s.close()
               except:
                  pass
            time.sleep(1)

        # Our modified batch file writes a dirty output with more than just the pid - clean it to get in parity
        # with *nix operation here.
        if common.is_win():
            self.__clean_win_pid()
            self._update_pid(process)
            print_("Started: {0} with pid: {1}".format(self.name, self.pid), file=sys.stderr, flush=True)
        elif update_pid:
            self._update_pid(process)
            if not self.is_running():
                raise NodeError("Error starting node %s" % self.name, process)

        if wait_other_notice:
            for node, mark in marks:
                node.watch_log_for_alive(self, from_mark=mark)

        if wait_for_binary_proto and self.cluster.version() >= '1.2':
            self.watch_log_for("Starting listening for CQL clients", from_mark=self.mark)
            # we're probably fine at that point but just wait some tiny bit more because
            # the msg is logged just before starting the binary protocol server
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
              jvm_args=[],
              wait_for_binary_proto=False,
              profile_options=None,
              use_jna=False):
        """
        Start the node. Options includes:
          - join_ring: if false, start the node with -Dcassandra.join_ring=False
          - no_wait: by default, this method returns when the node is started and listening to clients.
            If no_wait=True, the method returns sooner.
          - wait_other_notice: if True, this method returns only when all other live node of the cluster
            have marked this node UP.
          - replace_token: start the node with the -Dcassandra.replace_token option.
          - replace_address: start the node with the -Dcassandra.replace_address option.
        """
        raise Exception ("no impl")

        if self.is_running():
            raise NodeError("%s is already running" % self.name)

        for itf in list(self.network_interfaces.values()):
            if itf is not None and replace_address is None:
                common.check_socket_available(itf)

        if wait_other_notice:
            marks = [ (node, node.mark_log()) for node in list(self.cluster.nodes.values()) if node.is_running() ]


        cdir = self.get_install_dir()
        launch_bin = common.join_bin(cdir, 'bin', 'dse')
        # Copy back the dse scripts since profiling may have modified it the previous time
        shutil.copy(launch_bin, self.get_bin_dir())
        launch_bin = common.join_bin(self.get_path(), 'bin', 'dse')

        # If Windows, change entries in .bat file to split conf from binaries
        if common.is_win():
            self.__clean_bat()

        if profile_options is not None:
            config = common.get_config()
            if not 'yourkit_agent' in config:
                raise NodeError("Cannot enable profile. You need to set 'yourkit_agent' to the path of your agent in a ~/.ccm/config")
            cmd = '-agentpath:%s' % config['yourkit_agent']
            if 'options' in profile_options:
                cmd = cmd + '=' + profile_options['options']
            print_(cmd)
            # Yes, it's fragile as shit
            pattern=r'cassandra_parms="-Dlog4j.configuration=log4j-server.properties -Dlog4j.defaultInitOverride=true'
            common.replace_in_file(launch_bin, pattern, '    ' + pattern + ' ' + cmd + '"')

        os.chmod(launch_bin, os.stat(launch_bin).st_mode | stat.S_IEXEC)

        env = common.make_dse_env(self.get_install_dir(), self.get_path())

        if common.is_win():
            self._clean_win_jmx();

        pidfile = os.path.join(self.get_path(), 'cassandra.pid')
        args = [launch_bin, 'cassandra']

        if self.workload is not None:
            if 'hadoop' in self.workload:
                args.append('-t')
            if 'solr' in self.workload:
                args.append('-s')
            if 'spark' in self.workload:
                args.append('-k')
            if 'cfs' in self.workload:
                args.append('-c')
        args += [ '-p', pidfile, '-Dcassandra.join_ring=%s' % str(join_ring) ]
        if replace_token is not None:
            args.append('-Dcassandra.replace_token=%s' % str(replace_token))
        if replace_address is not None:
            args.append('-Dcassandra.replace_address=%s' % str(replace_address))
        if use_jna is False:
            args.append('-Dcassandra.boot_without_jna=true')
        args = args + jvm_args

        process = None
        if common.is_win():
            # clean up any old dirty_pid files from prior runs
            if (os.path.isfile(self.get_path() + "/dirty_pid.tmp")):
                os.remove(self.get_path() + "/dirty_pid.tmp")
            process = subprocess.Popen(args, cwd=self.get_bin_dir(), env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        else:
            process = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Our modified batch file writes a dirty output with more than just the pid - clean it to get in parity
        # with *nix operation here.
        if common.is_win():
            self.__clean_win_pid()
            self._update_pid(process)
        elif update_pid:
            if no_wait:
                time.sleep(2) # waiting 2 seconds nevertheless to check for early errors and for the pid to be set
            else:
                for line in process.stdout:
                    if verbose:
                        print_(line.rstrip('\n'))

            self._update_pid(process)

            if not self.is_running():
                raise NodeError("Error starting node %s" % self.name, process)

        if wait_other_notice:
            for node, mark in marks:
                node.watch_log_for_alive(self, from_mark=mark)

        if wait_for_binary_proto:
            self.watch_log_for("Starting listening for CQL clients")
            # we're probably fine at that point but just wait some tiny bit more because
            # the msg is logged just before starting the binary protocol server
            time.sleep(0.2)

        if self.cluster.hasOpscenter():
            self._start_agent()

        return process

    def import_config_files(self):
        # FIXME override node - enable logging
        self._update_config()
        self.copy_config_files()
        self.__update_yaml()
        ## loggers changed > 2.1
        #if self.get_base_cassandra_version() < 2.1:
        #    self._update_log4j()
        #else:
        #    self.__update_logback()
        # FIXME
        #self.__update_envfile()

    def import_dse_config_files(self):
        raise Exception ("no impl")
        self._update_config()
        if not os.path.isdir(os.path.join(self.get_path(), 'resources', 'dse', 'conf')):
            os.makedirs(os.path.join(self.get_path(), 'resources', 'dse', 'conf'))
        common.copy_directory(os.path.join(self.get_install_dir(), 'resources', 'dse', 'conf'), os.path.join(self.get_path(), 'resources', 'dse', 'conf'))
        self.__update_yaml()

    def copy_config_files_dse(self):
        raise Exception ("no impl")
        for product in ['dse', 'cassandra', 'hadoop', 'sqoop', 'hive', 'tomcat', 'spark', 'shark', 'mahout', 'pig', 'solr']:
            src_conf = os.path.join(self.get_install_dir(), 'resources', product, 'conf')
            dst_conf = os.path.join(self.get_path(), 'resources', product, 'conf')
            if os.path.isdir(dst_conf):
                common.rmdirs(dst_conf)
            shutil.copytree(src_conf, dst_conf)
            if product == 'solr':
                src_web = os.path.join(self.get_install_dir(), 'resources', product, 'web')
                dst_web = os.path.join(self.get_path(), 'resources', product, 'web')
                if os.path.isdir(dst_web):
                    common.rmdirs(dst_web)
                shutil.copytree(src_web, dst_web)
            if product == 'tomcat':
                src_lib = os.path.join(self.get_install_dir(), 'resources', product, 'lib')
                dst_lib = os.path.join(self.get_path(), 'resources', product, 'lib')
                if os.path.isdir(dst_lib):
                    common.rmdirs(dst_lib)
                shutil.copytree(src_lib, dst_lib)
                src_webapps = os.path.join(self.get_install_dir(), 'resources', product, 'webapps')
                dst_webapps = os.path.join(self.get_path(), 'resources', product, 'webapps')
                if os.path.isdir(dst_webapps):
                    common.rmdirs(dst_webapps)
                shutil.copytree(src_webapps, dst_webapps)

    def hard_link_or_copy(self,src,dst):
        try:
            os.link(src,dst)
        except OSError as oserror:
            if oserror.errno == errno.EXDEV or oserror.errno == errno.EMLINK:
                shutil.copy(src,dst)
            else:
                raise oserror

    def import_bin_files(self):
        # selectivly copying files to reduce risk of using unintended items
        files = ['cassandra.in.sh','nodetool']
        os.makedirs(os.path.join(self.get_path(), 'resources', 'cassandra', 'bin'))
        for name in files:
            self.hard_link_or_copy(os.path.join(self.get_install_dir(), 'resources', 'cassandra', 'bin',name), os.path.join(self.get_path(), 'resources', 'cassandra', 'bin',name))

        # selectivly copying files to reduce risk of using unintended items
        files = ['sstable2json']
        os.makedirs(os.path.join(self.get_path(), 'resources', 'cassandra', 'tools','bin'))
        for name in files:
            self.hard_link_or_copy(os.path.join(self.get_install_dir(), 'resources', 'cassandra', 'tools','bin',name), os.path.join(self.get_path(), 'resources', 'cassandra', 'tools','bin',name))

        # FIXME - currently no scripts only executable - copying exec
        urchin_mode = self.cluster.get_urchin_mode();
        self.hard_link_or_copy(os.path.join(self.get_install_dir(), 'build', urchin_mode, 'scylla'), os.path.join(self.get_bin_dir(),'scylla'))
        self.hard_link_or_copy(os.path.join(self.get_install_dir(), '../urchin-jmx', 'target', 'urchin-mbean-1.0.jar'), os.path.join(self.get_bin_dir(),'urchin-mbean-1.0.jar'))

        resources_bin_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'resources', 'bin')
        for name in os.listdir(resources_bin_dir):
            filename = os.path.join(resources_bin_dir, name)
            if os.path.isfile(filename):
                shutil.copy(filename, self.get_bin_dir())
                common.add_exec_permission(self.get_bin_dir(), name)

    def _save(self):
        # FIXME - overwrite node
        self.__update_yaml()
        # loggers changed > 2.1
        #if self.get_base_cassandra_version() < 2.1:
        #    self._update_log4j()
        #else:
        #    self.__update_logback()
        # FIXME
        #self.__update_envfile()
        self._update_config()

    def __update_yaml(self):
        # FIXME copied from node.py
        conf_file = os.path.join(self.get_conf_dir(), common.URCHIN_CONF)
        with open(conf_file, 'r') as f:
            data = yaml.load(f)

        data['cluster_name'] = self.cluster.name
        data['auto_bootstrap'] = self.auto_bootstrap
        data['initial_token'] = self.initial_token
        if not self.cluster.use_vnodes and self.get_base_cassandra_version() >= 1.2:
            data['num_tokens'] = 1
        if 'seeds' in data:
            # cassandra 0.7
            data['seeds'] = self.cluster.get_seeds()
        else:
            # cassandra 0.8
            data['seed_provider'][0]['parameters'][0]['seeds'] = ','.join(self.cluster.get_seeds())
        data['listen_address'], data['storage_port'] = self.network_interfaces['storage']
        data['rpc_address'], data['rpc_port'] = self.network_interfaces['thrift']
        if self.network_interfaces['binary'] is not None and self.get_base_cassandra_version() >= 1.2:
            _, data['native_transport_port'] = self.network_interfaces['binary']

        data['data_file_directories'] = [os.path.join(self.get_path(), 'data')]
        data['commitlog_directory'] = os.path.join(self.get_path(), 'commitlogs')
        data['saved_caches_directory'] = os.path.join(self.get_path(), 'saved_caches')

        if self.cluster.partitioner:
            data['partitioner'] = self.cluster.partitioner

        # FIXME add urchin options
        data['api_address'] = data['listen_address']
        # full_options = dict(list(self.cluster._config_options.items()) + list(self.__config_options.items())) # last win and we want node options to win
        full_options = dict(list(self.cluster._config_options.items())) # last win and we want node options to win
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

        # FIXME - for now create a cassandra conf file leaving only cassandra config items - this should be removed once tools are updated to remove urchin conf and use a "shrinked" version
        cassandra_conf_file = os.path.join(self.get_conf_dir(), common.CASSANDRA_CONF)
        cassandra_conf_items = {'cluster_name':0,'num_tokens':0,'hinted_handoff_enabled':0,'max_hint_window_in_ms':0,'hinted_handoff_throttle_in_kb':0,'max_hints_delivery_threads':0,'batchlog_replay_throttle_in_kb':0,'authenticator':0,'authorizer':0,'permissions_validity_in_ms':0,'partitioner':0,'data_file_directories':0,'commitlog_directory':0,'disk_failure_policy':0,'commit_failure_policy':0,'key_cache_size_in_mb':0,'key_cache_save_period':0,'key_cache_keys_to_save':0,'row_cache_size_in_mb':0,'row_cache_save_period':0,'row_cache_keys_to_save':0,'counter_cache_size_in_mb':0,'counter_cache_save_period':0,'counter_cache_keys_to_save':0,'memory_allocator':0,'commitlog_sync_batch_window_in_ms':0,'commitlog_sync':0,'commitlog_sync_period_in_ms':0,'commitlog_segment_size_in_mb':0,'seed_provider':0,'concurrent_reads':0,'concurrent_writes':0,'concurrent_counter_writes':0,'file_cache_size_in_mb':0,'memtable_heap_space_in_mb':0,'memtable_offheap_space_in_mb':0,'memtable_cleanup_threshold':0,'memtable_allocation_type':0,'commitlog_total_space_in_mb':0,'memtable_flush_writers':0,'index_summary_capacity_in_mb':0,'index_summary_resize_interval_in_minutes':0,'trickle_fsync':0,'trickle_fsync_interval_in_kb':0,'storage_port':0,'ssl_storage_port':0,'listen_address':0,'listen_interface':0,'listen_interface_prefer_ipv6':0,'broadcast_address':0,'internode_authenticator':0,'start_native_transport':0,'native_transport_port':0,'native_transport_max_threads':0,'native_transport_max_frame_size_in_mb':0,'native_transport_max_concurrent_connections':0,'native_transport_max_concurrent_connections_per_ip':0,'start_rpc':0,'rpc_address':0,'rpc_interface':0,'rpc_interface_prefer_ipv6':0,'rpc_port':0,'broadcast_rpc_address':0,'rpc_keepalive':0,'rpc_server_type':0,'rpc_min_threads':0,'rpc_max_threads':0,'rpc_send_buff_size_in_bytes':0,'rpc_recv_buff_size_in_bytes':0,'thrift_framed_transport_size_in_mb':0,'incremental_backups':0,'snapshot_before_compaction':0,'auto_snapshot':0,'tombstone_warn_threshold':0,'tombstone_failure_threshold':0,'column_index_size_in_kb':0,'batch_size_warn_threshold_in_kb':0,'concurrent_compactors':0,'compaction_throughput_mb_per_sec':0,'compaction_large_partition_warning_threshold_mb':0,'sstable_preemptive_open_interval_in_mb':0,'stream_throughput_outbound_megabits_per_sec':0,'inter_dc_stream_throughput_outbound_megabits_per_sec':0,'read_request_timeout_in_ms':0,'range_request_timeout_in_ms':0,'write_request_timeout_in_ms':0,'counter_write_request_timeout_in_ms':0,'cas_contention_timeout_in_ms':0,'truncate_request_timeout_in_ms':0,'request_timeout_in_ms':0,'cross_node_timeout':0,'streaming_socket_timeout_in_ms':0,'phi_convict_threshold':0,'endpoint_snitch':0,'dynamic_snitch_update_interval_in_ms':0,'dynamic_snitch_reset_interval_in_ms':0,'dynamic_snitch_badness_threshold':0,'request_scheduler':0,'request_scheduler_options':0,'request_scheduler_id':0,'server_encryption_options':0,'client_encryption_options':0,'internode_compression':0,'inter_dc_tcp_nodelay':0}
        cassandra_data = {}
        for key in data:
            if key in cassandra_conf_items:
                cassandra_data[key] = data[key]
        
        with open(cassandra_conf_file, 'w') as f:
            yaml.safe_dump(cassandra_data, f, default_flow_style=False)

    def __update_yaml_dse(self):
        raise Exception ("no impl")
        conf_file = os.path.join(self.get_path(), 'resources', 'dse', 'conf', 'dse.yaml')
        with open(conf_file, 'r') as f:
            data = yaml.load(f)

        data['system_key_directory'] = os.path.join(self.get_path(), 'keys')

        full_options = dict(list(self.cluster._dse_config_options.items()))
        for name in full_options:
            if not name is 'dse_yaml_file':
                value = full_options[name]
                if value is None:
                    try:
                        del data[name]
                    except KeyError:
                        # it is fine to remove a key not there:w
                        pass
                else:
                    data[name] = full_options[name]

        if 'dse_yaml_file' in full_options:
            with open(full_options['dse_yaml_file'], 'r') as f:
                user_yaml = yaml.load(f)
                data = common.yaml_merge(data, user_yaml)

        with open(conf_file, 'w') as f:
            yaml.safe_dump(data, f, default_flow_style=False)

    def _update_log4j(self):
        raise Exception ("no impl")
        super(UrchinNode, self)._update_log4j()

        conf_file = os.path.join(self.get_conf_dir(), common.LOG4J_CONF)
        append_pattern = 'log4j.appender.V.File='
        log_file = os.path.join(self.get_path(), 'logs', 'solrvalidation.log')
        if common.is_win():
            log_file = re.sub("\\\\", "/", log_file)
        common.replace_in_file(conf_file, append_pattern, append_pattern + log_file)

        append_pattern = 'log4j.appender.A.File='
        log_file = os.path.join(self.get_path(), 'logs', 'audit.log')
        if common.is_win():
            log_file = re.sub("\\\\", "/", log_file)
        common.replace_in_file(conf_file, append_pattern, append_pattern + log_file)

        append_pattern = 'log4j.appender.B.File='
        log_file = os.path.join(self.get_path(), 'logs', 'audit', 'dropped-events.log')
        if common.is_win():
            log_file = re.sub("\\\\", "/", log_file)
        common.replace_in_file(conf_file, append_pattern, append_pattern + log_file)

    def __generate_server_xml(self):
        raise Exception ("no impl")
        server_xml = os.path.join(self.get_path(), 'resources', 'tomcat', 'conf', 'server.xml')
        if os.path.isfile(server_xml):
            os.remove(server_xml)
        with open(server_xml, 'w+') as f:
            f.write('<Server port="8005" shutdown="SHUTDOWN">\n')
            f.write('  <Service name="Solr">\n')
            f.write('    <Connector port="8983" address="%s" protocol="HTTP/1.1" connectionTimeout="20000" maxThreads = "200" URIEncoding="UTF-8"/>\n' % self.network_interfaces['thrift'][0])
            f.write('    <Engine name="Solr" defaultHost="localhost">\n')
            f.write('      <Host name="localhost"  appBase="../solr/web"\n')
            f.write('            unpackWARs="true" autoDeploy="true"\n')
            f.write('            xmlValidation="false" xmlNamespaceAware="false">\n')
            f.write('      </Host>\n')
            f.write('    </Engine>\n')
            f.write('  </Service>\n')
            f.write('</Server>\n')
            f.close()

    def _get_directories(self):
        dirs = {}
        #for i in ['data', 'commitlogs', 'saved_caches', 'logs', 'bin', 'keys', 'resources']:
        for i in ['data', 'commitlogs', 'bin', 'conf','logs']:
            dirs[i] = os.path.join(self.get_path(), i)
        return dirs

    def _copy_agent(self):
        raise Exception ("no impl")
        agent_source = os.path.join(self.get_install_dir(), 'datastax-agent')
        agent_target = os.path.join(self.get_path(), 'datastax-agent')
        if os.path.exists(agent_source) and not os.path.exists(agent_target):
            shutil.copytree(agent_source, agent_target)

    def _start_agent(self):
        raise Exception ("no impl")
        agent_dir = os.path.join(self.get_path(), 'datastax-agent')
        if os.path.exists(agent_dir):
            self._write_agent_address_yaml(agent_dir)
            self._write_agent_log4j_properties(agent_dir)
            args = [os.path.join(agent_dir, 'bin', common.platform_binary('datastax-agent'))]
            subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def _stop_agent(self):
        raise Exception ("no impl")
        agent_dir = os.path.join(self.get_path(), 'datastax-agent')
        if os.path.exists(agent_dir):
            pidfile = os.path.join(agent_dir, 'datastax-agent.pid')
        if os.path.exists(pidfile):
            with open(pidfile, 'r') as f:
                pid = int(f.readline().strip())
                f.close()
            if pid is not None:
                try:
                    os.kill(pid, signal.SIGKILL)
                except OSError:
                    pass
            os.remove(pidfile)

    def _write_agent_address_yaml(self, agent_dir):
        raise Exception ("no impl")
        address_yaml = os.path.join(agent_dir, 'conf', 'address.yaml')
        if not os.path.exists(address_yaml):
            with open(address_yaml, 'w+') as f:
                (ip, port) = self.network_interfaces['thrift']
                jmx = self.jmx_port
                f.write('stomp_interface: 127.0.0.1\n')
                f.write('local_interface: %s\n' % ip)
                f.write('agent_rpc_interface: %s\n' % ip)
                f.write('agent_rpc_broadcast_address: %s\n' % ip)
                f.write('cassandra_conf: %s\n' % os.path.join(self.get_path(), 'resources', 'cassandra', 'conf', 'cassandra.yaml'))
                f.write('cassandra_install: %s\n' % self.get_path())
                f.write('cassandra_logs: %s\n' % os.path.join(self.get_path(), 'logs'))
                f.write('thrift_port: %s\n' % port)
                f.write('jmx_port: %s\n' % jmx)
                f.close()

    def _write_agent_log4j_properties(self, agent_dir):
        raise Exception ("no impl")
        log4j_properties = os.path.join(agent_dir, 'conf', 'log4j.properties')
        with open(log4j_properties, 'w+') as f:
            f.write('log4j.rootLogger=INFO,R\n')
            f.write('log4j.logger.org.apache.http=OFF\n')
            f.write('log4j.logger.org.eclipse.jetty.util.log=WARN,R\n')
            f.write('log4j.appender.R=org.apache.log4j.RollingFileAppender\n')
            f.write('log4j.appender.R.maxFileSize=20MB\n')
            f.write('log4j.appender.R.maxBackupIndex=5\n')
            f.write('log4j.appender.R.layout=org.apache.log4j.PatternLayout\n')
            f.write('log4j.appender.R.layout.ConversionPattern=%5p [%t] %d{ISO8601} %m%n\n')
            f.write('log4j.appender.R.File=./log/agent.log\n')
            f.close()

    # Overload - FIXME - urchin flush is aynch - so it returns immediatly
    def flush(self):
        self.nodetool("flush")
        time.sleep(2)
