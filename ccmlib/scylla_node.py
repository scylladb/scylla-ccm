# ccm node
from __future__ import with_statement

from datetime import datetime
import errno
import os
import signal
import shutil
import socket
import stat
import subprocess
import time
import threading

import psutil
import yaml
import glob
import re

from six import print_
from six.moves import xrange

from ccmlib import common
from ccmlib.node import Node
from ccmlib.node import NodeError
from ccmlib.node import TimeoutError


def wait_for(func, timeout, first=0.0, step=1.0, text=None):
    """
    Wait until func() evaluates to True.

    If func() evaluates to True before timeout expires, return the
    value of func(). Otherwise return None.

    :param func: Function that will be evaluated.
    :param timeout: Timeout in seconds
    :param first: Time to sleep before first attempt
    :param step: Time to sleep between attempts in seconds
    :param text: Text to print while waiting, for debug purposes
    """
    start_time = time.time()
    end_time = time.time() + timeout

    time.sleep(first)

    while time.time() < end_time:
        if text:
            print_("%s (%f secs)" % (text, (time.time() - start_time)))

        output = func()
        if output:
            return output

        time.sleep(step)

    return None


class ScyllaNode(Node):

    """
    Provides interactions to a Scylla node.
    """

    def __init__(self, name, cluster, auto_bootstrap, thrift_interface,
                 storage_interface, jmx_port, remote_debug_port, initial_token,
                 save=True, binary_interface=None, scylla_manager=None):
        super(ScyllaNode, self).__init__(name, cluster, auto_bootstrap,
                                         thrift_interface, storage_interface,
                                         jmx_port, remote_debug_port,
                                         initial_token, save, binary_interface)
        self.__global_log_level = 'info'
        self.__classes_log_level = {}
        self.get_cassandra_version()
        self._process_jmx = None
        self._process_jmx_waiter = None
        self._process_scylla = None
        self._process_scylla_waiter = None
        self._process_agent = None
        self._process_agent_waiter = None
        self._smp = 1
        self._smp_set_during_test = False
        self._mem_mb_per_cpu = 512
        self._mem_set_during_test = False
        self.__conf_updated = False
        self.scylla_manager = scylla_manager
        self.jmx_pid = None
        self.agent_pid = None
        self._create_directory()

    def scylla_mode(self):
        return self.cluster.get_scylla_mode()

    def is_scylla_reloc(self):
        return self.cluster.is_scylla_reloc()

    def set_smp(self, smp):
        self._smp =  smp
        self._smp_set_during_test = True

    def set_mem_mb_per_cpu(self, mem):
        self._mem_mb_per_cpu = mem
        self._mem_set_during_test = True

    def get_install_cassandra_root(self):
        return self.get_tools_java_dir()

    def get_node_cassandra_root(self):
        return os.path.join(self.get_path())

    def get_conf_dir(self):
        """
        Returns the path to the directory where Cassandra config are located
        """
        return os.path.join(self.get_path(), 'conf')

    def get_tool(self, toolname):
        return common.join_bin(self.get_tools_java_dir(), 'bin', toolname)

    def get_tool_args(self, toolname):
        raise NotImplementedError('ScyllaNode.get_tool_args')

    def get_env(self):
        update_conf = not self.__conf_updated
        if update_conf:
            self.__conf_updated = True
        return common.make_cassandra_env(self.get_install_cassandra_root(),
                                         self.get_node_cassandra_root(), update_conf=update_conf)

    def get_cassandra_version(self):
        # TODO: Handle versioning
        return '3.0'

    def set_log_level(self, new_level, class_name=None):
        known_level = {'TRACE' : 'trace', 'DEBUG' : 'debug', 'INFO' : 'info', 'WARN' : 'warn', 'ERROR' : 'error', 'OFF' : 'info'}
        if not new_level in known_level:
            raise common.ArgumentError("Unknown log level %s (use one of %s)" % (new_level, " ".join(known_level)))

        new_log_level = known_level[new_level]
        # TODO class_name can be validated against help-loggers
        if class_name:
            self.__classes_log_level[class_name] = new_log_level
        else:
            self.__global_log_level = new_log_level
        return self

    def set_workload(self, workload):
        raise NotImplementedError('ScyllaNode.set_workload')

    def cpuset(self, id, count, cluster_id):
        # leaving one core for other executables to run
        allocated_cpus = psutil.cpu_count() - 1
        start_id = (id * count + cluster_id) % allocated_cpus
        cpuset = []
        for cpuid in xrange(start_id, start_id + count):
            cpuset.append(str(cpuid % allocated_cpus))
        return cpuset

    def _wait_for_jmx(self):
        if self._process_jmx:
            self._process_jmx.wait()

    def _wait_for_scylla(self):
        if self._process_scylla:
            self._process_scylla.wait()

    def _wait_for_agent(self):
        if self._process_agent:
            self._process_agent.wait()

    def _start_jmx(self, data):
        jmx_jar_dir = os.path.join(self.get_path(), 'bin')
        jmx_java_bin = os.path.join(jmx_jar_dir, 'symlinks', 'scylla-jmx')
        jmx_jar = os.path.join(jmx_jar_dir, 'scylla-jmx-1.0.jar')
        args = [jmx_java_bin,
                '-Dapiaddress=%s' % data['listen_address'],
                '-Djavax.management.builder.initial=com.scylladb.jmx.utils.APIBuilder',
                '-Dcom.sun.management.jmxremote',
                '-Dcom.sun.management.jmxremote.port=%s' % self.jmx_port,
                '-Dcom.sun.management.jmxremote.rmi.port=%s' % self.jmx_port,
                '-Dcom.sun.management.jmxremote.local.only=false',
                '-Xmx256m',
                '-XX:+UseSerialGC',
                '-Dcom.sun.management.jmxremote.authenticate=false',
                '-Dcom.sun.management.jmxremote.ssl=false',
                '-jar',
                jmx_jar]
        log_file = os.path.join(self.get_path(), 'logs', 'system.log.jmx')
        jmx_log = open(log_file, 'a')
        env_copy = os.environ
        env_copy['SCYLLA_HOME'] = self.get_path()
        self._process_jmx = subprocess.Popen(args, stdout=jmx_log,
                                             stderr=jmx_log,
                                             close_fds=True,
                                             env=env_copy)
        self._process_jmx.poll()
        # When running on ccm standalone, the waiter thread would block
        # the create commands. Besides in that mode, waiting is unnecessary,
        # since the original popen reference is garbage collected.
        standalone = os.environ.get('SCYLLA_CCM_STANDALONE', None)
        if standalone is None:
            self._process_jmx_waiter = threading.Thread(target=self._wait_for_jmx)
            # Don't block the main thread on abnormal shutdown
            self._process_jmx_waiter.daemon = True
            self._process_jmx_waiter.start()
        pid_filename = os.path.join(self.get_path(), 'scylla-jmx.pid')
        with open(pid_filename, 'w') as pid_file:
            pid_file.write(str(self._process_jmx.pid))

    # return True iff detected bootstrap message in the log
    def wait_for_bootstrap_repair(self, from_mark=None, timeout=120):
        if from_mark is None:
            from_mark = self.mark_log()
        process=self._process_scylla
        starting_message = 'Starting listening for CQL clients'
        bootstrap_message = 'storage_service - JOINING: Starting to bootstrap'
        if not self.watch_log_for("{}|{}".format(starting_message, bootstrap_message), from_mark=from_mark, timeout=timeout, process=process):
            return False
        prev_mark = from_mark
        prev_mark_time = time.time()
        sleep_time = 10 if timeout >= 100 else 1
        while not self.grep_log(starting_message, from_mark=from_mark):
            process.poll()
            if process.returncode is not None:
                self.print_process_output(self.name, process, verbose)
                if process.returncode != 0:
                    raise RuntimeError("The process is dead, returncode={}".format(process.returncode))
            repair_pattern = r'repair - Repair \d+ out of \d+ ranges'
            streaming_pattern = r'range_streamer - Bootstrap .* streaming .* ranges'
            if self.grep_log("{}|{}".format(repair_pattern, streaming_pattern), from_mark=prev_mark):
                prev_mark = self.mark_log()
                prev_mark_time = time.time()
            elif time.time() - prev_mark_time >= timeout:
                raise TimeoutError("{}: Timed out waiting for '{}'".format(self.name, starting_message))
            time.sleep(sleep_time)
        return bool(self.grep_log(bootstrap_message, from_mark=from_mark))

    def _start_scylla(self, args, marks, update_pid, wait_other_notice,
                      wait_for_binary_proto, ext_env):
        log_file = os.path.join(self.get_path(), 'logs', 'system.log')
        # In case we are restarting a node
        # we risk reading the old cassandra.pid file
        self._delete_old_pid()

        scylla_log = open(log_file, 'a')
        try:
            env_copy = self._launch_env
        except AttributeError:
            env_copy = os.environ
        env_copy['SCYLLA_HOME'] = self.get_path()
        env_copy.update(ext_env)
        self._process_scylla = subprocess.Popen(args, stdout=scylla_log,
                                                stderr=scylla_log,
                                                close_fds=True,
                                                env=env_copy)
        self._process_scylla.poll()
        # When running on ccm standalone, the waiter thread would block
        # the create commands. Besides in that mode, waiting is unnecessary,
        # since the original popen reference is garbage collected.
        standalone = os.environ.get('SCYLLA_CCM_STANDALONE', None)
        if standalone is None:
            self._process_scylla_waiter = threading.Thread(target=self._wait_for_scylla)
            # Don't block the main thread on abnormal shutdown
            self._process_scylla_waiter.daemon = True
            self._process_scylla_waiter.start()
        pid_filename = os.path.join(self.get_path(), 'cassandra.pid')
        with open(pid_filename, 'w') as pid_file:
            pid_file.write(str(self._process_scylla.pid))

        if update_pid:
            self._update_pid(self._process_scylla)
            if not self.is_running():
                raise NodeError("Error starting node %s" % self.name,
                                self._process_scylla)

        if wait_other_notice:
            for node, mark in marks:
                node.watch_log_for_alive(self, from_mark=mark)

        if wait_for_binary_proto:
            try:
                self.wait_for_binary_interface(from_mark=self.mark, process=self._process_scylla, timeout=420)
            except TimeoutError as e:
                if not self.wait_for_bootstrap_repair(from_mark=self.mark):
                    raise e
                pass

        return self._process_scylla

    def _create_agent_config(self):
        conf_file = os.path.join(self.get_conf_dir(), 'scylla-manager-agent.yaml')
        ssl_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'scylla_test_ssl')

        data = dict()

        data['https'] = "{}:10001".format(self.address())
        data['auth_token'] = self.scylla_manager.auth_token
        data['tls_cert_file'] = os.path.join(ssl_dir, 'scylla-manager-agent.crt')
        data['tls_key_file'] = os.path.join(ssl_dir, 'scylla-manager-agent.key')
        data['logger'] = dict(level='debug')
        data['debug'] = "{}:56112".format(self.address())
        data['scylla'] = {'api_address': "{}".format(self.address()),
                          'api_port': 10000}
        data['prometheus'] = "{}:56090".format(self.address())
        data['s3'] = {"endpoint": os.getenv("AWS_S3_ENDPOINT"), "provider": "Minio"}

        with open(conf_file, 'w') as f:
            yaml.safe_dump(data, f, default_flow_style=False)
        return conf_file

    def start_scylla_manager_agent(self):
        agent_bin = os.path.join(self.scylla_manager._get_path(), 'bin', 'scylla-manager-agent')
        log_file = os.path.join(self.get_path(), 'logs', 'system.log.manager_agent')
        config_file = self._create_agent_config()

        agent_log = open(log_file, 'a')
        args = [agent_bin,
                '--config-file', config_file]
        self._process_agent = subprocess.Popen(args, stdout=agent_log,
                                             stderr=agent_log,
                                             close_fds=True)
        self._process_agent.poll()
        # When running on ccm standalone, the waiter thread would block
        # the create commands. Besides in that mode, waiting is unnecessary,
        # since the original popen reference is garbage collected.
        standalone = os.environ.get('SCYLLA_CCM_STANDALONE', None)
        if standalone is None:
            self._process_agent_waiter = threading.Thread(target=self._wait_for_agent)
            # Don't block the main thread on abnormal shutdown
            self._process_agent_waiter.daemon = True
            self._process_agent_waiter.start()
        pid_filename = os.path.join(self.get_path(), 'scylla-agent.pid')
        with open(pid_filename, 'w') as pid_file:
            pid_file.write(str(self._process_agent.pid))

        api_interface = common.parse_interface(self.address(), 10001)
        if not common.check_socket_listening(api_interface, timeout=180):
            raise Exception(
                "scylla manager agent interface %s:%s is not listening after 180 seconds, scylla manager agent may have failed to start."
                % (api_interface[0], api_interface[1]))
    
    def restart_scylla_manager_agent(self, gently):
        self.stop_scylla_manager_agent(gently=gently)
        
        self.start_scylla_manager_agent()

    def stop_scylla_manager_agent(self, gently):
        if gently:
            try:
                self._process_agent.terminate()
            except OSError:
                pass
        else:
            try:
                self._process_agent.kill()
            except OSError:
                pass

    def _wait_java_up(self, ip_addr, jmx_port):
        java_up = False
        iteration = 0
        while not java_up and iteration < 30:
            iteration += 1
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.settimeout(1.0)
                s.connect((ip_addr, jmx_port))
                java_up = True
                try:
                    s.close()
                except:
                    pass
            except (socket.timeout, ConnectionRefusedError):
                pass
            time.sleep(1)

        return java_up


    # Scylla Overload start
    def start(self, join_ring=True, no_wait=False, verbose=False,
              update_pid=True, wait_other_notice=None, replace_token=None,
              replace_address=None, jvm_args=None, wait_for_binary_proto=None,
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

        Extra command line options may be passed using the
        SCYLLA_EXT_OPTS environment variable.

        Extra environment variables for running scylla can be passed using the
        SCYLLA_EXT_ENV environment variable.
        Those are represented in a single string comprised of one or more
        pairs of "var=value" separated by either space or semicolon (';')
        """
        if wait_for_binary_proto is None:
            wait_for_binary_proto = self.cluster.force_wait_for_cluster_start and not no_wait
        if wait_other_notice is None:
            wait_other_notice = self.cluster.force_wait_for_cluster_start and not no_wait
        if jvm_args is None:
            jvm_args = []

        scylla_cassandra_mapping = {'-Dcassandra.replace_address_first_boot':
                                    '--replace-address-first-boot'}
        # Replace args in the form
        # ['-Dcassandra.foo=bar'] to ['-Dcassandra.foo', 'bar']
        translated_args = []
        new_jvm_args = []
        for jvm_arg in jvm_args:
            if '=' in jvm_arg:
                split_option = jvm_arg.split("=")
                e_msg = ("Option %s not in the form '-Dcassandra.foo=bar'. "
                         "Please check your test" % jvm_arg)
                assert len(split_option) == 2, e_msg
                option, value = split_option
                # If we have information on how to translate the jvm option,
                # translate it
                if option in scylla_cassandra_mapping:
                    translated_args += [scylla_cassandra_mapping[option],
                                        value]
                # Otherwise, just pass it as is
                else:
                    new_jvm_args.append(jvm_arg)
            else:
                new_jvm_args.append(jvm_arg)
        jvm_args = new_jvm_args

        if self.is_running():
            raise NodeError("%s is already running" % self.name)

        for itf in list(self.network_interfaces.values()):
            if itf is not None and replace_address is None:
                try:
                    common.check_socket_available(itf)
                except Exception as msg:
                    print("{}. Looking for offending processes...".format(msg))
                    for proc in psutil.process_iter():
                        if any(self.cluster.ipprefix in cmd for cmd in proc.cmdline()):
                            print("name={} pid={} cmdline={}".format(proc.name(), proc.pid, proc.cmdline()))
                    raise msg

        marks = []
        if wait_other_notice:
            marks = [(node, node.mark_log()) for node in
                     list(self.cluster.nodes.values()) if node.is_live()]

        self.mark = self.mark_log()

        launch_bin = common.join_bin(self.get_path(), 'bin', 'scylla')
        options_file = os.path.join(self.get_path(), 'conf', 'scylla.yaml')

        # TODO: we do not support forcing specific settings
        # TODO: workaround for api-address as we do not load it
        # from config file scylla#59
        conf_file = os.path.join(self.get_conf_dir(), common.SCYLLA_CONF)
        with open(conf_file, 'r') as f:
            data = yaml.safe_load(f)
        jvm_args = jvm_args + ['--api-address', data['api_address']]
        jvm_args = jvm_args + ['--collectd-hostname',
                               '%s.%s' % (socket.gethostname(), self.name)]

        # Let's add jvm_args and the translated args

        args = [launch_bin, '--options-file', options_file, '--log-to-stdout', '1'] + jvm_args + translated_args

        # Lets search for default overrides in SCYLLA_EXT_OPTS
        scylla_ext_opts = os.getenv('SCYLLA_EXT_OPTS', "").split()
        opts_i = 0
        orig_args = list(args)
        while opts_i < len(scylla_ext_opts):
            if scylla_ext_opts[opts_i].startswith("--scylla-manager="):
               opts_i += 1
            elif scylla_ext_opts[opts_i].startswith('-'):
                add = False
                if scylla_ext_opts[opts_i] not in orig_args:
                    add = True
                    args.append(scylla_ext_opts[opts_i])
                opts_i += 1
                while opts_i < len(scylla_ext_opts) and not scylla_ext_opts[opts_i].startswith('-'):
                    if add:
                        args.append(scylla_ext_opts[opts_i])
                    opts_i += 1

        if '--developer-mode' not in args:
            args += ['--developer-mode', 'true']
        if '--smp' not in args:
            # If --smp is not passed from cmdline, use default (--smp 1)
            args += ['--smp', str(self._smp)]
        elif self._smp_set_during_test:
            # If node.set_smp() is called during the test, ignore the --smp
            # passed from the cmdline.
            args[args.index('--smp') + 1] = str(self._smp)
        else:
            # Update self._smp based on command line parameter.
            # It may be used below, along with self._mem_mb_per_cpu, for calculating --memory
            self._smp = int(args[args.index('--smp') + 1])
        if '--memory' not in args:
            # If --memory is not passed from cmdline, use default (512M per cpu)
            args += ['--memory', '{}M'.format(self._mem_mb_per_cpu * self._smp)]
        elif self._mem_set_during_test:
            # If node.set_mem_mb_per_cpu() is called during the test, ignore the --memory
            # passed from the cmdline.
            args[args.index('--memory') + 1] = '{}M'.format(self._mem_mb_per_cpu * self._smp)
        if '--default-log-level' not in args:
            args += ['--default-log-level', self.__global_log_level]
        if self.scylla_mode() == 'debug' and '--blocked-reactor-notify-ms' not in args:
            args += ['--blocked-reactor-notify-ms', '5000']
        # TODO add support for classes_log_level
        if '--collectd' not in args:
            args += ['--collectd', '0']
        if '--cpuset' not in args:
            args += ['--overprovisioned']
        if '--prometheus-address' not in args:
            args += ['--prometheus-address', data['api_address']]
        if replace_address:
            args += ['--replace-address', replace_address]
        args += ['--unsafe-bypass-fsync', '1']

        ext_env = {}
        scylla_ext_env = os.getenv('SCYLLA_EXT_ENV', "").strip()
        if scylla_ext_env:
            scylla_ext_env = re.split(r'[; ]', scylla_ext_env)
            for s in scylla_ext_env:
                try:
                    [k, v] = s.split('=', 1)
                except ValueError as e:
                    print("Bad SCYLLA_EXT_ENV variable: {}: {}", s, e)
                else:
                    ext_env[k] = v

        scylla_process = self._start_scylla(args, marks, update_pid,
                                            wait_other_notice,
                                            wait_for_binary_proto,
                                            ext_env)
        self._start_jmx(data)

        ip_addr = data['listen_address']
        jmx_port = int(self.jmx_port)
        if not self._wait_java_up(ip_addr, jmx_port):
            e_msg = "Error starting node {}: unable to connect to scylla-jmx port {}:{}".format(
                     self.name, ip_addr, jmx_port)
            raise NodeError(e_msg, scylla_process)

        self.is_running()
        if self.scylla_manager and self.scylla_manager.is_agent_available:
            self.start_scylla_manager_agent()
        return scylla_process

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

    def _update_jmx_pid(self, wait=True):
        pidfile = os.path.join(self.get_path(), 'scylla-jmx.pid')

        start = time.time()
        while not (os.path.isfile(pidfile) and os.stat(pidfile).st_size > 0):
            elapsed = time.time() - start
            if elapsed > 30.0 or not wait:
                if wait:
                    print_("Timed out waiting for pidfile {} to be filled (after {} seconds): File {} size={}".format(
                            pidfile,
                            elapsed,
                            'exists' if os.path.isfile(pidfile) else 'does not exist' if not os.path.exists(pidfile) else 'is not a file',
                            os.stat(pidfile).st_size if os.path.exists(pidfile) else -1))
                break
            else:
                time.sleep(0.1)

        if not wait:
            self.jmx_pid = None
            return

        try:
            with open(pidfile, 'r') as f:
                self.jmx_pid = int(f.readline().strip())
        except IOError as e:
            raise NodeError('Problem starting node %s scylla-jmx due to %s' %
                            (self.name, e))

    def _update_scylla_agent_pid(self):
        pidfile = os.path.join(self.get_path(), 'scylla-agent.pid')

        start = time.time()
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
            with open(pidfile, 'r') as f:
                self.agent_pid = int(f.readline().strip())
        except IOError as e:
            raise NodeError('Problem starting node %s scylla-agent due to %s' %
                            (self.name, e))

    def do_stop(self, gently=True):
        """
        Stop the node.
          - gently: Let Scylla and Scylla JMX clean up and shut down properly.
            Otherwise do a 'kill -9' which shuts down faster.
        """

        if not self.is_running():
            return False

        self._update_jmx_pid(wait=False)
        if self.scylla_manager and self.scylla_manager.is_agent_available:
            self._update_scylla_agent_pid()
        for proc in [self._process_jmx, self._process_scylla, self._process_agent]:
            if proc:
                if gently:
                    try:
                        proc.terminate()
                    except OSError:
                        pass
                else:
                    try:
                        proc.kill()
                    except OSError:
                        pass
        else:
            signal_mapping = {True: signal.SIGTERM, False: signal.SIGKILL}
            for pid in [self.jmx_pid, self.pid, self.agent_pid]:
                if pid:
                    try:
                        os.kill(pid, signal_mapping[gently])
                    except OSError:
                        pass

        return True

    def _wait_until_stopped(self, wait_seconds=127):
        start_time = time.time()
        wait_time_sec = 1
        while True:
            if not self.is_running():
                return True
            elapsed = time.time() - start_time
            if elapsed >= wait_seconds:
                return False
            time.sleep(wait_time_sec)
            if elapsed + wait_time_sec > wait_seconds:
                wait_time_sec = wait_seconds - elapsed
            elif wait_time_sec <= 16:
                wait_time_sec *= 2

    def wait_until_stopped(self, wait_seconds=127, marks=[], dump_core=True):
        """
        Wait until node is stopped after do_stop was called.
          - wait_other_notice: return only when the other live nodes of the
            cluster have marked this node has dead.
          - marks: optional list of (node, mark) to call watch_log_for_death on.
        """

        if self.is_running():
            if not self._wait_until_stopped(wait_seconds):
                if self.jmx_pid:
                    try:
                        os.kill(self.jmx_pid, signal.SIGKILL)
                    except OSError:
                        pass
                if dump_core and self.pid:
                    # Aborting is intended to generate a core dump
                    # so the reason the node didn't stop normally can be studied.
                    print("{} is still running. Trying to generate coredump using kill({}, SIGQUIT)...".format(self.name, self.pid))
                    try:
                        os.kill(self.pid, signal.SIGQUIT)
                    except OSError:
                        pass
                    self._wait_until_stopped(300)
                if self.is_running():
                    raise NodeError("Problem stopping node %s" % self.name)

        for node, mark in marks:
            if node != self:
                node.watch_log_for_death(self, from_mark=mark)

    def stop(self, wait=True, wait_other_notice=False, other_nodes=None, gently=True, wait_seconds=127, marks=[]):
        """
        Stop the node.
          - wait: if True (the default), wait for the Scylla process to be
            really dead. Otherwise return after having sent the kill signal.
            stop() will wait up to wait_seconds, by default 127 seconds, for
            the Cassandra process to die. After this wait, it will throw an
            exception stating it couldn't stop the node.
          - wait_other_notice: return only when the other live nodes of the
            cluster have marked this node has dead.
          - other_nodes: optional list of nodes to apply wait_other_notice on.
          - marks: optional list of (node, mark) to call watch_log_for_death on.
          - gently: Let Scylla and Scylla JMX clean up and shut down properly.
            Otherwise do a 'kill -9' which shuts down faster.
        """
        was_running = self.is_running()
        if was_running:
            if wait_other_notice:
                if not other_nodes:
                    other_nodes = list(self.cluster.nodes.values())
                if not marks:
                    marks = [(node, node.mark_log()) for node in
                             other_nodes if
                             node.is_live() and node is not self]
            self.do_stop(gently=gently)

        if wait or wait_other_notice:
            self.wait_until_stopped(wait_seconds, marks, dump_core=gently)

        return was_running

    def import_config_files(self):
        # TODO: override node - enable logging
        self._create_directory()
        self._update_config()
        self.copy_config_files()
        self.update_yaml()
        self.__copy_logback_files()

    def copy_config_files(self):
        Node.copy_config_files(self)
        conf_pattern = os.path.join(self.get_tools_java_dir(), 'conf', "jvm*.options")
        for filename in glob.glob(conf_pattern):
            if os.path.isfile(filename):
                shutil.copy(filename, self.get_conf_dir())

    def get_tools_java_dir(self):
        return common.get_tools_java_dir(self.get_install_dir())

    def get_jmx_dir(self, relative_repos_root):
        return os.environ.get('SCYLLA_JMX_DIR', os.path.join(self.get_install_dir(), relative_repos_root, 'scylla-jmx'))

    def __copy_logback_files(self):
        shutil.copy(os.path.join(self.get_tools_java_dir(), 'conf', 'logback-tools.xml'),
                    os.path.join(self.get_conf_dir(), 'logback-tools.xml'))

    def import_dse_config_files(self):
        raise NotImplementedError('ScyllaNode.import_dse_config_files')

    def copy_config_files_dse(self):
        raise NotImplementedError('ScyllaNode.copy_config_files_dse')

    def hard_link_or_copy(self, src, dst, extra_perms=0, always_copy=False):
        def do_copy(src, dst, extra_perms=0):
            shutil.copy(src, dst)
            os.chmod(dst, os.stat(src).st_mode | extra_perms)

        if always_copy:
            return do_copy(src, dst, extra_perms)
        try:
            os.link(src, dst)
        except OSError as oserror:
            if oserror.errno == errno.EXDEV or oserror.errno == errno.EMLINK:
                do_copy(src, dst, extra_perms)
            else:
                raise RuntimeError("Unable to create hard link from %s to %s: %s" % (src, dst, oserror))

    def import_bin_files(self):
        # selectively copying files to reduce risk of using unintended items
        files = ['cassandra.in.sh', 'nodetool']
        os.makedirs(os.path.join(self.get_path(), 'resources', 'cassandra', 'bin'))

        for name in files:
            self.hard_link_or_copy(os.path.join(self.get_tools_java_dir(),
                                                'bin', name),
                                   os.path.join(self.get_path(),
                                                'resources', 'cassandra',
                                                'bin', name))

        # selectively copying files to reduce risk of using unintended items
        files = ['sstabledump', 'sstablelevelreset', 'sstablemetadata',
                 'sstablerepairedset', 'sstablesplit']
        os.makedirs(os.path.join(self.get_path(), 'resources', 'cassandra',
                                 'tools', 'bin'))
        for name in files:
            self.hard_link_or_copy(os.path.join(self.get_tools_java_dir(),
                                                'tools', 'bin', name),
                                   os.path.join(self.get_path(),
                                                'resources', 'cassandra',
                                                'tools', 'bin', name))

        # TODO: - currently no scripts only executable - copying exec
        if self.is_scylla_reloc():
            relative_repos_root = '../..'
            self.hard_link_or_copy(os.path.join(self.get_install_dir(), 'bin', 'scylla'),
                                   os.path.join(self.get_bin_dir(), 'scylla'),
                                   stat.S_IEXEC)
            os.environ['GNUTLS_SYSTEM_PRIORITY_FILE'] = os.path.join(self.get_install_dir(), 'scylla-core-package/libreloc/gnutls.config')
        else:
            relative_repos_root = '..'
            src = os.path.join(self.get_install_dir(), 'build', self.scylla_mode(), 'scylla')
            dst = os.path.join(self.get_bin_dir(), 'scylla')
            dbuild_so_dir = os.environ.get('SCYLLA_DBUILD_SO_DIR')
            if not dbuild_so_dir:
                self.hard_link_or_copy(src, dst, stat.S_IEXEC)
            else:
                self.hard_link_or_copy(src, dst, stat.S_IEXEC, always_copy=True)

                search_pattern = os.path.join(dbuild_so_dir, 'ld-linux-x86-64.so.*')
                res = glob.glob(search_pattern)
                if not res:
                    raise RuntimeError('{} not found'.format(search_pattern))
                if len(res) > 1:
                    raise RuntimeError('{}: found too make matches: {}'.format(search_pattern, res))
                loader = res[0]

                self._launch_env = dict(os.environ)
                self._launch_env['LD_LIBRARY_PATH'] = dbuild_so_dir

                patchelf_cmd = [loader, os.path.join(dbuild_so_dir, 'patchelf'), '--set-interpreter', loader, dst]
                def run_patchelf(patchelf_cmd):
                    p = subprocess.Popen(patchelf_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self._launch_env)
                    (stdout, stderr) = p.communicate()
                    return (p.returncode, stdout, stderr)

                (returncode, stdout, stderr) = run_patchelf(patchelf_cmd)
                if returncode != 0:
                    # Retry after stripping binary if hit
                    # https://github.com/scylladb/scylla/issues/5245
                    if stderr == 'read\n':
                        cmd = ['strip', dst]
                        subprocess.check_call(cmd)
                        (returncode, stdout, stderr) = run_patchelf(patchelf_cmd)
                if returncode != 0:
                    raise RuntimeError('{} exited with status {}.\nstdout:{}\nstderr:\n{}'.format(patchelf_cmd, returncode, stdout, stderr))

        if 'scylla-repository' in self.get_install_dir():
            self.hard_link_or_copy(os.path.join(self.get_install_dir(), 'scylla-jmx', 'scylla-jmx-1.0.jar'),
                                   os.path.join(self.get_bin_dir(), 'scylla-jmx-1.0.jar'))
            self.hard_link_or_copy(os.path.join(self.get_install_dir(), 'scylla-jmx', 'scylla-jmx'),
                                   os.path.join(self.get_bin_dir(), 'scylla-jmx'))
        else:
            self.hard_link_or_copy(os.path.join(self.get_jmx_dir(relative_repos_root), 'target', 'scylla-jmx-1.0.jar'),
                                   os.path.join(self.get_bin_dir(), 'scylla-jmx-1.0.jar'))
            self.hard_link_or_copy(os.path.join(self.get_jmx_dir(relative_repos_root), 'scripts', 'scylla-jmx'),
                                   os.path.join(self.get_bin_dir(), 'scylla-jmx'))

        os.makedirs(os.path.join(self.get_bin_dir(), 'symlinks'))
        os.symlink('/usr/bin/java', os.path.join(self.get_bin_dir(),
                                                 'symlinks',
                                                 'scylla-jmx'))

        parent_dir = os.path.dirname(os.path.realpath(__file__))
        resources_bin_dir = os.path.join(parent_dir, 'resources', 'bin')
        for name in os.listdir(resources_bin_dir):
            filename = os.path.join(resources_bin_dir, name)
            if os.path.isfile(filename):
                shutil.copy(filename, self.get_bin_dir())
                common.add_exec_permission(self.get_bin_dir(), name)

    def _save(self):
        # TODO: - overwrite node
        self.update_yaml()
        self._update_config()

    def update_yaml(self):
        # TODO: copied from node.py
        conf_file = os.path.join(self.get_conf_dir(), common.SCYLLA_CONF)
        with open(conf_file, 'r') as f:
            data = yaml.safe_load(f)

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
        data['hints_directory'] = os.path.join(self.get_path(), 'hints')
        data['saved_caches_directory'] = os.path.join(self.get_path(),
                                                      'saved_caches')
        data['view_hints_directory'] = os.path.join(self.get_path(), 'view_hints')

        if self.cluster.partitioner:
            data['partitioner'] = self.cluster.partitioner

        # TODO: add scylla options
        data['api_address'] = data['listen_address']
        # last win and we want node options to win
        full_options = dict(list(self.cluster._config_options.items()) +
                            list(self.get_config_options().items()))
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

        if 'alternator_port' in data or 'alternator_https_port' in data:
            data['alternator_address'] = data['listen_address']
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
        for i in ['data', 'commitlogs', 'bin', 'conf', 'logs', 'hints', 'view_hints']:
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

    def _wait_no_pending_flushes(self, wait_timeout=60):
        def no_pending_flushes():
            stdout, _ = self.nodetool('cfstats', timeout=wait_timeout)
            pending_flushes = False
            for line in stdout.splitlines():
                line = line.strip()
                if line.startswith('Pending flushes'):
                    _, pending_flushes_str = line.split(':')
                    pending_flushes_count = int(pending_flushes_str.strip())
                    if pending_flushes_count > 0:
                        pending_flushes = True
            return not pending_flushes
        result = wait_for(no_pending_flushes, timeout=wait_timeout, step=1.0)
        if result is None:
            raise NodeError("Node %s still has pending flushes after "
                            "%s seconds" % (self.name, wait_timeout))

    def flush(self):
        self.nodetool("flush")
        self._wait_no_pending_flushes()
