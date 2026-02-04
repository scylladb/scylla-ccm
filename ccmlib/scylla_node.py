# ccm node


from datetime import datetime
import errno
import functools
import json
import os
import signal
import shutil
import socket
import stat
import subprocess
import time
import threading
from pathlib import Path
from collections import OrderedDict
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union, TYPE_CHECKING
from enum import Enum
import uuid

import logging

import psutil
import glob
import re
import requests
from ruamel.yaml import YAML

from ccmlib.common import CASSANDRA_SH, BIN_DIR, wait_for, copy_directory, print_if_standalone

from ccmlib import common
from ccmlib.node import Node, NodeUpgradeError
from ccmlib.node import Status
from ccmlib.node import NodeError, ToolError
from ccmlib.node import TimeoutError
from ccmlib.scylla_repository import setup, get_scylla_version
from ccmlib.utils.version import parse_version

if TYPE_CHECKING:
    from ccmlib.scylla_cluster import ScyllaCluster


class ScyllaType:
    """
    Helper class for defining Scylla type definitions using type strings.

    Refer to the Cassandra class names found at
    https://github.com/scylladb/scylladb/blob/master/docs/dev/cql3-type-mapping.md for valid type strings.

    Examples :
        type1 = ScyllaType.make_partition_key("Int32Type")
        type2 = ScyllaType.make_clustering_key("Int32Type", "FloatType")
    """
    class TypeKind(Enum):
        REGULAR = 1
        CLUSTERING_KEY = 2
        PARTITION_KEY = 3

    kind: TypeKind
    types: Iterable[str]

    def __init__(self, kind: TypeKind, types: Iterable[str]):
        if not types:
            raise common.ArgumentError("Please pass at least one type to create ScyllaType")
        self.kind = kind
        self.types = types

    @classmethod
    def make_regular(cls, keytype: str):
        return cls(ScyllaType.TypeKind.REGULAR, (keytype,))

    @classmethod
    def make_clustering_key(cls, *keytypes):
        return cls(ScyllaType.TypeKind.CLUSTERING_KEY, keytypes)

    @classmethod
    def make_partition_key(cls, *keytypes):
        return cls(ScyllaType.TypeKind.PARTITION_KEY, keytypes)

    def as_types_args(self) -> List[str]:
        """Return the type definition as arguments to scylla types command"""
        args = []
        if self.kind == ScyllaType.TypeKind.CLUSTERING_KEY:
            args.append("--prefix-compound")
        elif self.kind == ScyllaType.TypeKind.PARTITION_KEY:
            args.append("--full-compound")
        for keytype in self.types:
            args.extend(["-t", keytype])
        return args


# Mapping of short option aliases to their long form equivalents
OPTION_ALIASES = {
    '-c': '--smp',
    '-m': '--memory',
}


def process_opts(opts):
    """
    Process command line options, normalizing short form to long form.

    Parses command line options that show up either like "--foo value-of-foo"
    or as a single option like "--yes-i-insist". Short options (-c, -m) are
    normalized to their long form equivalents (--smp, --memory).

    Args:
        opts: List of command line option strings

    Returns:
        OrderedDict mapping option keys to lists of values
    """
    ext_args = OrderedDict()
    opts_i = 0
    while opts_i < len(opts):
        # the command line options show up either like "--foo value-of-foo"
        # or as a single option like --yes-i-insist
        assert opts[opts_i].startswith('-')
        o = opts[opts_i]
        opts_i += 1
        if '=' in o:
            key, val = o.split('=', 1)
        else:
            key = o
            vals = []
            while opts_i < len(opts) and not opts[opts_i].startswith('-'):
                vals.append(opts[opts_i])
                opts_i += 1
            val = ' '.join(vals)
        # Normalize short option aliases to their long form
        key = OPTION_ALIASES.get(key, key)
        if not key.startswith("--scylla-manager"):
            ext_args.setdefault(key, []).append(val)
    return ext_args


class ScyllaNode(Node):

    """
    Provides interactions to a Scylla node.
    """

    def __init__(self, name, cluster: 'ScyllaCluster', auto_bootstrap,
                 storage_interface, jmx_port, remote_debug_port, initial_token,
                 save=True, binary_interface=None, scylla_manager=None, thrift_interface=None):
        self._node_install_dir = None
        self._node_scylla_version = None
        self._relative_repos_root = None
        self._launch_env = None
        super().__init__(name, cluster, auto_bootstrap,
                         storage_interface,
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
        self._smp = 2
        self._smp_set_during_test = False
        self._mem_mb_per_cpu = 512
        self._mem_mb_set_during_test = False
        self._memory = None
        self.__conf_updated = False
        self.scylla_manager = scylla_manager
        self.jmx_pid = None
        self.agent_pid = None
        self.upgraded = False
        self.upgrader = NodeUpgrader(node=self)
        self.node_hostid = None
        self._create_directory()

    @property
    def node_install_dir(self):
        if not self._node_install_dir:
            self._node_install_dir = self.get_install_dir()
        return self._node_install_dir

    @node_install_dir.setter
    def node_install_dir(self, install_dir):
        self._node_install_dir = install_dir
        # Force re-check based on the new install dir
        del self.has_jmx

    @property
    def node_scylla_version(self):
        if not self._node_scylla_version:
            self._node_scylla_version = self.get_node_scylla_version()
        return self._node_scylla_version

    @node_scylla_version.setter
    def node_scylla_version(self, install_dir):
        self._node_scylla_version = self.get_node_scylla_version(install_dir)

    @property
    def scylla_build_id(self):
        return self._run_scylla_executable_with_option(option="--build-id")

    @functools.cached_property
    def has_jmx(self):
        install_dir = self.node_install_dir
        if self.is_scylla_reloc():
            return os.path.isdir(os.path.join(install_dir, "jmx"))
        else:
            return os.path.isdir(os.path.join(install_dir, "tools", "jmx"))

    @property
    def scylla_yaml(self) -> Dict[str, Any]:
        return YAML().load(Path(self.get_conf_dir()) / common.SCYLLA_CONF)

    @property
    def api_port(self) -> int:
        return self.scylla_yaml.get('api_port', 10000)

    def scylla_mode(self):
        return self.cluster.get_scylla_mode()

    def is_scylla_reloc(self):
        return self.cluster.is_scylla_reloc()

    def set_smp(self, smp):
        self._smp = smp
        self._smp_set_during_test = True

    def smp(self):
        return self._smp

    def set_mem_mb_per_cpu(self, mem):
        self._mem_mb_per_cpu = mem
        self._mem_mb_set_during_test = True

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
        candidate_dirs = [
            os.path.join(self.node_install_dir, 'share', 'cassandra', BIN_DIR),
            os.path.join(self.get_cqlsh_dir(), BIN_DIR),
        ]
        tools_java_dir = self.get_tools_java_dir()
        if tools_java_dir:
            candidate_dirs.append(os.path.join(tools_java_dir, BIN_DIR))
        for candidate_dir in candidate_dirs:
            candidate = shutil.which(toolname, path=candidate_dir)
            if candidate:
                return candidate
        raise ValueError(f"tool {toolname} wasn't found in any path: {candidate_dirs}")

    def get_tool_args(self, toolname):
        raise NotImplementedError('ScyllaNode.get_tool_args')

    def get_env(self):
        if self._launch_env:
            return self._launch_env
        update_conf = not self.__conf_updated
        if update_conf:
            self.__conf_updated = True
        install_cassandra_root = self.get_install_cassandra_root()
        if not install_cassandra_root:
            return os.environ.copy()
        return common.make_cassandra_env(install_cassandra_root,
                                         self.get_node_cassandra_root(), update_conf=update_conf)

    def _get_environ(self, extra_env = None, /, **kwargs):
        env = self._launch_env or dict(os.environ)
        if extra_env is not None:
            env.update(extra_env)
        env.update(kwargs)
        return env

    def get_cassandra_version(self):
        # TODO: Handle versioning
        return '3.0'

    def set_log_level(self, new_level, class_name=None):
        known_level = {'TRACE' : 'trace', 'DEBUG' : 'debug', 'INFO' : 'info', 'WARN' : 'warn', 'ERROR' : 'error', 'OFF' : 'info'}
        if new_level not in known_level:
            raise common.ArgumentError(f"Unknown log level {new_level} (use one of {' '.join(known_level)})")

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
        for cpuid in range(start_id, start_id + count):
            cpuset.append(str(cpuid % allocated_cpus))
        return cpuset

    def memory(self):
        return self._memory

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
        jmx_jar_dir = os.path.join(self.get_path(), BIN_DIR)
        jmx_java_bin = os.path.join(jmx_jar_dir, 'symlinks', 'scylla-jmx')
        jmx_jar = os.path.join(jmx_jar_dir, 'scylla-jmx-1.0.jar')
        args = [jmx_java_bin,
                f"-Dapiaddress={data['listen_address']}",
                '-Djavax.management.builder.initial=com.scylladb.jmx.utils.APIBuilder',
                f"-Djava.rmi.server.hostname={data['listen_address']}",
                '-Dcom.sun.management.jmxremote',
                f"-Dcom.sun.management.jmxremote.host={data['listen_address']}",
                f'-Dcom.sun.management.jmxremote.port={self.jmx_port}',
                f'-Dcom.sun.management.jmxremote.rmi.port={self.jmx_port}',
                '-Dcom.sun.management.jmxremote.local.only=false',
                '-Xmx256m',
                '-XX:+UseSerialGC',
                '-Dcom.sun.management.jmxremote.authenticate=false',
                '-Dcom.sun.management.jmxremote.ssl=false',
                '-jar',
                jmx_jar]
        log_file = os.path.join(self.get_path(), 'logs', 'system.log.jmx')
        env = self._get_environ(SCYLLA_HOME=self.get_path())

        message = f"Starting scylla-jmx: args={args}"
        self.debug(message)
        with open(log_file, 'a') as jmx_log:
            jmx_log.write(f"{message}\n")
            self._process_jmx = subprocess.Popen(args, stdout=jmx_log, stderr=jmx_log, close_fds=True, env=env)
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

    # Wait for a starting node until is starts listening for CQL.
    # Possibly poll the log for long-running offline processes, like
    # bootstrap or resharding.
    # Return True iff detected bootstrap or resharding processes in the log
    def wait_for_starting(self, from_mark=None, timeout=None):
        if from_mark is None:
            from_mark = self.mark_log()
        if timeout is None:
            timeout = 120
        process=self._process_scylla
        starting_message = 'Starting listening for CQL clients'
        bootstrap_message = r'storage_service .* Starting to bootstrap'
        resharding_message = r'(compaction|database) -.*Resharding'
        if not self.watch_log_for(f"{starting_message}|{bootstrap_message}|{resharding_message}", from_mark=from_mark, timeout=timeout, process=process):
            return False
        prev_mark = from_mark
        prev_mark_time = time.time()
        sleep_time = 10 if timeout >= 100 else 1
        while not self.grep_log(starting_message, from_mark=from_mark):
            process.poll()
            if process.returncode is not None:
                self.print_process_output(self.name, process, verbose=True)
                if process.returncode != 0:
                    raise RuntimeError(f"The process is dead, returncode={process.returncode}")
            pat = '|'.join([
                r'repair - Repair \d+ out of \d+ ranges',
                r'repair - .*: Started to repair',
                r'range_streamer - Bootstrap .* streaming .* ranges',
                r'(compaction|database) -.*Resharded',
                r'compaction_manager - (Starting|Done with) off-strategy compaction',
                r'compaction - .* Reshaped'
            ])
            if self.grep_log(pat, from_mark=prev_mark):
                prev_mark = self.mark_log()
                prev_mark_time = time.time()
            elif time.time() - prev_mark_time >= timeout:
                raise TimeoutError(f"{self.name}: Timed out waiting for '{starting_message}'")
            time.sleep(sleep_time)
        return bool(self.grep_log(f"{bootstrap_message}|{resharding_message}", from_mark=from_mark))

    def _start_scylla(self, args, marks: List[Tuple['ScyllaNode', int]], update_pid,
                      wait_other_notice, wait_normal_token_owner,
                      wait_for_binary_proto, ext_env):
        log_file = os.path.join(self.get_path(), 'logs', 'system.log')
        # In case we are restarting a node
        # we risk reading the old cassandra.pid file
        self._delete_old_pid()

        env = self._get_environ(ext_env, SCYLLA_HOME=self.get_path())

        with open(log_file, 'a') as scylla_log:
            self._process_scylla = \
                subprocess.Popen(args, stdout=scylla_log, stderr=scylla_log, close_fds=True, env=env)
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
                raise NodeError(f"Error starting node {self.name}",
                                self._process_scylla)

        # Reset self.node_hostid so it will be retrieved again once the node restarts
        # since it might restart with a different host_id than it previously
        # had if it was wiped and reused for bootstrap / replace.
        self.node_hostid = None

        if wait_for_binary_proto:
            t = self.cluster.default_wait_for_binary_proto
            from_mark = self.mark
            try:
                self.wait_for_binary_interface(from_mark=from_mark, process=self._process_scylla, timeout=t)
            except TimeoutError:
                self.wait_for_starting(from_mark=self.mark, timeout=t)
                self.wait_for_binary_interface(from_mark=from_mark, process=self._process_scylla, timeout=t)

        if wait_other_notice:
            for node, mark in marks:
                t = self.cluster.default_wait_other_notice_timeout
                node.watch_log_for_alive(self, from_mark=mark, timeout=t)
                node.watch_rest_for_alive(self, timeout=t, wait_normal_token_owner=wait_normal_token_owner)
                self.watch_rest_for_alive(node, timeout=t, wait_normal_token_owner=wait_normal_token_owner)

        return self._process_scylla

    def _create_agent_config(self):
        conf_file = os.path.join(self.get_conf_dir(), 'scylla-manager-agent.yaml')
        ssl_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'scylla_test_ssl')

        data = dict()

        data['https'] = f"{self.address()}:10001"
        data['auth_token'] = self.scylla_manager.auth_token
        data['tls_cert_file'] = os.path.join(ssl_dir, 'scylla-manager-agent.crt')
        data['tls_key_file'] = os.path.join(ssl_dir, 'scylla-manager-agent.key')
        data['logger'] = dict(level='debug')
        data['debug'] = f"{self.address()}:56112"
        data['scylla'] = {'api_address': f"{self.address()}",
                          'api_port': self.api_port}
        data['prometheus'] = f"{self.address()}:56090"
        data['s3'] = {"endpoint": os.getenv("AWS_S3_ENDPOINT"), "provider": "Minio"}

        with open(conf_file, 'w') as f:
            YAML().dump(data, f)
        return conf_file

    def update_agent_config(self, new_settings, restart_agent_after_change=True):
        conf_file = os.path.join(self.get_conf_dir(), 'scylla-manager-agent.yaml')
        with open(conf_file, 'r') as f:
            current_config = YAML().load(f)

        current_config.update(new_settings)

        with open(conf_file, 'w') as f:
            YAML().dump(current_config, f)

        if restart_agent_after_change:
            self.restart_scylla_manager_agent(gently=True, recreate_config=False)

    def start_scylla_manager_agent(self, create_config=True):
        agent_bin = os.path.join(self.scylla_manager._get_path(), BIN_DIR, 'scylla-manager-agent')
        log_file = os.path.join(self.get_path(), 'logs', 'system.log.manager_agent')
        if create_config:
            config_file = self._create_agent_config()
        else:
            config_file = os.path.join(self.get_conf_dir(), 'scylla-manager-agent.yaml')

        args = [agent_bin,
                '--config-file', config_file]

        with open(log_file, 'a') as agent_log:
            self._process_agent = subprocess.Popen(args, stdout=agent_log, stderr=agent_log, close_fds=True)
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

        with open(config_file, 'r') as f:
            current_config = YAML().load(f)
            # Extracting currently configured port
            current_listening_port = int(current_config['https'].split(":")[1])

        api_interface = common.parse_interface(self.address(), current_listening_port)
        if not common.check_socket_listening(api_interface, timeout=180):
            raise Exception(
                "scylla manager agent interface %s:%s is not listening after 180 seconds, scylla manager agent may have failed to start."
                % (api_interface[0], api_interface[1]))
    
    def restart_scylla_manager_agent(self, gently, recreate_config=True):
        self.stop_scylla_manager_agent(gently=gently)
        
        self.start_scylla_manager_agent(create_config=recreate_config)

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
        def is_java_up():
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as _socket:
                _socket.settimeout(1.0)
                try:
                    _socket.connect((ip_addr, jmx_port))
                    return True
                except (socket.timeout, ConnectionRefusedError):
                    return False

        return wait_for(func=is_java_up, timeout=30, step=0.05)

    def node_install_dir_version(self):
        if not self.node_install_dir:
            return None
        return get_scylla_version(self.node_install_dir)

    @staticmethod
    def parse_size(s):
        iec_prefixes = {'k': 10,
                        'K': 10,
                        'M': 20,
                        'G': 30,
                        'T': 40}
        for prefix, power in list(iec_prefixes.items()):
            if s.endswith(prefix):
                return int(s[:-1]) * pow(2, power)
        return int(s)

    # Scylla Overload start
    def start(self, join_ring=True, no_wait=False, verbose=False,
              update_pid=True, wait_other_notice=None, wait_normal_token_owner=None, replace_token=None,
              replace_address=None, replace_node_host_id=None, jvm_args=None, wait_for_binary_proto=None,
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
          - wait_normal_token_owner: if wait_other_notice is True and wait_normal_token_owner
            is True or None, this method returns only when all other nodes see this node as normal
            token owner, and vice-versa
          - replace_token: start the node with the -Dcassandra.replace_token
            option.
          - replace_node_host_id: start the node with the
            --replace-node-first-boot option to replace a given node
            identified by its host_id.
          - replace_address: start the node with the deprecated
            --replace-address option.

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
        if wait_normal_token_owner is None and wait_other_notice:
            wait_normal_token_owner = True
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
            raise NodeError(f"{self.name} is already running")

        if not self.is_docker():
            for itf in list(self.network_interfaces.values()):
                if itf is not None and replace_address is None:
                    try:
                        common.check_socket_available(itf)
                    except Exception as msg:
                        print_if_standalone(f"{msg}. Looking for offending processes...", debug_callback=logging.error)
                        for proc in psutil.process_iter():
                            if any(self.cluster.get_ipprefix() in cmd for cmd in proc.cmdline()):
                                print_if_standalone(f"name={proc.name()} pid={proc.pid} cmdline={proc.cmdline()}",  debug_callback=logging.error)
                        raise msg

        marks = []
        if wait_other_notice:
            marks = [(node, node.mark_log()) for node in
                     list(self.cluster.nodes.values()) if node.is_live()]

        self.mark = self.mark_log()

        launch_bin = common.join_bin(self.get_path(), BIN_DIR, 'scylla')
        options_file = os.path.join(self.get_path(), 'conf', 'scylla.yaml')

        # TODO: we do not support forcing specific settings
        # TODO: workaround for api-address as we do not load it
        # from config file scylla#59
        conf_file = os.path.join(self.get_conf_dir(), common.SCYLLA_CONF)
        with open(conf_file, 'r') as f:
            data = YAML().load(f)
        jvm_args = jvm_args + ['--api-address', data['api_address']]

        args = [launch_bin, '--options-file', options_file, '--log-to-stdout', '1']

        MB = 1024 * 1024

        # Lets search for default overrides in SCYLLA_EXT_OPTS
        env_args = process_opts(os.getenv('SCYLLA_EXT_OPTS', "").split())

        # precalculate self._mem_mb_per_cpu if --memory is given in SCYLLA_EXT_OPTS
        # and it wasn't set explicitly by the test
        if not self._mem_mb_set_during_test and '--memory' in env_args:
            memory = self.parse_size(env_args['--memory'][0])
            smp = int(env_args['--smp'][0]) if '--smp' in env_args else self._smp
            self._mem_mb_per_cpu = int((memory / smp) // MB)

        cmd_args = process_opts(jvm_args)

        # use '--memory' in jmv_args if mem_mb_per_cpu was not set by the test
        if not self._mem_mb_set_during_test and '--memory' in cmd_args:
            self._memory = self.parse_size(cmd_args['--memory'][0])

        # use '--smp' in jvm_args if was not set by the test
        if not self._smp_set_during_test and '--smp' in cmd_args:
            self._smp = int(cmd_args['--smp'][0])

        ext_args = env_args
        ext_args.update(cmd_args)
        for k, v in ext_args.items():
            if k in ('--memory', '--smp'):
                continue

            if v and all(v):
                for val in v:
                    args += [k, val]
            else:
                args.append(k)

        args.extend(translated_args)

        # calculate memory from smp * mem_mb_per_cpu
        # if not given in jvm_args
        if not self._memory:
            self._memory = int(self._smp * self._mem_mb_per_cpu * MB)

        assert '--smp' not in args and '--memory' not in args, args
        args += ['--smp', str(self._smp)]
        args += ['--memory', f"{int(self._memory // MB)}M"]

        if '--developer-mode' not in args:
            args += ['--developer-mode', 'true']
        if '--default-log-level' not in args:
            args += ['--default-log-level', self.__global_log_level]
        if self.scylla_mode() == 'debug' and '--blocked-reactor-notify-ms' not in args:
            args += ['--blocked-reactor-notify-ms', '5000']
        # TODO add support for classes_log_level
        if '--cpuset' not in args:
            args += ['--overprovisioned']
        if '--prometheus-address' not in args:
            args += ['--prometheus-address', data['api_address']]
        if replace_node_host_id:
            assert replace_address is None, "replace_node_host_id and replace_address cannot be specified together"
            args += ['--replace-node-first-boot', replace_node_host_id]
        elif replace_address:
            args += ['--replace-address', replace_address]
        args += ['--unsafe-bypass-fsync', '1']

        current_node_version = self.node_install_dir_version() or self.cluster.version()
        current_node_is_enterprise = parse_version(current_node_version) > parse_version("2018.1")

        # The '--kernel-page-cache' was introduced by
        # https://github.com/scylladb/scylla/commit/8785dd62cb740522d80eb12f8272081f85be9b7e from 4.5 version 
        # and 2022.1 Enterprise version
        kernel_page_cache_supported = not current_node_is_enterprise and parse_version(current_node_version) >= parse_version('4.5.dev')
        kernel_page_cache_supported |= current_node_is_enterprise and parse_version(current_node_version) >= parse_version('2022.1.dev')
        if kernel_page_cache_supported and '--kernel-page-cache' not in args:
            args += ['--kernel-page-cache', '1']
        commitlog_o_dsync_supported = (
            (not current_node_is_enterprise and parse_version(current_node_version) >= parse_version('3.2'))
            or (current_node_is_enterprise and parse_version(current_node_version) >= parse_version('2020.1'))
            )
        if commitlog_o_dsync_supported:
            args += ['--commitlog-use-o-dsync', '0']

        # The '--max-networking-io-control-blocks' was introduced by
        # https://github.com/scylladb/scylla/commit/2cfc517874e98c5780c1b1b4c38440a8123f86f6 from 4.6 version
        # and 2022.1 Enterprise version
        max_networking_io_control_blocks_supported = not current_node_is_enterprise and parse_version(current_node_version) >= parse_version('4.6')
        max_networking_io_control_blocks_supported |= current_node_is_enterprise and parse_version(current_node_version) >= parse_version('2022.1.dev')        
        if max_networking_io_control_blocks_supported and '--max-networking-io-control-blocks' not in args:
            args += ['--max-networking-io-control-blocks', '1000']

        if '--rf-rack-valid-keyspaces' in args and parse_version(current_node_version) <= parse_version('2025.1'):
            arg_index = args.index('--rf-rack-valid-keyspaces')
            del args[arg_index+1],args[arg_index]
        
        ext_env = {}
        scylla_ext_env = os.getenv('SCYLLA_EXT_ENV', "").strip()
        if scylla_ext_env:
            scylla_ext_env = re.split(r'[; ]', scylla_ext_env)
            for s in scylla_ext_env:
                try:
                    [k, v] = s.split('=', 1)
                except ValueError as e:
                    print(f"Bad SCYLLA_EXT_ENV variable: {s}: {e}")
                else:
                    ext_env[k] = v

        message = f"Starting scylla: args={args} wait_other_notice={wait_other_notice} wait_for_binary_proto={wait_for_binary_proto}"
        self.debug(message)

        scylla_process = self._start_scylla(args=args, marks=marks, update_pid=update_pid,
                                            wait_other_notice=wait_other_notice,
                                            wait_normal_token_owner=wait_normal_token_owner,
                                            wait_for_binary_proto=wait_for_binary_proto,
                                            ext_env=ext_env)
        self.info(f"Started scylla: pid: {scylla_process.pid}")

        if self.has_jmx:
            self._start_jmx(data)

            ip_addr, _ = self.network_interfaces['storage']
            jmx_port = int(self.jmx_port)
            if not self._wait_java_up(ip_addr, jmx_port):
                e_msg = "Error starting node {}: unable to connect to scylla-jmx port {}:{}".format(
                         self.name, ip_addr, jmx_port)
                raise NodeError(e_msg, scylla_process)

        self._update_pid(scylla_process)
        wait_for(func=lambda: self.is_running(), timeout=10, step=0.01)

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
                    print("Timed out waiting for pidfile {} to be filled (after {} seconds): File {} size={}".format(
                            pidfile,
                            elapsed,
                            'exists' if os.path.isfile(pidfile) else 'does not exist' if not os.path.exists(pidfile) else 'is not a file',
                            os.stat(pidfile).st_size if os.path.exists(pidfile) else -1))
                break
            else:
                time.sleep(0.1)

        if os.path.isfile(pidfile) and os.stat(pidfile).st_size > 0:
            try:
                with open(pidfile, 'r') as f:
                    self.jmx_pid = int(f.readline().strip())
            except IOError as e:
                raise NodeError('Problem starting node %s scylla-jmx due to %s' %
                                (self.name, e))
        else:
            self.jmx_pid = None

    def nodetool(self, cmd, capture_output=True, wait=True, timeout=None, verbose=True):
        try:
            nodetool = [os.path.join(self.get_bin_dir(), "scylla"), "nodetool"]
            # some nodetool commands were added in 5.5, but we could test 5.4 in upgrade
            # tests, so check if this command is available before using it.
            command = next(arg for arg in cmd.split() if not arg.startswith('-'))
            subprocess.check_call(nodetool + [command, '--help'], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, env=self.get_env())
            if self.is_docker():
                host = 'localhost'
            else:
                host = self.address()
            nodetool.extend(['-h', host, '-p', str(self.api_port)])
            nodetool.extend(cmd.split())
            return self._do_run_nodetool(nodetool, capture_output, wait, timeout, verbose)
        except subprocess.CalledProcessError:
            pass

        # the java nodetool depends on JMX
        assert self.has_jmx
        # Fall-back to the java nodetool for pre 5.5.0~dev versions, which don't yet have the native nodetool
        # Kill scylla-jmx in case of timeout, to supply enough debugging information

        # pass the api_port to nodetool. if it is the nodetool-wrapper. it should
        # interpret the command line and use it for the -p option
        cmd = f"-Dcom.scylladb.apiPort={self.api_port} {cmd}"
        try:
            return super().nodetool(cmd, capture_output, wait, timeout, verbose)
        except subprocess.TimeoutExpired:
            self.error("nodetool timeout, going to kill scylla-jmx with SIGQUIT")
            self.kill_jmx(signal.SIGQUIT)
            time.sleep(5)  # give the java process time to print the threaddump into the log
            raise

    def kill_jmx(self, __signal):
        if self.jmx_pid:
            os.kill(self.jmx_pid, __signal)

    def _update_scylla_agent_pid(self):
        pidfile = os.path.join(self.get_path(), 'scylla-agent.pid')

        start = time.time()
        while not (os.path.isfile(pidfile) and os.stat(pidfile).st_size > 0):
            if time.time() - start > 30.0:
                print("Timed out waiting for pidfile {} to be filled (current time is {}): File {} size={}".format(
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

        did_stop = False
        if self.has_jmx:
            self._update_jmx_pid(wait=False)
        if self.scylla_manager and self.scylla_manager.is_agent_available:
            self._update_scylla_agent_pid()
        for proc in [self._process_jmx, self._process_scylla, self._process_agent]:
            if proc:
                did_stop = True
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
                    did_stop = True
                    try:
                        os.kill(pid, signal_mapping[gently])
                    except OSError:
                        pass

        return did_stop

    def _wait_until_stopped(self, wait_seconds):
        return wait_for(func=lambda: not self.is_running(), timeout=wait_seconds)

    def wait_until_stopped(self, wait_seconds=None, marks: List[Tuple['ScyllaNode', int]]=None, dump_core=True):
        """
        Wait until node is stopped after do_stop was called.
          - wait_other_notice: return only when the other live nodes of the
            cluster have marked this node has dead.
          - marks: optional list of (node, mark) to call watch_log_for_death on.
        """
        marks = marks or []
        if wait_seconds is None:
            wait_seconds = 127 if self.scylla_mode() != 'debug' else 600

        start = time.time()
        if self.is_running():
            if not self._wait_until_stopped(wait_seconds):
                if dump_core and self.pid:
                    # Aborting is intended to generate a core dump
                    # so the reason the node didn't stop normally can be studied.
                    self.warning("{} is still running after {} seconds. Trying to generate coredump using kill({}, SIGQUIT)...".format(
                        self.name, wait_seconds, self.pid))
                    try:
                        os.kill(self.pid, signal.SIGQUIT)
                    except OSError:
                        pass
                    self._wait_until_stopped(300)
                if self.is_running() and self.pid:
                    self.warning("{} is still running after {} seconds. Killing process using kill({}, SIGKILL)...".format(
                        self.name, wait_seconds, self.pid))
                    os.kill(self.pid, signal.SIGKILL)
                    self._wait_until_stopped(10)

        while self.jmx_pid and time.time() - start < wait_seconds:
            try:
                os.kill(self.jmx_pid, 0)
                time.sleep(1)
            except OSError:
                self.jmx_pid = None
                pass

        if self.jmx_pid:
            try:
                self.warning("{} scylla-jmx is still running. Killing process using kill({}, SIGKILL)...".format(
                    self.name, self.jmx_pid))
                os.kill(self.jmx_pid, signal.SIGKILL)
            except OSError:
                pass

        if self.is_running():
            raise NodeError(f"Problem stopping node {self.name}")

        for node, mark in marks:
            if node != self:
                node.watch_log_for_death(self, from_mark=mark)

    def stop(self, wait=True, wait_other_notice=False, other_nodes: List['ScyllaNode']=None, gently=True, wait_seconds=None, marks=None):
        """
        Stop the node.
          - wait: if True (the default), wait for the Scylla process to be
            really dead. Otherwise return after having sent the kill signal.
            stop() will wait up to wait_seconds, by default 127 seconds
            (or 600 in debug mode), for the Scylla process to stop gracefully.
            After this wait, it will try to kill the node using SIGQUIT,
            and if that failed, it will throw an
            exception stating it couldn't stop the node.
          - wait_other_notice: return only when the other live nodes of the
            cluster have marked this node has dead.
          - other_nodes: optional list of nodes to apply wait_other_notice on.
          - marks: optional list of (node, mark) to call watch_log_for_death on.
          - gently: Let Scylla and Scylla JMX clean up and shut down properly.
            Otherwise do a 'kill -9' which shuts down faster.
        """
        marks = marks or []
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
        tools_java_dir = self.get_tools_java_dir()
        if not tools_java_dir:
            # in newer scylla, the java-base scylla-tools is dropped, so this
            # directory cannot be found in that case.
            return
        conf_pattern = os.path.join(tools_java_dir, 'conf', "jvm*.options")
        for filename in glob.glob(conf_pattern):
            if os.path.isfile(filename):
                shutil.copy(filename, self.get_conf_dir())

    def get_tools_java_dir(self):
        return common.get_tools_java_dir(self.node_install_dir, self._relative_repos_root or '..')

    def get_jmx_dir(self):
        return common.get_jmx_dir(self.node_install_dir, self._relative_repos_root or '..')

    def get_cqlsh_dir(self):
        return os.path.join(self.node_install_dir, 'tools', 'cqlsh')

    def __copy_logback_files(self):
        tools_java_dir = self.get_tools_java_dir()
        if not tools_java_dir:
            return
        shutil.copy(os.path.join(tools_java_dir, 'conf', 'logback-tools.xml'),
                    os.path.join(self.get_conf_dir(), 'logback-tools.xml'))

    def import_dse_config_files(self):
        raise NotImplementedError('ScyllaNode.import_dse_config_files')

    def copy_config_files_dse(self):
        raise NotImplementedError('ScyllaNode.copy_config_files_dse')

    def clean_runtime_file(self):
        """Remove cassandra.in.sh file that created runtime during cluster build """
        cassandra_in_sh = os.path.join(self.get_node_cassandra_root(), BIN_DIR, CASSANDRA_SH)
        if os.path.exists(cassandra_in_sh):
            os.remove(cassandra_in_sh)

    def hard_link_or_copy(self, src, dst, extra_perms=0, always_copy=False, replace=False):
        def do_copy(src, dst, extra_perms=0):
            shutil.copy(src, dst)
            os.chmod(dst, os.stat(src).st_mode | extra_perms)

        if always_copy:
            return do_copy(src, dst, extra_perms)

        if os.path.exists(dst) and replace:
            os.remove(dst)

        try:
            os.link(src, dst)
        except OSError as oserror:
            if oserror.errno == errno.EXDEV or oserror.errno == errno.EMLINK:
                do_copy(src, dst, extra_perms)
            else:
                raise RuntimeError(f"Unable to create hard link from {src} to {dst}: {oserror}")

    def _copy_binaries(self, files, src_path, dest_path, exist_ok=False, replace=False, extra_perms=0):
        os.makedirs(dest_path, exist_ok=exist_ok)

        for name in files:
            self.hard_link_or_copy(src=os.path.join(src_path, name),
                                   dst=os.path.join(dest_path, name),
                                   extra_perms=extra_perms,
                                   replace=replace)


    def import_bin_files(self, exist_ok=False, replace=False):
        # selectively copying files to reduce risk of using unintended items
        tools_java_dir = self.get_tools_java_dir()
        if tools_java_dir:
            self._copy_binaries(files=[CASSANDRA_SH, 'nodetool'],
                                src_path=os.path.join(tools_java_dir, BIN_DIR),
                                dest_path=os.path.join(self.get_path(), 'resources', 'cassandra', BIN_DIR),
                                exist_ok=exist_ok,
                                replace=replace
                                )

            # selectively copying files to reduce risk of using unintended items
            # Copy sstable tools
            self._copy_binaries(files=['sstabledump', 'sstablelevelreset', 'sstablemetadata',
                                       'sstablerepairedset', 'sstablesplit'],
                                src_path=os.path.join(tools_java_dir, 'tools', BIN_DIR),
                                dest_path=os.path.join(self.get_path(), 'resources', 'cassandra', 'tools', BIN_DIR),
                                exist_ok=exist_ok,
                                replace=replace
                                )

        # TODO: - currently no scripts only executable - copying exec
        if self.is_scylla_reloc():
            self._relative_repos_root = '../..'
            self.hard_link_or_copy(src=os.path.join(self.node_install_dir, BIN_DIR, 'scylla'),
                                   dst=os.path.join(self.get_bin_dir(), 'scylla'),
                                   extra_perms=stat.S_IEXEC,
                                   replace=replace)
            os.environ['GNUTLS_SYSTEM_PRIORITY_FILE'] = self.gnutls_config_file
        else:
            self._relative_repos_root = '..'
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
                    raise RuntimeError(f'{search_pattern} not found')
                if len(res) > 1:
                    raise RuntimeError(f'{search_pattern}: found too make matches: {res}')
                loader = res[0]


                self._launch_env = self._get_environ(LD_LIBRARY_PATH=dbuild_so_dir)

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
                    raise RuntimeError(f'{patchelf_cmd} exited with status {returncode}.\nstdout:{stdout}\nstderr:\n{stderr}')

        if self.has_jmx:
            if 'scylla-repository' in self.node_install_dir:
                self.hard_link_or_copy(os.path.join(self.get_jmx_dir(), 'scylla-jmx-1.0.jar'),
                                       os.path.join(self.get_bin_dir(), 'scylla-jmx-1.0.jar'), replace=replace)
                self.hard_link_or_copy(os.path.join(self.get_jmx_dir(), 'scylla-jmx'),
                                       os.path.join(self.get_bin_dir(), 'scylla-jmx'), replace=replace)
                select_java = Path(self.get_jmx_dir()) / 'select-java'
            else:
                self.hard_link_or_copy(os.path.join(self.get_jmx_dir(), 'target', 'scylla-jmx-1.0.jar'),
                                       os.path.join(self.get_bin_dir(), 'scylla-jmx-1.0.jar'), replace=replace)
                self.hard_link_or_copy(os.path.join(self.get_jmx_dir(), 'scripts', 'scylla-jmx'),
                                       os.path.join(self.get_bin_dir(), 'scylla-jmx'), replace=replace)
                select_java = Path(self.get_jmx_dir()) / 'scripts' / 'select-java'

            os.makedirs(os.path.join(self.get_bin_dir(), 'symlinks'), exist_ok=exist_ok)
            scylla_jmx_file = os.path.join(self.get_bin_dir(), 'symlinks', 'scylla-jmx')
            if os.path.exists(scylla_jmx_file) and replace:
                os.remove(scylla_jmx_file)
            if java_home := os.environ.get('JAVA_HOME'):
                # user selecting specific Java
                java_exe = Path(java_home) / 'bin' / 'java'
                os.symlink(java_exe, scylla_jmx_file)
            elif select_java.exists():
                # JMX lookup logic
                os.symlink(select_java, scylla_jmx_file)
            else:
                # older scylla versions, just use default java
                java_exe = Path('/usr') / 'bin' / 'java'
                os.symlink(java_exe, scylla_jmx_file)

        parent_dir = os.path.dirname(os.path.realpath(__file__))
        resources_bin_dir = os.path.join(parent_dir, 'resources', BIN_DIR)
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
            data = YAML().load(f)

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
                ','.join(self.cluster.get_seeds(node=self)))
        data['listen_address'], data['storage_port'] = (
            self.network_interfaces['storage'])
        assert self.network_interfaces['binary'] is not None
        data['rpc_address'], data['native_transport_port'] = self.network_interfaces['binary']

        # Use "workdir,W" instead of "workdir", because scylla defines this option this way
        # and dtests compares names of used options with the names defined in scylla.
        data['workdir,W'] = self.get_path()
        # This option is set separately from the workdir to keep backward compatibility with scylla java tools
        # such as sstablelevelreset which needs this option to get the path to the data directory.
        data['data_file_directories'] = [os.path.join(self.get_path(), 'data')]
        # The default path of commitlog subdirectory is workdir/commitlog.
        # This option is set separately from the workdir to keep backward compatibility,
        # because some dtests use `commitlogs` value to get the commitlog directory.
        data['commitlog_directory'] = os.path.join(self.get_path(),
                                                   'commitlogs')

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
            YAML().dump(data, f)

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
            YAML().dump(cassandra_data, f)

    def __update_yaml_dse(self):
        raise NotImplementedError('ScyllaNode.__update_yaml_dse')

    def _update_log4j(self):
        raise NotImplementedError('ScyllaNode._update_log4j')

    def __generate_server_xml(self):
        raise NotImplementedError('ScyllaNode.__generate_server_xml')

    def _get_directories(self):
        dirs = {}
        for i in ['data', 'commitlogs', BIN_DIR, 'conf', 'logs', 'hints', 'view_hints']:
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

    def flush(self, ks=None, table=None, **kwargs):
        super().flush(ks, table, **kwargs)

    def _run_scylla_executable_with_option(self, option, scylla_exec_path=None):
        if not scylla_exec_path:
            scylla_exec_path = self.get_path()

        if not scylla_exec_path.endswith(BIN_DIR):
            scylla_exec_path = os.path.join(scylla_exec_path, BIN_DIR)

        scylla_exec = os.path.join(scylla_exec_path, 'scylla')

        run_output = subprocess.run(f"{scylla_exec} {option}", shell=True, capture_output=True, text=True)
        if run_output.returncode:
            raise NodeError(f"Failed to run Scylla executable with option '{option}'. Error:\n{run_output.stderr}")

        return run_output.stdout.strip()

    def get_node_scylla_version(self, scylla_exec_path=None):
        return self._run_scylla_executable_with_option(option="--version", scylla_exec_path=scylla_exec_path)

    def upgrade(self, upgrade_to_version):
        self.upgrader.upgrade(upgrade_version=upgrade_to_version)

    def rollback(self, upgrade_to_version):
        self.upgrader.upgrade(upgrade_version=upgrade_to_version, recover_system_tables=True)

    def hostid(self, timeout=60, force_refresh=False):
        if self.node_hostid and not force_refresh:
            return self.node_hostid
        m = self.grep_log("init - Setting local host id to (.+?)$")
        if m:
            self.node_hostid = m[-1][1].group(1)
            return self.node_hostid
        try:
            node_address = self.address()
            url = f"http://{node_address}:{self.api_port}/storage_service/hostid/local"
            response = requests.get(url=url, timeout=timeout)
            if response.status_code == requests.codes.ok:
                self.node_hostid = response.json()
                return self.node_hostid
        except Exception as e:
            self.error(f"Failed to get hostid using {url}: {e}")
        return None

    def watch_rest_for_alive(self, nodes: Union['ScyllaNode', List['ScyllaNode']], timeout=120, wait_normal_token_owner=True):
        """
        Use the REST API to wait until this node detects that the nodes listed
        in "nodes" become fully operational.
        This is similar to watch_log_for_alive but uses ScyllaDB's REST API
        instead of the log file and waits for the node to be really useable,
        not just "UP" (see issue #461)

        Params:
          - wait_normal_token_owner: return only when this node sees all other nodes as normal token owner (True by default).
        """
        logging.getLogger('urllib3.connectionpool').disabled = True
        try:
            nodes_tofind = nodes if isinstance(nodes, list) else [nodes]
            tofind = set([node.address() for node in nodes_tofind])
            tofind_host_id_map = dict([(node.address(), node.hostid()) for node in nodes_tofind])
            found = set()
            found_host_id_map = dict()
            url_live = f"http://{self.address()}:{self.api_port}/gossiper/endpoint/live"
            url_joining = f"http://{self.address()}:{self.api_port}/storage_service/nodes/joining"
            url_tokens = f"http://{self.address()}:{self.api_port}/storage_service/tokens/"
            url_host_ids = f"http://{self.address()}:{self.api_port}/storage_service/host_id"
            endtime = time.time() + timeout
            while time.time() < endtime:
                live = set()
                response = requests.get(url=url_live)
                if response.status_code == requests.codes.ok:
                    live = set(response.json())
                response = requests.get(url=url_joining)
                if response.status_code == requests.codes.ok:
                    live = live - set(response.json())
                # Verify that node knows not only about the existance of the
                # other node, but also its host_id as a normal token owner:
                if tofind.issubset(live):
                    # This node thinks that all given nodes are alive and not
                    # "joining", we're almost done, but still need to verify
                    # that the node knows the others' tokens.
                    check = tofind
                    have_no_tokens = set()
                    for n in check:
                        response = requests.get(url=url_tokens+n)
                        if response.text == '[]':
                            have_no_tokens.add(n)
                    if not have_no_tokens:
                        if not wait_normal_token_owner:
                            return
                        # and that the node knows that the others' are normal token owners.
                        host_id_map = dict()
                        response = requests.get(url=url_host_ids)
                        if response.status_code == requests.codes.ok:
                            for r in response.json():
                                host_id_map[r['key']] = r['value']
                        # Verify that the other nodes are considered normal token owners on this node
                        # and their host_id matches the host_id the client knows about
                        normal = set([addr for addr, id in host_id_map.items() \
                                    if addr in tofind_host_id_map and \
                                    (id == tofind_host_id_map[addr] or not tofind_host_id_map[addr])])
                        tofind = tofind.difference(normal)
                        if not tofind:
                            return
                        # Update cumulative maps for debugging
                        found = found.union(normal)
                        found_host_id_map.update(host_id_map)
                time.sleep(0.1)
            self.debug(f"watch_rest_for_alive: tofind={tofind} found={found}: tofind_host_id_map={tofind_host_id_map} found_host_id_map={found_host_id_map}")
            raise TimeoutError(f"watch_rest_for_alive() timeout after {timeout} seconds")
        finally:
            logging.getLogger('urllib3.connectionpool').disabled = False

    @property
    def gnutls_config_file(self):
        candidates = [
            Path(self.node_install_dir) / 'scylla-core-package' / 'libreloc' / 'gnutls.config',
            Path(self.node_install_dir) / 'libreloc' / 'gnutls.config',
        ]
        for candidate in candidates:
            if candidate.exists():
                return str(candidate)
        raise ValueError(f"gnutls.config wasn't found in any path: {candidates}")

    def run_scylla_sstable(self, command, additional_args=None, keyspace=None, datafiles=None, column_families=None, batch=False, text=True, env=None):
        """Invoke scylla-sstable, with the specified command (operation) and additional_args.

        For more information about scylla-sstable, see https://docs.scylladb.com/stable/operating-scylla/admin-tools/scylla-sstable.html.

        Params:
        * command - The scylla-sstable command (operation) to run.
        * additional_args - Additional command-line arguments to pass to scylla-sstable, this should be a list of strings.
        * keyspace - Restrict the operation to sstables of this keyspace.
        * datafiles - Restrict the operation to the specified sstables (Data components).
        * column_families - Restrict the operation to sstables of these column_families. Must contain exactly one column family when datafiles is used.
        * batch - If True, all sstables will be passed in a single batch. If False, sstables will be passed one at a time.
            Batch-mode can be only used if column_families contains a single item.
        * text - If True, output of the command is treated as text, if not as bytes

        If datafiles is provided, the caller is responsible for making sure these
        files are not removed by ScyllaDB while the tool is running.
        If datafiles = None, this function will create a snapshot and dump
        sstables from the snapshot to ensure that the sstables are not removed
        while the tools is running.
        The snapshot is removed after the dump completed, but it is left there in
        case of error, for post-mortem analysis.

        Returns: map: {sstable: (stdout, stderr)} of all invokations. When batch == True, a single entry will be present, with empty key.

        Raises: subprocess.CalledProcessError if scylla-sstable returns a non-zero exit code.
        """
        if additional_args is None:
            additional_args = []

        scylla_path = common.join_bin(self.get_path(), BIN_DIR, 'scylla')
        ret = {}

        if datafiles is None and keyspace is not None and self.is_running():
            tag = "sstable-dump-{}".format(uuid.uuid1())
            kts = ",".join(f"{keyspace}.{column_family}" for column_family in column_families)
            self.debug(f"run_scylla_sstable(): creating snapshot with tag {tag} to be used for sstable dumping")
            self.nodetool(f"snapshot -t {tag} {kts}")
            sstables = []
            for column_family in column_families:
                sstables.extend(glob.glob(os.path.join(self.get_path(), 'data', keyspace, f"{column_family}-*/snapshots/{tag}/*-Data.db")))
        else:
            sstables = self._Node__gather_sstables(datafiles, keyspace, column_families)
            tag = None

        self.debug(f"run_scylla_sstable(): preparing to dump sstables {sstables}")

        def do_invoke(sstables):
            # there are chances that the table is not replicated on this node,
            # in that case, scylla tool will fail to dump the sstables and error
            # out. let's just return an empty list for the partitions, so that
            # dump_sstables() can still parse it in the same way.
            if not sstables:
                empty_dump = {'sstables': {'anonymous': []}}
                stdout, stderr = json.dumps(empty_dump), ''
                if text:
                    return stdout, stderr
                else:
                    return stdout.encode('utf-8'), stderr.encode('utf-8')
            common_args = [scylla_path, "sstable", command] + additional_args
            _env = self._get_environ()
            _env.update(env or {})
            res = subprocess.run(common_args + sstables, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=text, check=False, env=_env)
            if res.returncode:
                raise ToolError(command=' '.join(common_args + sstables), exit_status=res.returncode, stdout=res.stdout, stderr=res.stderr)
            return (res.stdout, res.stderr)

        if batch:
            if column_families is None or len(column_families) > 1:
                raise NodeError("run_scylla_sstable(): batch mode can only be used in conjunction with a single column_family")
            ret[""] = do_invoke(sstables)
        else:
            for sst in sstables:
                ret[sst] = do_invoke([sst])

        # Deliberately not putting this in a `finally` block, if the command
        # above failed, leave the snapshot with the sstables around for
        # post-mortem analysis.
        if tag is not None:
            self.nodetool(f"clearsnapshot -t {tag} {keyspace}")

        return ret

    def dump_sstables(self,
                      keyspace: str,
                      column_family: str,
                      datafiles: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """dump the partitions in the specified table using `scylla sstable dump-data`

        :param keyspace: restrict the operation to sstables of this keyspace
        :param column_family: restrict the operation to sstables of this column_family
        :param datafiles: restrict the operation to the given sstables
        :return: return all the partitions collected in the specified sstables
        :raises: subprocess.CalledProcessError if scylla-sstable returns a non-zero exit code.

        the $SSTABLE is returned from this method, please see
        https://opensource.docs.scylladb.com/stable/operating-scylla/admin-tools/scylla-sstable.html#dump-data
        for the JSON schema of it.

        a typical return value might look like:
        ```
        [
          {
            'key': {'token': '-4069959284402364209',
                    'raw': '000400000001',
                    'value': '1'},
            'tombstone': {'timestamp': 1690533264324595,
                          'deletion_time': '2023-07-28 08:34:24z'}
          },
          {
            'key': {'token': '-2249632751995682149',
                    'raw': '00040000005e',
                    'value': '94'},
            'clustering_elements': [
              {
                'type': 'clustering-row',
                'key': {...},
                'marker': {...},
                'columns': {
                  'c1': {
                    'is_live': True,
                    'type': 'regular',
                    'timestamp': 1691723027979972,
                    'ttl': '1234s',
                    'expiry': '2023-08-11 03:24:21z',
                    'value': 'new',
                  },
                  'c2': {...}
                }
              }
            ]
          }
        ]
        ```
        """
        sstable_dumps = self.run_scylla_sstable('dump-data', ['--merge'],
                                                keyspace=keyspace,
                                                column_families=[column_family],
                                                datafiles=datafiles,
                                                batch=True)
        assert '' in sstable_dumps
        stdout, _ = sstable_dumps['']
        return json.loads(stdout)['sstables']['anonymous']

    def dump_sstable_stats(self,
                           keyspace: str,
                           column_family: str,
                           datafiles: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """dump the partitions in the specified table using `scylla sstable dump-statistics`

        :param keyspace: restrict the operation to sstables of this keyspace
        :param column_family: restrict the operation to sstables of this column_family
        :param datafiles: restrict the operation to the given sstables
        :return: return all the statistics collected in the specified sstables
        :raises: subprocess.CalledProcessError if scylla-sstable returns a non-zero exit code.

        the $SSTABLE is returned from this method, please see
        https://opensource.docs.scylladb.com/stable/operating-scylla/admin-tools/scylla-sstable.html#dump-statistics
        for the JSON schema of it.

        a typical return value might look like:
        ```
        {
          "/home/joe/scylladb/testlog/release/scylla-6/data/system_schema/columns-24101c25a2ae3af787c1b40ee1aca33f/mc-2-big-Data.db": {
            "offsets": {
              "validation": 36,
              "compaction": 89,
              "stats": 117,
              "serialization": 4510
            },
            "validation": {
              "partitioner": "org.apache.cassandra.dht.Murmur3Partitioner",
              "filter_chance": 0.01
            },
            "compaction": {
              "cardinality": [
                255,
                255,
                ...
              ]
            },
            "stats": {
              "estimated_partition_size": [
                {
                  "offset": 1,
                  "value": 0
                },
                ...
              ],
              "estimated_cells_count": [
                {
                  "offset": 1,
                  "value": 0
                },
                ...
              ],
              "position": {
                "id": 200533039,
                "pos": 277953
              },
              "min_timestamp": 1691641905811667,
              "max_timestamp": 1691641905811668,
              "min_local_deletion_time": 1691641905,
              "max_local_deletion_time": 2147483647,
              "min_ttl": 0,
              "max_ttl": 0,
              "compression_ratio": 0.3668619791666667,
              "estimated_tombstone_drop_time": {
                "1691641905": 2
              },
              "sstable_level": 0,
              "repaired_at": 0,
              ...
              "columns_count": 1755,
              "rows_count": 351,
              ...
            },
            "serialization_header": {
              "min_timestamp_base": 248761905811667,
              ...
            }
          },
          ...
        }
        ```
        """
        sstable_stats = self.run_scylla_sstable('dump-statistics',
                                                keyspace=keyspace,
                                                column_families=[column_family],
                                                datafiles=datafiles,
                                                batch=True,
                                                text=False,
                                                )
        assert '' in sstable_stats
        stdout, _ = sstable_stats['']
        return json.loads(stdout.decode('utf-8', 'ignore'))['sstables']

    def dump_sstable_scylla_metadata(self,
                           keyspace: str,
                           column_family: str,
                           datafiles: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """dump scylla metadata using `scylla sstable dump-scylla-metadata`

        :param keyspace: restrict the operation to sstables of this keyspace
        :param column_family: restrict the operation to sstables of this column_family
        :param datafiles: restrict the operation to the given sstables
        :return: return all the statistics collected in the specified sstables
        :raises: subprocess.CalledProcessError if scylla-sstable returns a non-zero exit code.

        the $SSTABLE is returned from this method, please see
        https://opensource.docs.scylladb.com/stable/operating-scylla/admin-tools/scylla-sstable.html#dump-scylla-metadata
        for the JSON schema of it.
        """

        additional_args = []
        env = {'SCYLLA_CONF': str(Path(self.get_conf_dir()))}
        if keyspace:
            additional_args += ['--keyspace', keyspace]

        if column_family:
            additional_args += ['--table', column_family]

        sstable_stats = self.run_scylla_sstable('dump-scylla-metadata',
                                                additional_args=additional_args,
                                                keyspace=keyspace,
                                                column_families=[column_family],
                                                datafiles=datafiles,
                                                batch=True,
                                                text=False,
                                                env=env)
        assert '' in sstable_stats
        stdout, _ = sstable_stats['']
        return json.loads(stdout.decode('utf-8', 'ignore'))['sstables']


    def validate_sstable(self,
                         keyspace: str,
                         column_family: str,
                         datafiles: Optional[List[str]] = None):
        """validate sstables in given keyspace.column_family using `scylla sstable validate`

        :param keyspace: restrict the operation to sstables of this keyspace
        :param column_family: restrict the operation to sstables of this column_family
        :param datafiles: restrict the operation to the given sstables
        :return: return all the validation results performed of the specified sstables
        :raises: subprocess.CalledProcessError if scylla-sstable returns a non-zero exit code.

        the $ROOT is returned from this method, please see
        https://opensource.docs.scylladb.com/stable/operating-scylla/admin-tools/scylla-sstable#validate
        for the JSON schema of it
        """
        outputs = self.run_scylla_sstable('validate',
                                          keyspace=keyspace,
                                          column_families=[column_family],
                                          datafiles=datafiles,
                                          batch=True)
        stdout, _ = outputs['']
        return json.loads(stdout)['sstables']

    def validate_sstable_checksums(self,
                                   keyspace: str,
                                   column_family: str,
                                   datafiles: Optional[List[str]] = None):
        """validate sstables in given keyspace.column_family using `scylla sstable validate-checksums`

        :param keyspace: restrict the operation to sstables of this keyspace
        :param column_family: restrict the operation to sstables of this column_family
        :param datafiles: restrict the operation to the given sstables
        :return: return all the validation results performed of the specified sstables
        :raises: subprocess.CalledProcessError if scylla-sstable returns a non-zero exit code.

        the $ROOT is returned from this method, please see
        https://opensource.docs.scylladb.com/stable/operating-scylla/admin-tools/scylla-sstable#validate-checksums
        for the JSON schema of it
        """
        outputs = self.run_scylla_sstable('validate-checksums',
                                          keyspace=keyspace,
                                          column_families=[column_family],
                                          datafiles=datafiles,
                                          batch=True)
        stdout, _ = outputs['']
        return json.loads(stdout)['sstables']

    def wait_for_compactions(self,
                             keyspace=None,
                             column_family=None,
                             idle_timeout=None):
        if idle_timeout is None:
            idle_timeout = 300 if self.scylla_mode() != 'debug' else 900
        super().wait_for_compactions(keyspace=keyspace,
                                     column_family=column_family,
                                     idle_timeout=idle_timeout)

    def run_scylla_types(self, action: str, scylla_type: ScyllaType, *values: Tuple[Any], extra_args: Optional[List[Any]]=None):
        """Invoke scylla-types command, with the specified action and extra_args.

        For more information about scylla-types, see https://opensource.docs.scylladb.com/stable/operating-scylla/admin-tools/scylla-types.html

        Params:
        * action - The scylla-types operation to run.
        * scylla_type - The key type of the values being passed to the tool.
        * value - The values on which the action is to be applied.
        * extra_args - Additional command-line arguments to pass to scylla-types action.

        Returns: stdout

        Raises: ToolError if scylla-types returns a non-zero exit code.
        """
        if extra_args is None:
            extra_args = []
        extra_args.extend(scylla_type.as_types_args())

        scylla_path = common.join_bin(self.get_path(), BIN_DIR, 'scylla')
        cmd_and_args = [scylla_path, "types", action, *extra_args, "--", *values]
        cmd_and_args = [str(value) for value in cmd_and_args]
        res = subprocess.run(cmd_and_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False, env=self._get_environ())
        if res.returncode:
            raise ToolError(command=' '.join(cmd_and_args), exit_status=res.returncode, stdout=res.stdout, stderr=res.stderr)
        return res.stdout.strip()

    def _get_common_repair_options(self, keyspace: Optional[str] = None, tables: Optional[List[str]] = None, hosts: Optional[List[str]] = None, dcs: Optional[List[str]] = None):
        """
        Serializes repair options common for both vnode and tablet repair
        """
        options = []
        if keyspace:
            options.append(keyspace)
            if tables:
                options.append(" ".join(tables))

        if hosts:
            options.append("--in-hosts")
            options.append(",".join(hosts))

        if dcs:
            options.append("--in-dc")
            options.append(",".join(dcs))

        return options

    def vnode_repair(self, keyspace: Optional[str] = None, tables: Optional[List[str]] = None, hosts: Optional[List[str]] = None, dcs: Optional[List[str]] = None,
                   end_token: Optional[int] = None, local: bool = False, partitioner_range: bool = False, start_token: Optional[int] = None, timeout=None):
        """
        Serializes repair options and runs nodetool repair
        """
        options = self._get_common_repair_options(keyspace, tables, hosts, dcs)

        if end_token:
            options.append("--end-token")
            options.append(f"{end_token}")

        if local:
            options.append("--in-local-dc")

        if partitioner_range:
            options.append("--partitioner-range")

        if start_token:
            options.append("--start-token")
            options.append(f"{start_token}")

        return self.nodetool("repair " + " ".join(options), timeout=timeout)

    def tablet_repair(self, keyspace: Optional[str] = None, tables: Optional[List[str]] = None, hosts: Optional[List[str]] = None, dcs: Optional[List[str]] = None,
                           tablet_tokens: Optional[List[str]] = None, timeout=None):
        """
        Serializes repair options and runs nodetool cluster repair
        """
        options = self._get_common_repair_options(keyspace, tables, hosts, dcs)

        if tablet_tokens:
            options.append("--tablet-tokens")
            options.append(",".join(tablet_tokens))

        return self.nodetool("cluster repair " + " ".join(options), timeout=timeout)

    def repair(self, keyspace: Optional[str] = None, tables: Optional[List[str]] = None, hosts: Optional[List[str]] = None, dcs: Optional[List[str]] = None,
               end_token: Optional[int] = None, local: bool = False, partitioner_range: bool = False, start_token: Optional[int] = None,
               tablet_tokens: Optional[List[str]] = None, timeout: Optional[int] = None):
        """
        Runs both vnode (nodetool repair) and tablet (nodetool cluster repair) repair with specified params
        :param keyspace: the keyspace to repair
        :param tables: tables to repair in the given keyspace
        :param hosts: hosts participating in the repair
        :param dcs: dcs participating in repair
        :param end_token: token on which to end repair (for vnode repair only)
        :param local: if true repair is run only on local dc (for vnode repair only)
        :param partitioner_range: if true only the primary replicas are repaired (for vnode repair only)
        :param start_token: token on which to begin repair (for vnode repair only)
        :param tablet_tokens: tokens owned by the tablets to repair (for tablet repair only)
        :param timeout: timeout of nodetool operations
        """
        outs_and_errs = []
        if keyspace:
            url = f"http://{self.address()}:{self.api_port}/storage_service/keyspaces"
            resp = requests.get(url=url, params={"replication": "vnodes"})
            resp.raise_for_status()

            if keyspace in resp.json():
                res = self.vnode_repair(keyspace, tables, hosts, dcs, end_token, local, partitioner_range, start_token, timeout=timeout)
                outs_and_errs.append(res)
            else:
                res = self.tablet_repair(keyspace, tables, hosts, dcs, tablet_tokens, timeout=timeout)
                outs_and_errs.append(res)
        else:
            res = self.vnode_repair(keyspace, tables, hosts, dcs, end_token, local, partitioner_range, start_token, timeout=timeout)
            outs_and_errs.append(res)

            res = self.tablet_repair(keyspace, tables, hosts, dcs, tablet_tokens, timeout=timeout)
            outs_and_errs.append(res)

        outs, errs = zip(*outs_and_errs)
        return outs, errs


class NodeUpgrader:

    """
    Upgrade node is supported when uses relocatable packages only
    """

    def __init__(self, node: ScyllaNode):
        """
        :param node: node that should be upgraded/downgraded
        """
        self.node = node
        self._scylla_version_for_upgrade = None
        self.orig_install_dir = node.node_install_dir
        self.install_dir_for_upgrade = None

    @property
    def scylla_version_for_upgrade(self):
        return self._scylla_version_for_upgrade

    @scylla_version_for_upgrade.setter
    def scylla_version_for_upgrade(self, scylla_version_for_upgrade: str):
        """
        :param scylla_version_for_upgrade: relocatables name. Example: unstable/master:2020-11-18T08:57:53Z
        """
        self._scylla_version_for_upgrade = scylla_version_for_upgrade

    def _setup_relocatable_packages(self):
        try:
            cdir, _ = setup(self.scylla_version_for_upgrade)
        except Exception as exc:
            raise NodeUpgradeError(f"Failed to setup relocatable packages. {exc}")
        return cdir

    def _import_executables(self, install_dir):
        try:
            self.node.node_install_dir = install_dir
            self.node.import_bin_files(exist_ok=True, replace=True)
        except Exception as exc:
            self.node.node_install_dir = self.orig_install_dir
            raise NodeUpgradeError(f"Failed to import executables files. {exc}")

    def _recover_system_tables(self):
        """
        Part of the rollback procedure is to restore system tables.
        For this goal a snapshot will be taken before downgrade and restore the system tables after downgrade
        """
        def _get_snapshot_folder_name():
            """
            Some new table may not exist in old version. "peer" table exists in the all versions.
            Find snapshot folder name
            """
            system_peers = [p for p in node_system_ks_directory.iterdir() if p.name.startswith("peers-")]
            # Choose first created snapshot folder
            snapshot_folder = [snap for snap in sorted((system_peers[0] / 'snapshots').iterdir(), key=os.path.getmtime)]

            if not snapshot_folder:
                raise NodeError("Unable to recover '%s' sstables: snapshot is not found", system_folder)

            return snapshot_folder[0].name

        node_data_directory = Path(self.node.get_path()) / 'data'
        if not node_data_directory.exists():
            raise NodeError("Data directory %s is not found", node_data_directory.name)

        snapshot_folder_name = None
        for system_folder in ['system', 'system_schema']:
            node_system_ks_directory = node_data_directory / system_folder
            if not snapshot_folder_name:
                snapshot_folder_name = _get_snapshot_folder_name()

            for recover_table in node_system_ks_directory.iterdir():
                if not recover_table.is_dir():
                    continue

                recover_keyspace_snapshot = recover_table / 'snapshots' / snapshot_folder_name
                if not recover_keyspace_snapshot.exists():
                    continue

                # Remove all data file before copying snapshot
                for the_file in recover_table.iterdir():
                    if the_file.is_file():
                        the_file.unlink()

                copy_directory(recover_keyspace_snapshot, recover_table)

    def upgrade(self, upgrade_version: str, recover_system_tables: bool = False):
        """
        :param upgrade_version: relocatables folder. Example: unstable/master:2020-11-18T08:57:53Z
        :param recover_system_tables: restore system tables during rollback (
               https://github.com/scylladb/scylla-enterprise/issues/1950 )
        """
        # Part of the rollback procedure is to restore system tables.
        # For this goal a snapshot will be taken before downgrade and restore the system tables after downgrade
        self.scylla_version_for_upgrade = upgrade_version
        cdir = self._setup_relocatable_packages()

        self.node.nodetool("snapshot")

        self.node.stop(wait_other_notice=True)
        if self.node.status != Status.DOWN:
            raise NodeUpgradeError(f"Node {self.node.name} failed to stop before upgrade")

        self._import_executables(cdir)
        self.node.clean_runtime_file()

        if recover_system_tables:
            self._recover_system_tables()

        try:
            self.node.start(wait_other_notice=True, wait_for_binary_proto=True)
        except Exception as exc:
            raise NodeUpgradeError(f"Node {self.node.name} failed to start after upgrade. Error: {exc}")

        if self.node.status != Status.UP:
            self.node.node_install_dir = self.orig_install_dir
            raise NodeUpgradeError(f"Node {self.node.name} failed to start after upgrade")

        self.install_dir_for_upgrade = cdir
        self.node.node_scylla_version = self.install_dir_for_upgrade
        self.validate_version_after_upgrade()
        self.node.upgraded = True

    def validate_version_after_upgrade(self):
        expected_version = self.node.get_node_scylla_version(self.install_dir_for_upgrade)
        if self.node.node_scylla_version != expected_version:
            raise NodeUpgradeError("Node hasn't been upgraded. Expected version after upgrade: %s, Got: %s" % (
                                    expected_version, self.node.node_scylla_version))
