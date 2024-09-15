# ccm node


import errno
import glob
import itertools
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import time
import warnings
from datetime import datetime
import locale
from collections import namedtuple

from ruamel.yaml import YAML

from ccmlib import common
from ccmlib.cli_session import CliSession
from ccmlib.repository import setup
from ccmlib.utils.version import parse_version


class Status():
    UNINITIALIZED = "UNINITIALIZED"
    UP = "UP"
    DOWN = "DOWN"
    DECOMMISSIONED = "DECOMMISSIONED"


class NodeUpgradeError(Exception):

    def __init__(self, msg):
        Exception.__init__(self, msg)


class NodeError(Exception):

    def __init__(self, msg, process=None):
        Exception.__init__(self, msg)
        self.process = process

class TimeoutError(Exception):

    def __init__(self, data):
        Exception.__init__(self, str(data))


class ToolError(Exception):

    def __init__(self, command, exit_status, stdout=None, stderr=None):
        self.command = command
        self.exit_status = exit_status
        self.stdout = stdout
        self.stderr = stderr

        message = f"Subprocess {command} exited with non-zero status; exit status: {exit_status}"
        if stdout:
            message += "; \nstdout: "
            message += self.__decode(stdout)
        if stderr:
            message += "; \nstderr: "
            message += self.__decode(stderr)

        Exception.__init__(self, message)

    def __decode(self, value):
        if isinstance(value, bytes):
            return bytes.decode(value, locale.getpreferredencoding(False))
        return value


NodetoolError = ToolError

# Groups: 0 = ks, 1 = cf, 2 = tmp or none, 3 = version, 4 = identifier (generation), 4 = "big-" or none, 5 = suffix (Compacted or Data.db)
_sstable_regexp = re.compile(r'((?P<keyspace>[^\s-]+)-(?P<cf>[^\s-]+)-)?(?P<tmp>tmp(link)?-)?(?P<version>[^\s-]+)-(?P<identifier>[^-]+)-(?P<big>big-)?(?P<suffix>[a-zA-Z]+)\.[a-zA-Z0-9]+$')


class Node(object):

    """
    Provides interactions to a Cassandra node.
    """

    def __init__(self, name, cluster, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save=True, binary_interface=None):
        """
        Create a new Node.
          - name: the name for that node
          - cluster: the cluster this node is part of
          - auto_bootstrap: whether or not this node should be set for auto-bootstrap
          - thrift_interface: the (host, port) tuple for thrift
          - storage_interface: the (host, port) tuple for internal cluster communication
          - jmx_port: the port for JMX to bind to
          - remote_debug_port: the port for remote debugging
          - initial_token: the token for this node. If None, use Cassandra token auto-assignment
          - save: copy all data useful for this node to the right position.  Leaving this true
            is almost always the right choice.
        """
        self.name = name
        self.cluster = cluster
        self.status = Status.UNINITIALIZED
        self.auto_bootstrap = auto_bootstrap
        self.network_interfaces = {'storage': common.normalize_interface(storage_interface),
                                   'binary': common.normalize_interface(binary_interface)}
        self.jmx_port = jmx_port
        self.remote_debug_port = remote_debug_port
        self.initial_token = initial_token
        self.pid = None
        self.all_pids = []
        self.data_center = None
        self.rack = None
        self.workload = None
        self.__config_options = {}
        self.__install_dir = None
        self.__global_log_level = None
        self.__classes_log_level = {}
        self.__conf_updated = False
        if save:
            self.import_config_files()
            self.import_bin_files()
            if common.is_win():
                self.__clean_bat()
        else:
            self._create_directory()

    @staticmethod
    def load(path, name, cluster):
        """
        Load a node from from the path on disk to the config files, the node name and the
        cluster the node is part of.
        """
        node_path = os.path.join(path, name)
        filename = os.path.join(node_path, 'node.conf')
        with open(filename, 'r') as f:
            data = YAML().load(f)
        try:
            itf = data['interfaces']
            initial_token = None
            if 'initial_token' in data:
                initial_token = data['initial_token']
            remote_debug_port = 2000
            if 'remote_debug_port' in data:
                remote_debug_port = data['remote_debug_port']
            binary_interface = None
            if 'binary' in itf and itf['binary'] is not None:
                binary_interface = tuple(itf['binary'])
            node = cluster.create_node(data['name'], data['auto_bootstrap'], None, tuple(itf['storage']), data[
                                       'jmx_port'], remote_debug_port, initial_token, save=False, binary_interface=binary_interface)
            node.status = data['status']
            if 'pid' in data:
                node.pid = int(data['pid'])
            if 'docker_id' in data:
                node.pid = data['docker_id']
            if 'install_dir' in data:
                node.__install_dir = data['install_dir']
            if 'config_options' in data:
                node.__config_options = data['config_options']
            if 'data_center' in data:
                node.data_center = data['data_center']
            if 'rack' in data:
                node.rack = data['rack']
            if 'workload' in data:
                node.workload = data['workload']
            return node
        except KeyError as k:
            raise common.LoadError("Error Loading " + filename + ", missing property: " + str(k))

    def get_path(self):
        """
        Returns the path to this node top level directory (where config/data is stored)
        """
        return os.path.join(self.cluster.get_path(), self.name)

    def get_bin_dir(self):
        """
        Returns the path to the directory where Cassandra scripts are located
        """
        return os.path.join(self.get_path(), 'bin')

    def get_tool(self, toolname):
        return [common.join_bin(self.get_install_dir(), 'bin', toolname)]

    def get_env(self):
        update_conf = not self.__conf_updated
        if update_conf:
            self.__conf_updated = True
        return common.make_cassandra_env(self.get_install_dir(), self.get_path(), update_conf, hardcode_java_version='8')

    def get_install_cassandra_root(self):
        return self.get_install_dir()

    def is_scylla(self):
        return common.isScylla(self.get_install_dir())

    @staticmethod
    def is_docker():
        return False

    def get_node_cassandra_root(self):
        return self.get_path()

    def get_conf_dir(self):
        """
        Returns the path to the directory where Cassandra config are located
        """
        return os.path.join(self.get_path(), 'conf')

    def get_config_options(self):
        return self.__config_options

    def address(self):
        """
        Returns the IP use by this node for internal communication
        """
        return self.network_interfaces['storage'][0]

    def get_install_dir(self):
        """
        Returns the path to the cassandra source directory used by this node.
        """
        if not self.__install_dir:
            return self.cluster.get_install_dir()
        else:
            common.validate_install_dir(self.__install_dir)
            return self.__install_dir

    def set_install_dir(self, install_dir=None, version=None, verbose=False, upgrade=False):
        """
        Sets the path to the cassandra source directory for use by this node.
        """
        if version is None:
            self.__install_dir = install_dir
            if install_dir is not None:
                common.validate_install_dir(install_dir)
        else:
            dir, v = setup(version, verbose=verbose)
            self.__install_dir = dir
        self.import_config_files()
        if upgrade:
            self.import_bin_files(exist_ok=True, replace=True)
        else:
            self.import_bin_files()
        self.__conf_updated = False
        return self

    def set_workload(self, workload):
        raise common.ArgumentError("Cannot set workload on cassandra node")

    def get_cassandra_version(self):
        try:
            return common.get_version_from_build(self.get_install_dir())
        except common.CCMError:
            return self.cluster.cassandra_version()

    def get_base_cassandra_version(self):
        version = self.get_cassandra_version()
        return float(version[:version.index('.') + 2])

    def set_configuration_options(self, values=None, batch_commitlog=None):
        """
        Set Cassandra configuration options.
        ex:
            node.set_configuration_options(values={
                'hinted_handoff_enabled' : True,
                'concurrent_writes' : 64,
            })
        The batch_commitlog option gives an easier way to switch to batch
        commitlog (since it requires setting 2 options and unsetting one).
        """
        if values is not None:
            for k, v in values.items():
                self.__config_options[k] = v
        if batch_commitlog is not None:
            if batch_commitlog:
                self.__config_options["commitlog_sync"] = "batch"
                self.__config_options["commitlog_sync_batch_window_in_ms"] = 5
                self.__config_options["commitlog_sync_period_in_ms"] = None
            else:
                self.__config_options["commitlog_sync"] = "periodic"
                self.__config_options["commitlog_sync_period_in_ms"] = 10000
                self.__config_options["commitlog_sync_batch_window_in_ms"] = None

        self.import_config_files()

    def get_configuration_options(self):
        return self.__config_options

    def show(self, only_status=False, show_cluster=True):
        """
        Print infos on this node configuration.
        """
        self.__update_status()
        indent = ''.join([" " for i in range(0, len(self.name) + 2)])
        print(f"{self.name}: {self.__get_status_string()}")
        if not only_status:
            if show_cluster:
                print(f"{indent}{'cluster'}={self.cluster.name}")
            print(f"{indent}{'auto_bootstrap'}={self.auto_bootstrap}")
            if self.network_interfaces['binary'] is not None:
                print(f"{indent}{'binary'}={self.network_interfaces['binary']}")
            print(f"{indent}{'storage'}={self.network_interfaces['storage']}")
            print(f"{indent}{'jmx_port'}={self.jmx_port}")
            print(f"{indent}{'remote_debug_port'}={self.remote_debug_port}")
            print(f"{indent}{'initial_token'}={self.initial_token}")
            if self.pid:
                print(f"{indent}{'pid'}={self.pid}")

    def is_running(self):
        """
        Return true if the node is running
        """
        self.__update_status()
        return self.status == Status.UP or self.status == Status.DECOMMISSIONED

    def is_live(self):
        """
        Return true if the node is live (it's run and is not decommissioned).
        """
        self.__update_status()
        return self.status == Status.UP

    def logfilename(self):
        """
        Return the path to the current Cassandra log of this node.
        """
        return os.path.join(self.get_path(), 'logs', 'system.log')

    def debuglogfilename(self):
        return os.path.join(self.get_path(), 'logs', 'debug.log')

    def upgradesstables_if_command_available(self):
        stdout, _ = self.nodetool('help')
        return True if "upgradesstables" in stdout else False

    def get_node_supported_sstable_versions(self):
        match = self.grep_log(r'Feature (.*)_SSTABLE_FORMAT is enabled')
        return [m[1].group(1).lower() for m in match] if match else []

    def check_node_sstables_format(self, timeout=10):
        node_system_folder = os.path.join(self.get_path(), 'data', 'system')
        find_cmd = f"find {node_system_folder} -type f ! -path *snapshots* -printf %f\\n".split()
        try:
            result = subprocess.run(find_cmd, capture_output=True, timeout=timeout, text=True)
            assert not result.stderr, result.stderr
            assert result.stdout, f"Empty command '\"{find_cmd}\" output"
            all_sstable_files = result.stdout.splitlines()

            sstable_version_regex = re.compile(r'(\w+)-[^-]+-(.+)\.(db|txt|sha1|crc32)')

            sstable_versions = {sstable_version_regex.search(f).group(1) for f in all_sstable_files if
                                sstable_version_regex.search(f)}
            return sstable_versions
        except Exception:
            raise

    def grep_log(self, expr, filter_expr=None, filename='system.log', from_mark=None):
        """
        Returns a list of lines matching the regular expression in parameter
        in the Cassandra log of this node
        """
        matchings = []
        pattern = re.compile(expr)
        if filter_expr:
            filter_pattern = re.compile(filter_expr)
        else:
            filter_pattern = None
        with open(os.path.join(self.get_path(), 'logs', filename)) as f:
            if from_mark:
                f.seek(from_mark)
            for line in f:
                m = pattern.search(line)
                if m and not (filter_pattern and re.search(filter_pattern, line)):
                    matchings.append((line, m))
        return matchings

    def grep_log_for_errors(self, filename='system.log', distinct_errors=False, search_str=None, case_sensitive=True, from_mark=None):
        """
        Returns a list of errors with stack traces
        in the Cassandra log of this node
        """
        if search_str:
            search_str = search_str if case_sensitive else search_str.lower()

        log_file = os.path.join(self.get_path(), 'logs', filename)
        if not os.path.exists(log_file):
            return []

        if from_mark is None and hasattr(self, 'error_mark'):
            from_mark = self.error_mark

        with open(log_file) as f:
            if from_mark:
                f.seek(from_mark)
            return _grep_log_for_errors(f.read(), distinct_errors=distinct_errors, search_str=search_str, case_sensitive=case_sensitive)

    def mark_log_for_errors(self, filename='system.log'):
        """
        Ignore errors behind this point when calling
        node.grep_log_for_errors()
        """
        self.error_mark = self.mark_log(filename)

    def mark_log(self, filename='system.log'):
        """
        Returns "a mark" to the current position of this node Cassandra log.
        This is for use with the from_mark parameter of watch_log_for_* methods,
        allowing to watch the log from the position when this method was called.
        """
        log_file = os.path.join(self.get_path(), 'logs', filename)
        if not os.path.exists(log_file):
            return 0
        with open(log_file) as f:
            f.seek(0, os.SEEK_END)
            return f.tell()

    def print_process_output(self, name, proc, verbose=False):
        try:
            stderr = proc.communicate()[1]
        except ValueError:
            [stdout, stderr] = ['', '']
        if stderr is None:
            stderr = ''
        if len(stderr) > 1:
            print(f"[{name} ERROR] {stderr.strip()}")

    # This will return when exprs are found or it timeouts
    def watch_log_for(self, exprs, from_mark=None, timeout=600, process=None, verbose=False, filename='system.log', polling_interval=0.01):
        """
        Watch the log until all (regular) expression are found or the timeout is reached.
        On successful completion, a list of pairs (line matched, match object) is returned.
        If the timeout expired before matching all expressions in `exprs`,
        a TimeoutError is raised.
        The `polling_interval` determines how long we sleep when
        reaching the end of file before attempting to read the next line.
        """
        deadline = time.time() + timeout
        tofind = [exprs] if isinstance(exprs, str) else exprs
        tofind = [re.compile(e) for e in tofind]
        matchings = []
        reads = ""
        if len(tofind) == 0:
            return None

        log_file = os.path.join(self.get_path(), 'logs', filename)
        while not os.path.exists(log_file):
            time.sleep(.1)
            if process:
                process.poll()
                if process.returncode is not None:
                    self.print_process_output(self.name, process, verbose)
                    if process.returncode != 0:
                        raise RuntimeError()  # Shouldn't reuse RuntimeError but I'm lazy

        with open(log_file) as f:
            if from_mark:
                f.seek(from_mark)

            while True:
                # First, if we have a process to check, then check it.
                # Skip on Windows - stdout/stderr is cassandra.bat
                if not common.is_win():
                    if process:
                        process.poll()
                        if process.returncode is not None:
                            self.print_process_output(self.name, process, verbose)
                            if process.returncode != 0:
                                raise RuntimeError()  # Shouldn't reuse RuntimeError but I'm lazy

                line = f.readline()
                if line:
                    reads = reads + line
                    for e in tofind:
                        m = e.search(line)
                        if m:
                            matchings.append((line, m))

                            # This is bogus! - One should not modify the list from inside the loop which iterates on
                            # that list.
                            # However since we are going to break from the loop a few lines below that should be ok.
                            tofind.remove(e)

                            if len(tofind) == 0:
                                return matchings[0] if isinstance(exprs, str) else matchings
                            break
                else:
                    # yep, it's ugly
                    # FIXME: consider using inotify with IN_MODIFY to monitor the file
                    time.sleep(polling_interval)
                    if time.time() > deadline:
                        raise TimeoutError(time.strftime("%d %b %Y %H:%M:%S", time.gmtime()) + " [" + self.name + "] Missing: " + str(
                            [e.pattern for e in tofind]) + ":\n" + reads[:50] + f".....\nSee {filename} for remainder")

                if process:
                    if common.is_win():
                        if not self.is_running():
                            return None
                    else:
                        process.poll()
                        if process.returncode is not None:
                            if process.returncode == 0:
                                return None
                            else:
                                raise RuntimeError(f"The process is dead, returncode={process.returncode}")

    def watch_log_for_death(self, nodes, from_mark=None, timeout=600, filename='system.log'):
        """
        Watch the log of this node until it detects that the provided other
        nodes are marked dead. This method returns nothing but throw a
        TimeoutError if all the requested node have not been found to be
        marked dead before timeout sec.
        A mark as returned by mark_log() can be used as the from_mark
        parameter to start watching the log from a given position. Otherwise
        the log is watched from the beginning.
        """
        tofind = nodes if isinstance(nodes, list) else [nodes]
        tofind = [f"{node.address()}.* now (dead|DOWN)" for node in tofind]
        self.watch_log_for(tofind, from_mark=from_mark, timeout=timeout, filename=filename)

    def watch_log_for_alive(self, nodes, from_mark=None, timeout=120, filename='system.log'):
        """
        Watch the log of this node until it detects that the provided other
        nodes are marked UP. This method works similarly to watch_log_for_death.
        """
        tofind = nodes if isinstance(nodes, list) else [nodes]
        tofind = [f"{node.address()}.* now UP" for node in tofind]
        self.watch_log_for(tofind, from_mark=from_mark, timeout=timeout, filename=filename)

    def wait_for_binary_interface(self, **kwargs):
        """
        Waits for the Binary CQL interface to be listening.  If > 1.2 will check
        log for 'Starting listening for CQL clients' before checking for the
        interface to be listening.

        Emits a warning if not listening after 10 seconds.
        """
        if parse_version(self.cluster.version()) >= parse_version('1.2'):
            self.watch_log_for("Starting listening for CQL clients", **kwargs)

        binary_itf = self.network_interfaces['binary']
        if not common.check_socket_listening(binary_itf, timeout=10):
            warnings.warn("Binary interface %s:%s is not listening after 10 seconds, node may have failed to start."
                          % (binary_itf[0], binary_itf[1]))

    def start(self,
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
              use_jna=False,
              quiet_start=False):
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
        if jvm_args is None:
            jvm_args = []
        # Validate Windows env
        if common.is_win() and not common.is_ps_unrestricted() and self.cluster.version() >= '2.1':
            raise NodeError("PS Execution Policy must be unrestricted when running C* 2.1+")

        if not common.is_win() and quiet_start:
            print("WARN: Tried to set Windows quiet start behavior, but we're not running on Windows.")

        if self.is_running():
            raise NodeError(f"{self.name} is already running")

        for itf in list(self.network_interfaces.values()):
            if itf is not None and replace_address is None:
                common.check_socket_available(itf)

        if wait_other_notice:
            marks = [(node, node.mark_log()) for node in list(self.cluster.nodes.values()) if node.is_live()]

        self.mark = self.mark_log()

        cdir = self.get_install_dir()
        launch_bin = common.join_bin(cdir, 'bin', 'cassandra')
        # Copy back the cassandra scripts since profiling may have modified it the previous time
        shutil.copy(launch_bin, self.get_bin_dir())
        launch_bin = common.join_bin(self.get_path(), 'bin', 'cassandra')

        # If Windows, change entries in .bat file to split conf from binaries
        if common.is_win():
            self.__clean_bat()

        if profile_options is not None:
            config = common.get_config()
            if 'yourkit_agent' not in config:
                raise NodeError("Cannot enable profile. You need to set 'yourkit_agent' to the path of your agent in a ~/.ccm/config")
            cmd = f"-agentpath:{config['yourkit_agent']}"
            if 'options' in profile_options:
                cmd = cmd + '=' + profile_options['options']
            print(cmd)
            # Yes, it's fragile as shit
            pattern = r'cassandra_parms="-Dlog4j.configuration=log4j-server.properties -Dlog4j.defaultInitOverride=true'
            common.replace_in_file(launch_bin, pattern, '    ' + pattern + ' ' + cmd + '"')

        os.chmod(launch_bin, os.stat(launch_bin).st_mode | stat.S_IEXEC)

        env = self.get_env()

        if common.is_win():
            self._clean_win_jmx()

        pidfile = os.path.join(self.get_path(), 'cassandra.pid')
        args = [launch_bin, '-p', pidfile, f'-Dcassandra.join_ring={str(join_ring)}']
        args.append(f"-Dcassandra.logdir={os.path.join(self.get_path(), 'logs')}")
        if replace_token is not None:
            args.append(f'-Dcassandra.replace_token={str(replace_token)}')
        if replace_address is not None:
            args.append(f'-Dcassandra.replace_address={str(replace_address)}')
        if use_jna is False:
            args.append('-Dcassandra.boot_without_jna=true')
        env['JVM_EXTRA_OPTS'] = env.get('JVM_EXTRA_OPTS', "") + " " + " ".join(jvm_args)

        # In case we are restarting a node
        # we risk reading the old cassandra.pid file
        self._delete_old_pid()

        process = None
        FNULL = open(os.devnull, 'w')
        stdout_sink = subprocess.PIPE if verbose else FNULL
        if common.is_win():
            # clean up any old dirty_pid files from prior runs
            if (os.path.isfile(self.get_path() + "/dirty_pid.tmp")):
                os.remove(self.get_path() + "/dirty_pid.tmp")

            if quiet_start and self.cluster.version() >= '2.2.4':
                args.append('-q')

            process = subprocess.Popen(args, cwd=self.get_bin_dir(), env=env, stdout=stdout_sink, stderr=subprocess.PIPE, universal_newlines=True)
        else:
            process = subprocess.Popen(args, env=env, stdout=stdout_sink, stderr=subprocess.PIPE, universal_newlines=True)
        # Our modified batch file writes a dirty output with more than just the pid - clean it to get in parity
        # with *nix operation here.

        if verbose:
            stdout, stderr = process.communicate()
            print(stdout)
            print(stderr)

        if common.is_win():
            self.__clean_win_pid()
            self._update_pid(process)
            print(f"Started: {self.name} with pid: {self.pid}", file=sys.stderr, flush=True)
        elif update_pid:
            self._update_pid(process)

            if not self.is_running():
                raise NodeError(f"Error starting node {self.name}", process)

        if wait_other_notice:
            for node, mark in marks:
                node.watch_log_for_alive(self, from_mark=mark)

        if wait_for_binary_proto:
            self.wait_for_binary_interface(from_mark=self.mark)

        return process

    def stop(self, wait=True, wait_other_notice=False, other_nodes=None, gently=True, wait_seconds=127):
        """
        Stop the node.
          - wait: if True (the default), wait for the Cassandra process to be
            really dead. Otherwise return after having sent the kill signal.
            stop() will wait up to wait_seconds, by default 127 seconds, for
            the Cassandra process to die. After this wait, it will throw an
            exception stating it couldn't stop the node.
          - wait_other_notice: return only when the other live nodes of the
            cluster have marked this node has dead.
          - gently: Let Cassandra clean up and shut down properly. Otherwise do
            a 'kill -9' which shuts down faster.
        """
        if self.is_running():
            if wait_other_notice:
                if not other_nodes:
                    other_nodes = list(self.cluster.nodes.values())
                marks = [(node, node.mark_log()) for node in other_nodes if node.is_live() and node is not self]

            if common.is_win():
                # Just taskkill the instance, don't bother trying to shut it down gracefully.
                # Node recovery should prevent data loss from hard shutdown.
                # We have recurring issues with nodes not stopping / releasing files in the CI
                # environment so it makes more sense just to murder it hard since there's
                # really little downside.

                # We want the node to flush its data before shutdown as some tests rely on small writes being present.
                # The default Periodic sync at 10 ms may not have flushed data yet, causing tests to fail.
                # This is not a hard requirement, however, so we swallow any exceptions this may throw and kill anyway.
                if gently is True:
                    try:
                        self.flush()
                    except:
                        print(f"WARN: Failed to flush node: {self.name} on shutdown.")
                        pass

                os.system("taskkill /F /PID " + str(self.pid))
                if self._find_pid_on_windows():
                    print(f"WARN: Failed to terminate node: {self.name} with pid: {self.pid}")
            else:
                if gently:
                    os.kill(self.pid, signal.SIGTERM)
                else:
                    os.kill(self.pid, signal.SIGKILL)

            if wait_other_notice:
                for node, mark in marks:
                    node.watch_log_for_death(self, from_mark=mark)
            else:
                time.sleep(.1)

            still_running = self.is_running()
            if still_running and wait:
                # The sum of 7 sleeps starting at 1 and doubling each time
                # is 2**7-1 (=127). So to sleep an arbitrary wait_seconds
                # we need the first sleep to be wait_seconds/(2**7-1).
                wait_time_sec = wait_seconds/(2**7-1.0)
                for i in range(0, 7):
                    # we'll double the wait time each try and cassandra should
                    # not take more than 1 minute to shutdown
                    time.sleep(wait_time_sec)
                    if not self.is_running():
                        return True
                    wait_time_sec = wait_time_sec * 2
                raise NodeError(f"Problem stopping node {self.name}")
            else:
                return True
        else:
            return False

    @staticmethod
    def _parse_pending_tasks(output, keyspace, column_family):
        # "nodetool compactionstats" prints the compaction stats like:
        # pending tasks: 42
        # - ks1.cf1: 13
        # - ks1.cf2: 19
        # - ks2.cf1: 10
        head_pattern = r"pending tasks:\s*(?P<tasks>\d+)"
        tail_pattern = re.compile(r'\-\s+(\w+)\.(\w+):\s*(\d+)')
        head, *tail = output.strip().split('\n')

        if keyspace is None and column_family is None:
            matched = re.search(head_pattern, head)
            if not matched:
                raise RuntimeError(f"Cannot find 'pending tasks' in nodetool output.\nOutput: {output}")
            return int(matched.group('tasks'))

        # if keyspace or column_family is specified, check active tasks
        # of the specified ks.cf instead
        def matches(expected, actual):
            if expected is None:
                return True
            return expected == actual

        total = 0
        for line in tail:
            m = tail_pattern.search(line)
            if not m:
                break
            ks, cf, num = m.groups()
            if matches(keyspace, ks) and matches(column_family, cf):
                total += int(num)
        return total

    def wait_for_compactions(self,
                             keyspace=None,
                             column_family=None,
                             idle_timeout=300):
        """Wait for all compactions to finish on this node.

        :param keyspace: only wait for the compactions performed for specified
            keyspace. if not specified, all keyspaces are waited
        :param column_family: only wait for the compactions performed for
            specified column_family. if not specified, all keyspaces are waited
        :param idle_timeout: the time in seconds to wait for progress.
            Total time to wait is undeteremined, as long as we observe forward
            progress.
        """
        pending_tasks = -1
        last_change = None
        while not last_change or time.time() - last_change < idle_timeout:
            output, err = self.nodetool("compactionstats", capture_output=True)
            n = self._parse_pending_tasks(output, keyspace, column_family)
            # no active tasks, good!
            if n == 0:
                return
            if n != pending_tasks:
                last_change = time.time()
                if 0 < pending_tasks < n:
                    # background progress
                    self.warning(f"Pending compaction tasks increased from {pending_tasks} to {n} while waiting for compactions.")
                pending_tasks = n
            time.sleep(1)
        raise TimeoutError(f"Waiting for compactions timed out after {idle_timeout} seconds with pending tasks remaining: {output}.")

    def _do_run_nodetool(self, nodetool, capture_output=True, wait=True, timeout=None, verbose=True):
        if capture_output and not wait:
            raise common.ArgumentError("Cannot set capture_output while wait is False.")
        env = self.get_env()
        if verbose:
            self.debug(f"nodetool cmd={nodetool} wait={wait} timeout={timeout}")
        if capture_output:
            p = subprocess.Popen(nodetool, universal_newlines=True, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = p.communicate(timeout=timeout)
        else:
            p = subprocess.Popen(nodetool, env=env, universal_newlines=True)
            stdout, stderr = None, None

        if wait:
            exit_status = p.wait(timeout=timeout)
            if exit_status != 0:
                raise NodetoolError(" ".join(nodetool), exit_status, stdout, stderr)

        ignored_patterns = (re.compile("WARNING: debug mode. Not for benchmarking or production"),
                            re.compile("==[0-9]+==WARNING: ASan doesn't fully support makecontext/swapcontext functions and may produce false positives in some cases!"))

        def keep(line):
            self.debug(f"checking {line}")
            for ignored_pattern in ignored_patterns:
                if ignored_pattern.fullmatch(line):
                    return False
            return True

        def filter_debug_errors(stderr):
            if stderr is None:
                return stderr
            return '\n'.join([line for line in stderr.split('\n') if keep(line)])

        return stdout, filter_debug_errors(stderr)

    def nodetool(self, cmd, capture_output=True, wait=True, timeout=None, verbose=True):
        """
        Setting wait=False makes it impossible to detect errors,
        if capture_output is also False. wait=False allows us to return
        while nodetool is still running.
        When wait=True, timeout may be set to a number, in seconds,
        to limit how long the function will wait for nodetool to complete.
        """
        if self.is_scylla() and not self.is_docker():
            host = self.address()
        else:
            host = 'localhost'
        nodetool = self.get_tool('nodetool')

        if not isinstance(nodetool, list):
            nodetool = [nodetool]
        # see https://www.oracle.com/java/technologies/javase/8u331-relnotes.html#JDK-8278972
        args = ['-h', host, '-p', str(self.jmx_port), '-Dcom.sun.jndi.rmiURLParsing=legacy']
        if len(nodetool) > 1:
            nodetool.extend(cmd.split())
            # put args at the end of command line options, as "scylla nodetool"
            # expects them as options of the subcommand
            nodetool.extend(args)
        else:
            # while java-based tool considers them as options of the nodetool
            # itself
            nodetool.extend(args)
            nodetool.extend(cmd.split())

        return self._do_run_nodetool(nodetool, capture_output, wait, timeout, verbose)

    def dsetool(self, cmd):
        raise common.ArgumentError('Cassandra nodes do not support dsetool')

    def dse(self, dse_options=[]):
        raise common.ArgumentError('Cassandra nodes do not support dse')

    def hadoop(self, hadoop_options=[]):
        raise common.ArgumentError('Cassandra nodes do not support hadoop')

    def hive(self, hive_options=[]):
        raise common.ArgumentError('Cassandra nodes do not support hive')

    def pig(self, pig_options=[]):
        raise common.ArgumentError('Cassandra nodes do not support pig')

    def sqoop(self, sqoop_options=[]):
        raise common.ArgumentError('Cassandra nodes do not support sqoop')

    def bulkload(self, options):
        loader_bin = common.join_bin(self.get_path(), 'bin', 'sstableloader')
        env = self.get_env()
        host, port = self.network_interfaces['binary']
        args = ['-d', host, '-p', str(port)]
        os.execve(loader_bin, [common.platform_binary('sstableloader')] + args + options, env)

    def scrub(self, options):
        scrub_bin = self.get_tool('sstablescrub')
        env = self.get_env()
        os.execve(scrub_bin, [common.platform_binary('sstablescrub')] + options, env)

    def verify(self, options):
        verify_bin = self.get_tool('sstableverify')
        env = self.get_env()
        os.execve(verify_bin, [common.platform_binary('sstableverify')] + options, env)

    def run_cqlsh(self, cmds=None, show_output=False, cqlsh_options=None, return_output=False, timeout=600, extra_env=None):
        cqlsh_options = cqlsh_options or []
        cqlsh = self.get_tool('cqlsh')
        if not isinstance(cqlsh, list):
            cqlsh = [cqlsh]

        env = self.get_env()
        if extra_env:
            env.update(extra_env)

        assert self.get_base_cassandra_version() >= 2.1
        host, port = self.network_interfaces['binary']
        args = cqlsh_options + [host, str(port)] if '--cloudconf' not in cqlsh_options else cqlsh_options
        sys.stdout.flush()
        if cmds is None:
            if common.is_win():
                subprocess.Popen(cqlsh + args, env=env, creationflags=subprocess.CREATE_NEW_CONSOLE, universal_newlines=True)
            else:
                os.execve(cqlsh[0], cqlsh + args, env)
        else:
            p = subprocess.Popen(cqlsh + args, env=env, stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)
            for cmd in cmds.split(';'):
                cmd = cmd.strip()
                if cmd:
                    p.stdin.write(cmd + ';\n')
            p.stdin.write("quit;\n")

            try:
                output = p.communicate(timeout=timeout)
            except subprocess.TimeoutExpired as e:
                self.error(f"(EE) {e}")
                pat = re.compile(r'\r*\n')
                if e.stdout:
                    for err in pat.split(e.stdout.decode()):
                        self.error(f"(EE) <stdout> {err}")
                if e.stderr:
                    for err in pat.split(e.stderr.decode()):
                        self.error(f"(EE) <stdout> {err}")
                raise e

            for err in output[1].split('\n'):
                if err.strip():
                    common.print_if_standalone(f"(EE) {err}", debug_callback=self.debug, end='')

            if show_output:
                common.print_if_standalone(output[0], debug_callback=self.debug, end='')

            if return_output:
                return output

    def set_log_level(self, new_level, class_name=None):
        known_level = ['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'OFF']
        if new_level not in known_level:
            raise common.ArgumentError(f"Unknown log level {new_level} (use one of {' '.join(known_level)})")

        if class_name:
            self.__classes_log_level[class_name] = new_level
        else:
            self.__global_log_level = new_level
        # loggers changed > 2.1
        if self.get_base_cassandra_version() < 2.1:
            self._update_log4j()
        else:
            self.__update_logback()
        return self

    #
    # Update log4j config: copy new log4j-server.properties into
    # ~/.ccm/name-of-cluster/nodeX/conf/log4j-server.properties
    #
    def update_log4j(self, new_log4j_config):
        cassandra_conf_dir = os.path.join(self.get_conf_dir(),
                                          'log4j-server.properties')
        common.copy_file(new_log4j_config, cassandra_conf_dir)

    #
    # Update logback config: copy new logback.xml into
    # ~/.ccm/name-of-cluster/nodeX/conf/logback.xml
    #
    def update_logback(self, new_logback_config):
        cassandra_conf_dir = os.path.join(self.get_conf_dir(),
                                          'logback.xml')
        common.copy_file(new_logback_config, cassandra_conf_dir)

    def clear(self, clear_all=False, only_data=False, saved_caches=False):
        data_dirs = ['data']
        if not only_data:
            data_dirs.append("commitlogs")
            if clear_all:
                data_dirs.append('logs')
                if saved_caches:
                    data_dirs.append('saved_caches')
        for d in data_dirs:
            full_dir = os.path.join(self.get_path(), d)
            if only_data:
                for dir in os.listdir(full_dir):
                    keyspace_dir = os.path.join(full_dir, dir)
                    if os.path.isdir(keyspace_dir) and not dir.startswith("system"):
                        for table in os.listdir(keyspace_dir):
                            table_dir = os.path.join(keyspace_dir, table)
                            shutil.rmtree(table_dir)
                            os.mkdir(table_dir)
            else:
                for root, dirs, files in os.walk(full_dir):
                    for f in files:
                        os.unlink(os.path.join(root, f))
                    for d in dirs:
                        shutil.rmtree(os.path.join(root, d))

        # Needed for any subdirs stored underneath a data directory.
        # Common for hints post CASSANDRA-6230
        self._create_directory()

    def run_sstable2json(self, out_file=None, keyspace=None, datafiles=None, column_families=None, enumerate_keys=False):
        if out_file is None:
            out_file = subprocess.PIPE
        sstable2json = self._find_cmd('sstabledump')
        env = self.get_env()
        sstablefiles = self.__gather_sstables(datafiles, keyspace, column_families)
        common.print_if_standalone(str(sstablefiles), debug_callback=self.info, end='')
        for sstablefile in sstablefiles:
            common.print_if_standalone(f"-- {os.path.basename(sstablefile)} -----", debug_callback=self.info, end='')
            args = sstable2json + [sstablefile]
            if enumerate_keys:
                args = args + ["-e"]
            try:
                res = subprocess.run(args, env=env, stdout=out_file, stderr=subprocess.PIPE, text=True, check=True)
                common.print_if_standalone(res.stdout if res.stdout else '', debug_callback=self.info, end='')
            except subprocess.CalledProcessError as e:
                if e.stderr and 'Cannot find file' in e.stderr:
                    pass
                else:
                    raise ToolError(e.cmd, e.returncode, e.stdout, e.stderr)

    def run_json2sstable(self, in_file, ks, cf, keyspace=None, datafiles=None, column_families=None, enumerate_keys=False):
        json2sstable = self._find_cmd('json2sstable')
        env = self.get_env()
        sstablefiles = self.__gather_sstables(datafiles, keyspace, column_families)

        for sstablefile in sstablefiles:
            in_file_name = os.path.abspath(in_file.name)
            args = json2sstable + ["-s", "-K", ks, "-c", cf, in_file_name, sstablefile]
            subprocess.call(args, env=env)

    def run_sstablesplit(self, datafiles=None, size=None, keyspace=None, column_families=None,
                         no_snapshot=False, debug=False):
        sstablesplit = self._find_cmd('sstablesplit')
        env = self.get_env()
        sstablefiles = self.__gather_sstables(datafiles, keyspace, column_families)

        results = []

        def do_split(f):
            print(f"-- {os.path.basename(f)}-----")
            cmd = sstablesplit
            if size is not None:
                cmd += ['-s', str(size)]
            if no_snapshot:
                cmd.append('--no-snapshot')
            if debug:
                cmd.append('--debug')
            cmd.append(f)
            p = subprocess.Popen(cmd, cwd=os.path.join(self.get_path(), 'resources', 'cassandra', 'tools', 'bin'),
                                 env=env, stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)
            (out, err) = p.communicate()
            rc = p.returncode
            results.append((out, err, rc))

        for sstablefile in sstablefiles:
            do_split(sstablefile)

        return results

    def run_sstablemetadata(self, output_file=None, datafiles=None, keyspace=None, column_families=None):
        cdir = self.get_install_dir()
        sstablemetadata = self._find_cmd('sstablemetadata')
        env = self.get_env()
        sstablefiles = self.__gather_sstables(datafiles=datafiles, keyspace=keyspace, columnfamilies=column_families)
        results = []

        for sstable in sstablefiles:
            cmd = sstablemetadata + [sstable]
            if output_file is None:
                p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=env, universal_newlines=True)
                (out, err) = p.communicate()
                rc = p.returncode
                results.append((out, err, rc))
            else:
                subprocess.call(cmd, env=env, stdout=output_file)
        if output_file is None:
            return results

    def run_sstableexpiredblockers(self, output_file=None, keyspace=None, column_family=None):
        cdir = self.get_install_dir()
        sstableexpiredblockers = self._find_cmd('sstableexpiredblockers')
        env = self.get_env()
        cmd = sstableexpiredblockers + [keyspace, column_family]
        results = []
        if output_file is None:
            p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=env, universal_newlines=True)
            (out, err) = p.communicate()
            rc = p.returncode
            results.append((out, err, rc))
        else:
            subprocess.call(cmd, env=env, stdout=output_file)
        if output_file is None:
            return results

    def get_sstablespath(self, output_file=None, datafiles=None, keyspace=None, tables=None):
        sstablefiles = self.__gather_sstables(datafiles=datafiles, keyspace=keyspace, columnfamilies=tables)
        return sstablefiles

    def run_sstablerepairedset(self, set_repaired=True, datafiles=None, keyspace=None, column_families=None):
        cdir = self.get_install_dir()
        sstablerepairedset = self._find_cmd('sstablerepairedset')
        env = self.get_env()
        sstablefiles = self.__gather_sstables(datafiles, keyspace, column_families)

        for sstable in sstablefiles:
            if set_repaired:
                cmd = sstablerepairedset + ["--really-set", "--is-repaired", sstable]
            else:
                cmd = sstablerepairedset + ["--really-set", "--is-unrepaired", sstable]
            subprocess.call(cmd, env=env)

    def run_sstablelevelreset(self, keyspace, cf, output=False):
        cdir = self.get_install_dir()
        sstablelevelreset = self._find_cmd('sstablelevelreset')
        env = self.get_env()
        sstablefiles = self.__cleanup_sstables(keyspace, cf)

        cmd = sstablelevelreset + ["--really-reset", keyspace, cf]

        if output:
            p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=env, universal_newlines=True)
            (stdout, stderr) = p.communicate()
            rc = p.returncode
            return (stdout, stderr, rc)
        else:
            return subprocess.call(cmd, env=env)

    def run_sstableofflinerelevel(self, keyspace, cf, dry_run=False, output=False):
        cdir = self.get_install_dir()
        sstableofflinerelevel = self._find_cmd('sstableofflinerelevel')
        env = self.get_env()
        sstablefiles = self.__cleanup_sstables(keyspace, cf)

        if dry_run:
            cmd = sstableofflinerelevel + ["--dry-run", keyspace, cf]
        else:
            cmd = sstableofflinerelevel + [keyspace, cf]

        if output:
            p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=env, universal_newlines=True)
            (stdout, stderr) = p.communicate()
            rc = p.returncode
            return (stdout, stderr, rc)
        else:
            return subprocess.call(cmd, env=env, universal_newlines=True)

    def run_sstableverify(self, keyspace, cf, options=None, output=False):
        sstableverify = self.get_tool('sstableverify')
        env = self.get_env()
        sstablefiles = self.__cleanup_sstables(keyspace, cf)
        if not isinstance(sstableverify, list):
            sstableverify = [sstableverify]
        cmd = sstableverify + [keyspace, cf]
        if options is not None:
            cmd[1:1] = options

        if output:
            p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=env, universal_newlines=True)
            (stdout, stderr) = p.communicate()
            rc = p.returncode
            return (stdout, stderr, rc)
        else:
            return subprocess.call(cmd, env=env, universal_newlines=True)

    def _find_cmd(self, cmd):
        """
        Locates command under cassandra root and fixes permissions if needed
        """
        cdir = self.get_install_cassandra_root()
        if self.get_base_cassandra_version() >= 2.1:
            fcmd = common.join_bin(cdir, os.path.join('tools', 'bin'), cmd)
        else:
            fcmd = common.join_bin(cdir, 'bin', cmd)
        try:
            os.chmod(fcmd, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
        except:
            print(f"WARN: Couldn't change permissions to use {cmd}.")
            print("WARN: If it didn't work, you will have to do so manually.")
        return [fcmd]

    def list_keyspaces(self):
        keyspaces = os.listdir(os.path.join(self.get_path(), 'data'))
        keyspaces.remove('system')
        keyspaces.remove('system_schema')
        return keyspaces

    def unlink(self, file_path):
        """
        unlink a file/directory while changing to the needed permissions as needed
        :param file_path: file to unlink
        :return:
        """
        os.unlink(file_path)

    def chmod(self, file_path, permissions):
        """
        unlink a file/directory while changing to the needed permissions as needed
        :param file_path: file to unlink
        :param permissions: the new permissions
        :return:
        """
        os.chmod(file_path, permissions)

    def get_sstables(self, keyspace, column_family, ignore_unsealed=True, cleanup_unsealed=False):
        keyspace_dir = os.path.join(self.get_path(), 'data', keyspace)
        cf_glob = '*'
        if column_family:
            # account for changes in data dir layout from CASSANDRA-5202
            if self.get_base_cassandra_version() < 2.1:
                cf_glob = column_family
            else:
                cf_glob = column_family + '-*'
        if not os.path.exists(keyspace_dir):
            raise common.ArgumentError(f"Unknown keyspace {keyspace}")

        # data directory layout is changed from 1.1
        if self.get_base_cassandra_version() < 1.1:
            files = glob.glob(os.path.join(keyspace_dir, f"{column_family}*-Data.db"))
        elif self.get_base_cassandra_version() < 2.2:
            files = glob.glob(os.path.join(keyspace_dir, cf_glob, f"{keyspace}-{column_family}*-Data.db"))
        else:
            files = glob.glob(os.path.join(keyspace_dir, cf_glob, "*big-Data.db"))
        for f in files:
            if os.path.exists(f.replace('Data.db', 'Compacted')):
                files.remove(f)
        if ignore_unsealed or cleanup_unsealed:
            # sstablelevelreset tries to open all sstables under the configured
            # `data_file_directories` in cassandra.yaml. if any of them does not
            # have missing component, it complains in its stderr. and some tests
            # using this tool checks the stderr for unexpected error message, so
            # we need to remove all unsealed sstable files in this case.
            for toc_tmp in glob.glob(os.path.join(keyspace_dir, cf_glob, '*TOC.txt.tmp')):
                if cleanup_unsealed:
                    self.info(f"get_sstables: Cleaning up unsealed SSTable: {toc_tmp}")
                    for unsealed in glob.glob(toc_tmp.replace('TOC.txt.tmp', '*')):
                        os.remove(unsealed)
                else:
                    self.info(f"get_sstables: Ignoring unsealed SSTable: {toc_tmp}")
                data_sst = toc_tmp.replace('TOC.txt.tmp', 'Data.db')
                try:
                    files.remove(data_sst)
                except ValueError:
                    pass
        return files

    def stress_process(self, stress_options, **kwargs):
        if stress_options is None:
            stress_options = []
        else:
            stress_options = stress_options[:]

        if self.is_docker():
            stress = self.get_tool("cassandra-stress")
        else:
            stress = common.get_stress_bin(self.get_install_dir())
            stress = [stress]

        # handle cassandra-stress >= 3.11 parameters
        has_limit = any(['limit=' in o for o in stress_options])
        has_throttle = any(['throttle=' in o for o in stress_options])
        if has_limit or has_throttle:
            args = stress + ['version']
            _kwargs = {**kwargs, **dict(stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)}
            p = subprocess.Popen(args, **_kwargs)
            stdout, stderr = p.communicate()

            if p.returncode == 1 and has_throttle:
                # cassandra-stress doesn't version command
                stress_options = [o.replace('throttle=', 'limit=') for o in stress_options]
            elif p.returncode == 0 and has_limit:
                # cassandra-stress has version command
                stress_options = [o.replace('limit=', 'throttle=') for o in stress_options]

        options_implying_node = ['-d', '-node', '-cloudconf']
        if any(opt in options_implying_node for opt in stress_options):
            # no need to specify address
            pass
        else:
            if self.cluster.cassandra_version() < '2.1':
                stress_options.append('-d')
                stress_options.append(self.address())
            else:
                stress_options.append('-node')
                stress_options.append(self.address())
                # specify used jmx port if not already set
                if not [opt for opt in stress_options if opt.startswith('jmx=')]:
                    stress_options.extend(['-port', 'jmx=' + self.jmx_port])

        args = stress + stress_options
        stdout_handle = kwargs.pop("stdout", subprocess.PIPE)
        stderr_handle = kwargs.pop("stderr", subprocess.PIPE)
        p = subprocess.Popen(args,
                             stdout=stdout_handle, stderr=stderr_handle, universal_newlines=True,
                             **kwargs)
        return p

    def stress(self, stress_options=None, capture_output=False, **kwargs):
        if capture_output:
            self.warning("passing `capture_output` to stress_object() is deprecated")
        p = self.stress_process(stress_options=stress_options, **kwargs)
        try:
            return handle_external_tool_process(p, ['stress'] + stress_options)
        except KeyboardInterrupt:
            pass

    @staticmethod
    def _set_stress_val(key, val, res):
        def parse_num(s):
            return float(s.replace(',', ''))

        if "[" in val:
            p = re.compile(r'^\s*([\d\.\,]+\d?)\s*\[.*')
            m = p.match(val)
            if m:
                res[key] = parse_num(m.group(1))
            p = re.compile(r'^.*READ:\s*([\d\.\,]+\d?)[^\d].*')
            m = p.match(val)
            if m:
                res[key + ":read"] = parse_num(m.group(1))
            p = re.compile(r'.*WRITE:\s*([\d\.\,]+\d?)[^\d].*')
            m = p.match(val)
            if m:
                res[key + ":write"] = parse_num(m.group(1))
        else:
            try:
                res[key] = parse_num(val)
            except ValueError:
                res[key] = val

    def stress_object(self, stress_options=None, ignore_errors=None, **kwargs):
        if ignore_errors:
            self.warning("passing `ignore_errors` to stress_object() is deprecated")
        ret = self.stress(stress_options, **kwargs)
        p = re.compile(r'^\s*([^:]+)\s*:\s*(\S.*)\s*$')
        res = {}
        start = False
        for line in [s.strip() for s in ret.stdout.splitlines()]:
            if start:
                m = p.match(line)
                if m:
                    Node._set_stress_val(m.group(1).strip().lower(), m.group(2).strip(), res)
            else:
                if line == 'Results:':
                    start = True
        return res

    def shuffle(self, cmd):
        cdir = self.get_install_dir()
        shuffle = common.join_bin(cdir, 'bin', 'cassandra-shuffle')
        host = self.address()
        args = [shuffle, '-h', host, '-p', str(self.jmx_port)] + [cmd]
        try:
            subprocess.call(args)
        except KeyboardInterrupt:
            pass

    def data_size(self, live_data=None):
        """Uses `nodetool info` to get the size of a node's data in KB."""
        if live_data is not None:
            warnings.warn("The 'live_data' keyword argument is deprecated.",
                          DeprecationWarning)
        info = self.nodetool('info', capture_output=True)[0]
        return _get_load_from_info_output(info)

    def row_cache_entries(self):
        """Uses `nodetool info` to get the node row cache entry count."""
        info = self.nodetool('info', capture_output=True)[0]
        return _get_row_cache_entries_from_info_output(info)

    def flush(self, ks=None, table=None, **kwargs):
        cmd = "flush"
        if ks:
            cmd += f" {ks}"
        if table:
            cmd += f" {table}"
        self.nodetool(cmd, **kwargs)

    def compact(self, keyspace = "", tables = []):
        compact_cmd = ["compact"]
        if keyspace:
            compact_cmd.append(keyspace)
        compact_cmd += tables
        self.nodetool(" ".join(compact_cmd))

    def drain(self, block_on_log=False):
        mark = self.mark_log()
        self.nodetool("drain")
        if block_on_log:
            self.watch_log_for("DRAINED", from_mark=mark)

    def repair(self, options=[], **kwargs):
        args = ["repair"] + options
        cmd = ' '.join(args)
        return self.nodetool(cmd, **kwargs)

    def move(self, new_token):
        self.nodetool("move " + str(new_token))

    def cleanup(self):
        self.nodetool("cleanup")

    def version(self):
        self.nodetool("version")

    def decommission(self):
        self.nodetool("decommission")
        self.status = Status.DECOMMISSIONED
        self._update_config()

    def hostid(self, timeout=60, force_refresh=False):
        if not hasattr(self, 'node_hostid') or force_refresh:
            info = self.nodetool('info', capture_output=True, timeout=timeout)[0]
            id_lines = [s for s in info.split('\n')
                        if s.startswith('ID')]
            if not len(id_lines) == 1:
                msg = ('Expected output from `nodetool info` to contain exactly 1 '
                    'line starting with "ID". Found:\n') + info
                raise RuntimeError(msg)
            id_line = id_lines[0].replace(":", "").split()
            self.node_hostid = id_line[1]
        return self.node_hostid

    def get_datacenter_name(self):
        info = self.nodetool('info', capture_output=True)[0]
        id_lines = [s for s in info.split('\n')
                    if s.startswith('Data Center')]
        if not len(id_lines) == 1:
            msg = ('Expected output from `nodetool info` to contain exactly 1 '
                   'line starting with "ID". Found:\n') + info
            raise RuntimeError(msg)
        dc_name = id_lines[0].split(":")[1].strip()
        return dc_name

    def removeToken(self, token):
        self.nodetool("removeToken " + str(token))

    def removenode(self, hid):
        self.nodetool("removenode " + str(hid))

    def import_config_files(self):
        self._create_directory()
        self._update_config()
        self.copy_config_files()
        self.__update_yaml()
        # loggers changed > 2.1
        if self.get_base_cassandra_version() < 2.1:
            self._update_log4j()
        else:
            self.__update_logback()
        self.__update_envfile()

    def import_dse_config_files(self):
        raise common.ArgumentError('Cannot import DSE configuration files on a Cassandra node')

    def copy_config_files(self, clobber=False):
        conf_dir = os.path.join(self.get_install_dir(), 'conf')
        for name in os.listdir(conf_dir):
            filename = os.path.join(conf_dir, name)
            if os.path.isfile(filename) and \
                (clobber or not os.path.exists(os.path.join(self.get_conf_dir(), name))):
                    shutil.copy(filename, self.get_conf_dir())

    def import_bin_files(self, exist_ok=False, replace=False):
        bin_dir = os.path.join(self.get_install_dir(), 'bin')
        for name in os.listdir(bin_dir):
            filename = os.path.join(bin_dir, name)
            if os.path.isfile(filename):
                shutil.copy(filename, self.get_bin_dir())
                common.add_exec_permission(bin_dir, name)

    def __clean_bat(self):
        # While the Windows specific changes to the batch files to get them to run are
        # fairly extensive and thus pretty brittle, all the changes are very unique to
        # the needs of ccm and shouldn't be pushed into the main repo.

        # Change the nodes to separate jmx ports
        bin_dir = os.path.join(self.get_path(), 'bin')
        jmx_port_pattern = "-Dcom.sun.management.jmxremote.port="
        bat_file = os.path.join(bin_dir, "cassandra.bat")
        common.replace_in_file(bat_file, jmx_port_pattern, " " + jmx_port_pattern + self.jmx_port + "^")

        # Split binaries from conf
        home_pattern = "if NOT DEFINED CASSANDRA_HOME set CASSANDRA_HOME=%CD%"
        common.replace_in_file(bat_file, home_pattern, "set CASSANDRA_HOME=" + self.get_install_dir())

        classpath_pattern = "set CLASSPATH=\\\"%CASSANDRA_HOME%\\\\conf\\\""
        common.replace_in_file(bat_file, classpath_pattern, "set CCM_DIR=\"" + self.get_path() + "\"\nset CLASSPATH=\"%CCM_DIR%\\conf\"")

        # escape the double quotes in name of the lib files in the classpath
        jar_file_pattern = "do call :append \"%%i\""
        for_statement = r'for %%i in ("%CASSANDRA_HOME%\lib\*.jar")'
        common.replace_in_file(bat_file, jar_file_pattern, for_statement + " do call :append \\\"%%i\\\"")

        # escape double quotes in java agent path
        class_dir_pattern = "-javaagent:"
        common.replace_in_file(bat_file, class_dir_pattern, " -javaagent:\\\"%CASSANDRA_HOME%\\lib\\jamm-0.2.5.jar\\\"^")

        # escape the double quotes in name of the class directories
        class_dir_pattern = "set CASSANDRA_CLASSPATH="
        main_classes = "\\\"%CASSANDRA_HOME%\\build\\classes\\main\\\""
        common.replace_in_file(bat_file, class_dir_pattern, "set CASSANDRA_CLASSPATH=%CLASSPATH%;" +
                               main_classes)

        # background the server process and grab the pid
        run_text = "\\\"%JAVA_HOME%\\bin\\java\\\" %JAVA_OPTS% %CASSANDRA_PARAMS% -cp %CASSANDRA_CLASSPATH% \\\"%CASSANDRA_MAIN%\\\""
        run_pattern = ".*-cp.*"
        common.replace_in_file(bat_file, run_pattern, "wmic process call create \"" + run_text + "\" > \"" +
                               self.get_path() + "/dirty_pid.tmp\"\n")

        # On Windows, remove the VerifyPorts check from cassandra.ps1
        if self.cluster.version() >= '2.1':
            common.replace_in_file(os.path.join(self.get_path(), 'bin', 'cassandra.ps1'), '        VerifyPortsAreAvailable', '')

        # Specifically call the .ps1 file in our node's folder
        common.replace_in_file(bat_file, 'powershell /file .*', 'powershell /file "' + os.path.join(self.get_path(), 'bin', 'cassandra.ps1" %*'))

    def _save(self):
        self.__update_yaml()
        # loggers changed > 2.1
        if self.get_base_cassandra_version() < 2.1:
            self._update_log4j()
        else:
            self.__update_logback()
        self.__update_envfile()
        self._update_config()

    def _create_directory(self):
        dir_name = self.get_path()
        if not os.path.exists(dir_name):
            os.mkdir(dir_name)
            for _, dir in list(self._get_directories().items()):
                if not os.path.exists(dir):
                    os.mkdir(dir)

    def _update_config(self):
        dir_name = self.get_path()
        if not os.path.exists(dir_name):
            return
        filename = os.path.join(dir_name, 'node.conf')
        values = {
            'name': self.name,
            'status': self.status,
            'auto_bootstrap': self.auto_bootstrap,
            'interfaces': self.network_interfaces,
            'jmx_port': self.jmx_port,
            'config_options': self.__config_options,
        }
        if self.pid:
            values['pid'] = self.pid
        if self.initial_token:
            values['initial_token'] = self.initial_token
        if self.__install_dir is not None:
            values['install_dir'] = self.__install_dir
        if self.remote_debug_port:
            values['remote_debug_port'] = self.remote_debug_port
        if self.data_center:
            values['data_center'] = self.data_center
        if self.workload is not None:
            values['workload'] = self.workload
        with open(filename, 'w') as f:
            YAML().dump(values, f)

    def __update_yaml(self):
        conf_file = os.path.join(self.get_conf_dir(), common.CASSANDRA_CONF)
        with open(conf_file, 'r') as f:
            data = YAML().load(f)

        with open(conf_file, 'r') as f:
            yaml_text = f.read()

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
        data['rpc_address'], data['native_transport_port'] = self.network_interfaces['binary']
        if self.network_interfaces['binary'] is not None and self.get_base_cassandra_version() >= 1.2:
            _, data['native_transport_port'] = self.network_interfaces['binary']

        data['data_file_directories'] = [os.path.join(self.get_path(), 'data')]
        data['commitlog_directory'] = os.path.join(self.get_path(), 'commitlogs')
        data['saved_caches_directory'] = os.path.join(self.get_path(), 'saved_caches')
        if parse_version(self.get_cassandra_version()) > parse_version('3.0') and 'hints_directory' in yaml_text:
            data['hints_directory'] = os.path.join(self.get_path(), 'data', 'hints')

        if self.cluster.partitioner:
            data['partitioner'] = self.cluster.partitioner

        full_options = dict(list(self.cluster._config_options.items()) + list(self.__config_options.items()))  # last win and we want node options to win
        for name in full_options:
            value = full_options[name]
            if (isinstance(value, str) and len(value) == 0) or value is None:
                try:
                    del data[name]
                except KeyError:
                    # it is fine to remove a key not there
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
            YAML().dump(data, f)

    def _update_log4j(self):
        append_pattern = 'log4j.appender.R.File='
        conf_file = os.path.join(self.get_conf_dir(), common.LOG4J_CONF)
        log_file = os.path.join(self.get_path(), 'logs', 'system.log')
        # log4j isn't partial to Windows \.  I can't imagine why not.
        if common.is_win():
            log_file = re.sub("\\\\", "/", log_file)
        common.replace_in_file(conf_file, append_pattern, append_pattern + log_file)

        # Setting the right log level

        # Replace the global log level
        if self.__global_log_level is not None:
            append_pattern = 'log4j.rootLogger='
            common.replace_in_file(conf_file, append_pattern, append_pattern + self.__global_log_level + ',stdout,R')

        # Class specific log levels
        for class_name in self.__classes_log_level:
            logger_pattern = 'log4j.logger'
            full_logger_pattern = logger_pattern + '.' + class_name + '='
            common.replace_or_add_into_file_tail(conf_file, full_logger_pattern, full_logger_pattern + self.__classes_log_level[class_name])

    def __update_logback(self):
        conf_file = os.path.join(self.get_conf_dir(), common.LOGBACK_CONF)

        self.__update_logback_loglevel(conf_file)

        tools_conf_file = os.path.join(self.get_conf_dir(), common.LOGBACK_TOOLS_CONF)
        self.__update_logback_loglevel(tools_conf_file)

    def __update_logback_loglevel(self, conf_file):
        # Setting the right log level - 2.2.2 introduced new debug log
        if parse_version(self.get_cassandra_version()) >= parse_version('2.2.2') and self.__global_log_level:
            if self.__global_log_level in ['DEBUG', 'TRACE']:
                root_log_level = self.__global_log_level
                cassandra_log_level = self.__global_log_level
            elif self.__global_log_level == 'INFO':
                root_log_level = self.__global_log_level
                cassandra_log_level = 'DEBUG'
            elif self.__global_log_level in ['WARN', 'ERROR']:
                root_log_level = 'INFO'
                cassandra_log_level = 'DEBUG'
                system_log_filter_pattern = '<level>.*</level>'
                common.replace_in_file(conf_file, system_log_filter_pattern, '      <level>' + self.__global_log_level + '</level>')
            elif self.__global_log_level == 'OFF':
                root_log_level = self.__global_log_level
                cassandra_log_level = self.__global_log_level

            cassandra_append_pattern = '<logger name="org.apache.cassandra" level=".*"/>'
            common.replace_in_file(conf_file, cassandra_append_pattern, '  <logger name="org.apache.cassandra" level="' + cassandra_log_level + '"/>')
        else:
            root_log_level = self.__global_log_level

        # Replace the global log level and org.apache.cassandra log level
        if self.__global_log_level is not None:
            root_append_pattern = '<root level=".*">'
            common.replace_in_file(conf_file, root_append_pattern, '<root level="' + root_log_level + '">')

        # Class specific log levels
        for class_name in self.__classes_log_level:
            logger_pattern = '\t<logger name="'
            full_logger_pattern = logger_pattern + class_name + '" level=".*"/>'
            common.replace_or_add_into_file_tail(conf_file, full_logger_pattern, logger_pattern + class_name + '" level="' + self.__classes_log_level[class_name] + '"/>')

    def __update_envfile(self):
        # The cassandra-env.ps1 file has been introduced in 2.1
        if common.is_win() and self.get_base_cassandra_version() >= 2.1:
            conf_file = os.path.join(self.get_conf_dir(), common.CASSANDRA_WIN_ENV)
            jmx_port_pattern = r'^\s+\$JMX_PORT='
            jmx_port_setting = f'    $JMX_PORT="{self.jmx_port}"'
            remote_debug_options = f'    $env:JVM_OPTS="$env:JVM_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address={self.jmx_port}"'
        else:
            conf_file = os.path.join(self.get_conf_dir(), common.CASSANDRA_ENV)
            jmx_port_pattern = 'JMX_PORT='
            jmx_port_setting = f'JMX_PORT="{self.jmx_port}"'
            remote_debug_options = f'JVM_OPTS="$JVM_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address={self.jmx_port}"'

        common.replace_in_file(conf_file, jmx_port_pattern, jmx_port_setting)

        if self.remote_debug_port != '0':
            remote_debug_port_pattern = '((-Xrunjdwp:)|(-agentlib:jdwp=))transport=dt_socket,server=y,suspend=n,address='
            common.replace_in_file(conf_file, remote_debug_port_pattern, remote_debug_options)

        if parse_version(self.get_cassandra_version()) < parse_version('2.0.1'):
            common.replace_in_file(conf_file, "-Xss", '    JVM_OPTS="$JVM_OPTS -Xss228k"')

        for itf in list(self.network_interfaces.values()):
            if itf is not None and common.interface_is_ipv6(itf):
                if common.is_win():
                    common.replace_in_file(conf_file,
                                           '-Djava.net.preferIPv4Stack=true',
                                           '\t$env:JVM_OPTS="$env:JVM_OPTS -Djava.net.preferIPv4Stack=false -Djava.net.preferIPv6Addresses=true"')
                else:
                    common.replace_in_file(conf_file,
                                           '-Djava.net.preferIPv4Stack=true',
                                           'JVM_OPTS="$JVM_OPTS -Djava.net.preferIPv4Stack=false -Djava.net.preferIPv6Addresses=true"')
                break

    def __update_status(self):
        if self.pid is None:
            if self.status == Status.UP or self.status == Status.DECOMMISSIONED:
                self.status = Status.DOWN
            return

        old_status = self.status

        # os.kill on windows doesn't allow us to ping a process
        if common.is_win():
            self.__update_status_win()
        else:
            try:
                os.kill(self.pid, 0)
            except OSError as err:
                if err.errno == errno.ESRCH:
                    # not running
                    if self.status == Status.UP or self.status == Status.DECOMMISSIONED:
                        self.status = Status.DOWN
                elif err.errno == errno.EPERM:
                    # no permission to signal this process
                    if self.status == Status.UP or self.status == Status.DECOMMISSIONED:
                        self.status = Status.DOWN
                else:
                    # some other error
                    raise err
            else:
                if self.status == Status.DOWN or self.status == Status.UNINITIALIZED:
                    self.status = Status.UP

        if not old_status == self.status:
            if old_status == Status.UP and self.status == Status.DOWN:
                self.pid = None
            self._update_config()

    def __update_status_win(self):
        if self._find_pid_on_windows():
            if self.status == Status.DOWN or self.status == Status.UNINITIALIZED:
                self.status = Status.UP
        else:
            self.status = Status.DOWN

    def _find_pid_on_windows(self):
        found = False
        try:
            import psutil
            found = psutil.pid_exists(self.pid)
        except ImportError:
            print("WARN: psutil not installed. Pid tracking functionality will suffer. See README for details.")
            cmd = 'tasklist /fi "PID eq ' + str(self.pid) + '"'
            proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, universal_newlines=True)

            for line in proc.stdout:
                if re.match("Image", line):
                    found = True
        return found

    def _get_directories(self):
        dirs = {}
        for i in ['data', 'commitlogs', 'saved_caches', 'logs', 'conf', 'bin', os.path.join('data', 'hints')]:
            dirs[i] = os.path.join(self.get_path(), i)
        return dirs

    def __get_status_string(self):
        if self.status == Status.UNINITIALIZED:
            return f"{Status.DOWN} ({'Not initialized'})"
        else:
            return self.status

    def __clean_win_pid(self):
        start = common.now_ms()
        if self.get_base_cassandra_version() >= 2.1:
            # Spin for up to 15s waiting for .bat to write the pid file
            pidfile = self.get_path() + "/cassandra.pid"
            while (not os.path.isfile(pidfile)):
                now = common.now_ms()
                if (now - start > 15000):
                    raise Exception('Timed out waiting for pid file.')
                else:
                    time.sleep(.001)
            # Spin for up to 10s waiting for .bat to fill the pid file
            start = common.now_ms()
            while (os.stat(pidfile).st_size == 0):
                now = common.now_ms()
                if (now - start > 10000):
                    raise Exception('Timed out waiting for pid file to be filled.')
                else:
                    time.sleep(.001)
        else:
            try:
                # Spin for 500ms waiting for .bat to write the dirty_pid file
                while (not os.path.isfile(self.get_path() + "/dirty_pid.tmp")):
                    now = common.now_ms()
                    if (now - start > 500):
                        raise Exception('Timed out waiting for dirty_pid file.')
                    else:
                        time.sleep(.001)

                with open(self.get_path() + "/dirty_pid.tmp", 'r') as f:
                    found = False
                    process_regex = re.compile('ProcessId')

                    readStart = common.now_ms()
                    readEnd = common.now_ms()
                    while (found is False and readEnd - readStart < 500):
                        line = f.read()
                        if (line):
                            m = process_regex.search(line)
                            if (m):
                                found = True
                                linesub = line.split('=')
                                pidchunk = linesub[1].split(';')
                                win_pid = pidchunk[0].lstrip()
                                with open(self.get_path() + "/cassandra.pid", 'w') as pidfile:
                                    found = True
                                    pidfile.write(win_pid)
                        else:
                            time.sleep(.001)
                        readEnd = common.now_ms()
                    if not found:
                        raise Exception('Node: %s  Failed to find pid in ' +
                                        self.get_path() +
                                        '/dirty_pid.tmp. Manually kill it and check logs - ccm will be out of sync.')
            except Exception as e:
                print("ERROR: Problem starting " + self.name + " (" + str(e) + ")")
                raise Exception('Error while parsing <node>/dirty_pid.tmp in path: ' + self.get_path())

    def _delete_old_pid(self):
        pidfile = os.path.join(self.get_path(), 'cassandra.pid')
        if os.path.isfile(pidfile):
            os.remove(pidfile)

    def _update_pid(self, process):
        pidfile = os.path.join(self.get_path(), 'cassandra.pid')

        start = time.time()
        while not (os.path.isfile(pidfile) and os.stat(pidfile).st_size > 0):
            if (time.time() - start > 30.0):
                print("Timed out waiting for pidfile {} to be filled (current time is %s): File {} size={}".format(
                        pidfile,
                        datetime.now(),
                        'exists' if os.path.isfile(pidfile) else 'does not exist' if not os.path.exists(pidfile) else 'is not a file',
                        os.stat(pidfile).st_size if os.path.exists(pidfile) else -1))
                break
            else:
                time.sleep(0.1)

        try:
            with open(pidfile, 'r') as f:
                if common.is_win() and self.get_base_cassandra_version() >= 2.1:
                    self.pid = int(f.readline().strip().decode('utf-16'))
                else:
                    self.pid = int(f.readline().strip())
                self.all_pids = list(set(self.all_pids) | {self.pid})
        except IOError as e:
            raise NodeError(f'Problem starting node {self.name} due to {e}', process)
        self.__update_status()

    def __gather_sstables(self, datafiles=None, keyspace=None, columnfamilies=None):
        files = []
        if keyspace is None:
            for k in self.list_keyspaces():
                files = files + self.get_sstables(k, "")
        elif datafiles is None:
            if columnfamilies is None:
                files = files + self.get_sstables(keyspace, "")
            else:
                for cf in columnfamilies:
                    files = files + self.get_sstables(keyspace, cf)
        else:
            if not columnfamilies or len(columnfamilies) > 1:
                raise common.ArgumentError("Exactly one column family must be specified with datafiles")

            cf_dir = os.path.join(os.path.realpath(self.get_path()), 'data', keyspace, columnfamilies[0])

            sstables = set()
            for datafile in datafiles:
                if not os.path.isabs(datafile):
                    datafile = os.path.join(os.getcwd(), datafile)

                if not datafile.startswith(cf_dir + '-') and not datafile.startswith(cf_dir + os.sep):
                    raise NodeError("File doesn't appear to belong to the specified keyspace and column family: " + datafile)

                sstable = _sstable_regexp.match(os.path.basename(datafile))
                if not sstable:
                    raise NodeError("File doesn't seem to be a valid sstable filename: " + datafile)

                sstable = sstable.groupdict()
                if not sstable['tmp'] and sstable['identifier'] not in sstables:
                    if not os.path.exists(datafile):
                        raise IOError("File doesn't exist: " + datafile)
                    sstables.add(sstable['identifier'])
                    files.append(datafile)

        return files

    def __cleanup_sstables(self, keyspace, cf):
        system_keyspace_names = ['system_schema', 'system']
        if keyspace not in system_keyspace_names:
            for system_keyspace_name in system_keyspace_names:
                self.get_sstables(system_keyspace_name, '', cleanup_unsealed=True)
        try:
            return self.get_sstables(keyspace, cf, cleanup_unsealed=True)
        except common.ArgumentError:
            return []

    def _clean_win_jmx(self):
        if self.get_base_cassandra_version() >= 2.1:
            sh_file = os.path.join(common.CASSANDRA_CONF_DIR, common.CASSANDRA_WIN_ENV)
            dst = os.path.join(self.get_path(), sh_file)
            common.replace_in_file(dst, r"^\s+\$JMX_PORT=", "    $JMX_PORT=\"" + self.jmx_port + "\"")

            # properly use single and double quotes to count for single quotes in the CASSANDRA_CONF path
            common.replace_in_file(
                dst,
                'CASSANDRA_PARAMS=', '    $env:CASSANDRA_PARAMS=\'-Dcassandra' +                        # -Dcassandra
                ' -Dlogback.configurationFile=/"\' + "$env:CASSANDRA_CONF" + \'/logback.xml"\'' +       # -Dlogback.configurationFile=/"$env:CASSANDRA_CONF/logback.xml"
                ' + \' -Dcassandra.config=file:"\' + "///$env:CASSANDRA_CONF" + \'/cassandra.yaml"\'')  # -Dcassandra.config=file:"///$env:CASSANDRA_CONF/cassandra.yaml"

    def get_conf_option(self, option):
        conf_file = os.path.join(self.get_conf_dir(), common.CASSANDRA_CONF)
        with open(conf_file, 'r') as f:
            data = YAML().load(f)

        if option in data:
            return data[option]
        else:
            return None

    def pause(self):
        try:
            import psutil
            p = psutil.Process(self.pid)
            p.suspend()
        except ImportError:
            if common.is_win():
                print("WARN: psutil not installed. Pause functionality will not work properly on Windows.")
            else:
                os.kill(self.pid, signal.SIGSTOP)

    def resume(self):
        try:
            import psutil
            p = psutil.Process(self.pid)
            p.resume()
        except ImportError:
            if common.is_win():
                print("WARN: psutil not installed. Resume functionality will not work properly on Windows.")
            else:
                os.kill(self.pid, signal.SIGCONT)

    def kill(self, __signal):
        os.kill(self.pid, __signal)

    def rmtree(self, path):
        """
        delete a directory content without removing the directory
        since in docker those are mountpoint into the docker container, and deleting them leave break scylla
        """
        for root, dirs, files in os.walk(path):
            for f in files:
                os.unlink(os.path.join(root, f))
            for d in dirs:
                shutil.rmtree(os.path.join(root, d))

    def jstack(self, opts=None):
        opts = [] if opts is None else opts
        jstack_location = os.path.abspath(os.path.join(os.environ['JAVA_HOME'],
                                                       'bin',
                                                       'jstack'))
        jstack_cmd = [jstack_location, '-J-d64'] + opts + [str(self.pid)]
        return subprocess.Popen(jstack_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

    def _log_message(self, message):
        return f"{self.name}: {message}"

    def debug(self, message):
        self.cluster.debug(self._log_message(message))

    def info(self, message):
        self.cluster.info(self._log_message(message))

    def warning(self, message):
        self.cluster.warning(self._log_message(message))

    def error(self, message):
        self.cluster.error(self._log_message(message))

    def get_node_scylla_version(selff, scylla_exec_path=None): ...



def _get_load_from_info_output(info):
    load_lines = [s for s in info.split('\n')
                  if s.startswith('Load')]
    if not len(load_lines) == 1:
        msg = ('Expected output from `nodetool info` to contain exactly 1 '
               'line starting with "Load". Found:\n') + info
        raise RuntimeError(msg)
    load_line = load_lines[0].split()

    unit_multipliers = {'KB': 1,
                        'MB': 1024,
                        'GB': 1024 * 1024,
                        'TB': 1024 * 1024 * 1024}
    load_num, load_units = load_line[2], load_line[3]

    try:
        load_mult = unit_multipliers[load_units]
    except KeyError:
        expected = ', '.join(list(unit_multipliers))
        msg = ('Expected `nodetool info` to report load in one of the '
               'following units:\n'
               '    {expected}\n'
               'Found:\n'
               '    {found}').format(expected=expected, found=load_units)
        raise RuntimeError(msg)

    return float(load_num) * load_mult


def _get_row_cache_entries_from_info_output(info):
    row_cache_lines = [s for s in info.split('\n')
                       if s.startswith('Row Cache')]
    if not len(row_cache_lines) == 1:
        msg = ('Expected output from `nodetool info` to contain exactly 1 '
               'line starting with "Row Cache". Found:\n') + info
        raise RuntimeError(msg)
    row_cache_line = row_cache_lines[0].replace(",", "").split()
    return int(row_cache_line[4])


def _grep_log_for_errors(log, distinct_errors=False, search_str=None, case_sensitive=True):
    def make_pat_for_log_level(level):
        kwargs = {} if case_sensitive else {'flags': re.IGNORECASE}
        return re.compile(rf'\b{level}\b', **kwargs)

    error_pat = make_pat_for_log_level("ERROR")
    info_pat = make_pat_for_log_level("INFO")
    debug_pat = make_pat_for_log_level("DEBUG")

    matchings = []
    it = iter(log.splitlines())
    for line in it:
        l = line if case_sensitive else line.lower()
        is_error_line = (error_pat.search(l) and
                         not debug_pat.search(error_pat.split(l)[0])) if not search_str else search_str in l
        if is_error_line:
            append_line = line if not search_str else l[l.rfind(search_str):]
            if not distinct_errors:
                append_line = [append_line]
            matchings.append(append_line)
            if not distinct_errors:
                try:
                    it, peeker = itertools.tee(it)
                    while not info_pat.search(next(peeker)):
                        matchings[-1].append(next(it))
                except StopIteration:
                    break
    if distinct_errors:
        matchings = list(set(matchings))
    return matchings

def handle_external_tool_process(process, cmd_args):
    out, err = process.communicate()
    if (out is not None) and isinstance(out, bytes):
        out = out.decode()
    if (err is not None) and isinstance(err, bytes):
        err = err.decode()
    rc = process.returncode

    if rc != 0:
        raise ToolError(cmd_args, rc, out, err)

    ret = namedtuple('Subprocess_Return', 'stdout stderr rc')
    return ret(stdout=out, stderr=err, rc=rc)
