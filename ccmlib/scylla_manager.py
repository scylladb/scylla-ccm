import os
import shutil
import time
import subprocess
import signal
import yaml
import uuid
import datetime
import tarfile

from six import print_
from distutils.version import LooseVersion

import docker

from ccmlib import common
from ccmlib.node import NodeError


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

    def _version(self):
        stdout, _ = self.sctool(["version"], ignore_exit_status=True)
        version_string = stdout[stdout.find(": ") + 2:].strip()  # Removing unnecessary information
        version_code = LooseVersion(version_string)
        return version_code

    def _install(self, install_dir):
        self._copy_config_files(install_dir)
        self._copy_bin_files(install_dir)
        self.version = self._version()
        self._update_config(install_dir)

    def _get_api_address(self):
        return "%s:5080" % self.scylla_cluster.get_node_ip(1)

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
        data['prometheus'] = "{}:56091".format(self.scylla_cluster.get_node_ip(1))
        # Changing port to 56091 since the manager and the first node share the same ip and 56090 is already in use
        # by the first node's manager agent
        data["debug"] = "{}:5611".format(self.scylla_cluster.get_node_ip(1))
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

        api_interface = common.parse_interface(self._get_api_address(), 5080)
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

    def sctool(self, cmd, ignore_exit_status=False):
        sctool = os.path.join(self._get_path(), 'bin', 'sctool')
        args = [sctool, '--api-url', "http://%s/api/v1" % self._get_api_address()]
        args += cmd
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        stdout, stderr = p.communicate()
        exit_status = p.wait()
        if exit_status != 0 and not ignore_exit_status:
            raise Exception(" ".join(args), exit_status, stdout, stderr)
        return stdout, stderr


class ScyllaManagerDocker:
    def __init__(self, scylla_cluster, install_dir=None):
        self.scylla_cluster = scylla_cluster
        self.install_dir = install_dir
        self.auth_token = str(uuid.uuid4())
        #TODO make configurable
        self.image_name = "scylladb/scylla-manager:latest"
        self.image = None
        self.container = None
        self.client = docker.from_env()
        self._prepare()

    def _prepare(self):
        self.image = self.client.images.pull(self.image_name)
        try:
            self.container = self.client.containers.run(self.image, detach=True, network_mode="host", auto_remove=True)
            self.version = self._version()
            self._copy_config_files()
        finally:
            self.container.stop()
        self._update_config()

    def _copy_config_files(self):
        obj, stat = self.container.get_archive("/etc/scylla-manager/scylla-manager.yaml")
        with open(os.path.join(self._get_path(), "scylla-manager.yaml"), "wb") as dst:
            with tarfile.open(fileobj=obj, mode='r:') as t:
                # There should be only one member
                for member in t.members:
                    contents = t.extractfile(member).read()
                    dst.write(contents)

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
        data['prometheus'] = "{}:56091".format(self.scylla_cluster.get_node_ip(1))
        # Changing port to 56091 since the manager and the first node share the same ip and 56090 is already in use
        # by the first node's manager agent
        data["debug"] = "{}:5611".format(self.scylla_cluster.get_node_ip(1))
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

    def _version(self):
        stdout, _ = self.sctool(["version"], ignore_exit_status=True)
        version_string = stdout[stdout.find(": ") + 2:].strip()  # Removing unnecessary information
        version_code = LooseVersion(version_string)
        return version_code

    def _get_api_address(self):
        return "%s:5080" % self.scylla_cluster.get_node_ip(1)

    @property
    def is_agent_available(self):
        return False

    def _get_path(self):
        return os.path.join(self.scylla_cluster.get_path(), common.SCYLLAMANAGER_DIR)

    def _update_pid(self):
        pass

    def start(self):
        self.container = self.client.containers.run(self.image, detach=True, network_mode="host", auto_remove=True,
                                                    volumes={
                                                        self._get_path(): {"bind": "/etc/scylla-manager/", "mode": 'ro'}
                                                    })

    def stop(self, gently):
        if self.container:
            self.container.stop()

    def sctool(self, cmd, ignore_exit_status=False):
        if self.container is None:
            raise Exception("Scylla Manager container not started")

        exit_status, (stdout, stderr) = self.container.exec_run("/usr/bin/sctool {}".format(cmd), demux=True)
        if exit_status != 0 and not ignore_exit_status:
            raise Exception(cmd, exit_status, stdout, stderr)
        return stdout, stderr