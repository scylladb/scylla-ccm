import os
from subprocess import run, PIPE
import logging

import yaml

from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_node import ScyllaNode
from ccmlib.node import Status
from ccmlib import common


LOGGER = logging.getLogger("ccm")


class ScyllaDockerCluster(ScyllaCluster):
    def __init__(self, *args, **kwargs):
        super(ScyllaDockerCluster, self).__init__(*args, **kwargs)
        self.docker_image = kwargs['docker_image']

    def create_node(self, name, auto_bootstrap, thrift_interface,
                    storage_interface, jmx_port, remote_debug_port,
                    initial_token, save=True, binary_interface=None):

        return ScyllaDockerNode(name, self, auto_bootstrap, thrift_interface,
                                storage_interface, jmx_port, remote_debug_port,
                                initial_token, save=save, binary_interface=binary_interface,
                                scylla_manager=self._scylla_manager)

    def _update_config(self):
        node_list = [node.name for node in list(self.nodes.values())]
        seed_list = [node.name for node in self.seeds]
        filename = os.path.join(self.get_path(), 'cluster.conf')
        docker_image = self.docker_image
        with open(filename, 'w') as f:
            yaml.safe_dump({
                'name': self.name,
                'nodes': node_list,
                'seeds': seed_list,
                'partitioner': self.partitioner,
                'config_options': self._config_options,
                'id': self.id,
                'ipprefix': self.ipprefix,
                'docker_image': docker_image
            }, f)


class ScyllaDockerNode(ScyllaNode):
    def __init__(self, *args, **kwargs):
        kwargs['save'] = False
        super(ScyllaDockerNode, self).__init__(*args, **kwargs)
        self.base_data_path = '/usr/lib/scylla'
        self.local_base_data_path = os.path.join(self.get_path(), 'data')
        self.local_yaml_path = os.path.join(self.get_path(), 'conf')
        self.docker_name = f'{self.cluster.get_path().split("/")[-1]}-{self.cluster.name}-{self.name}'
        self.jmx_port = "7199"  # The old CCM code expected to get a string and not int
        self.log_thread = None

    def _get_directories(self):
        dirs = {}
        for i in ['data', 'commitlogs', 'conf', 'logs', 'hints', 'view_hints', 'saved_caches']:
            dirs[i] = os.path.join(self.get_path(), i)
        return dirs

    @staticmethod
    def get_docker_name():
        return run(["docker", "ps", "-a"], stdout=PIPE).stdout.decode('utf-8').split()[-1]

    def is_scylla(self):
        return True

    @staticmethod
    def is_docker():
        return True

    def read_scylla_yaml(self):
        conf_file = os.path.join(self.get_conf_dir(), common.SCYLLA_CONF)
        with open(conf_file, 'r') as f:
            return yaml.safe_load(f)

    def update_yaml(self):
        if not os.path.exists(f'{self.local_yaml_path}/scylla.yaml'):
            run(['bash', '-c', f'docker run --rm --entrypoint cat {self.cluster.docker_image} /etc/scylla/scylla.yaml > {self.local_yaml_path}/scylla.yaml'])
        super(ScyllaDockerNode, self).update_yaml()

        conf_file = os.path.join(self.get_conf_dir(), common.SCYLLA_CONF)
        data = self.read_scylla_yaml()

        data['api_address'] = '127.0.0.1'
        if 'alternator_port' in data or 'alternator_https_port' in data:
            data['alternator_address'] = "0.0.0.0"

        data['data_file_directories'] = [os.path.join(self.base_data_path, 'data')]
        data[f'commitlog_directory'] = os.path.join(self.base_data_path, 'commitlogs')
        for directory in ['hints', 'view_hints', 'saved_caches']:
            data[f'{directory}_directory'] = os.path.join(self.base_data_path, directory)

        with open(conf_file, 'w') as f:
            yaml.safe_dump(data, f, default_flow_style=False)

    def create_docker(self):
        # TODO: handle smp correctly via the correct param/api (or only via commandline params)
        # TODO: mount of the data dir
        # TODO: pass down the full command line params, since the docker ones doesn't support all of them ?
        # TODO: pass down a unique tag, with the cluster name, or id, if we have such in ccm, like test_id in SCT ?
        # TODO: add volume map to: hints, ...

        if not self.pid:
            node1 = self.cluster.nodelist()[0]
            if not self.name == node1.name:
                seeds = f"--seeds {node1.network_interfaces['thrift'][0]}"
            else:
                seeds = ''
            scylla_yaml = self.read_scylla_yaml()
            ports = ""
            if 'alternator_port' in scylla_yaml:
                ports += f" -v {scylla_yaml['alternator_port']}"
            if 'alternator_https_port' in scylla_yaml:
                ports += f" -v {scylla_yaml['alternator_https_port']}"

            mount_points = []
            for directory in ['data', 'commitlogs', 'hints', 'view_hints', 'saved_caches']:
                mount_points.append(
                    f'-v {os.path.join(self.get_path(),directory)}:{os.path.join(self.base_data_path, directory)}')

            res = run(['bash', '-c', f"docker run {ports} -v {self.local_yaml_path}/scylla.yaml:/etc/scylla/scylla.yaml "
                                     f"{' '.join(mount_points)} --name {self.docker_name} -v /tmp:/tmp "
                                     f"-d {self.cluster.docker_image} --smp 1 {seeds}"], stdout=PIPE, stderr=PIPE)
            self.pid = res.stdout.decode('utf-8').strip()

            if not res.returncode == 0:
                LOGGER.error(res)
                raise BaseException(f'failed to create docker {self.docker_name}')

            if not self.log_thread:
                self.log_thread = DockerLogger(self, os.path.join(self.get_path(), 'logs', 'system.log'))
                self.log_thread.start()

            self.watch_log_for("supervisord started with", from_mark=0, timeout=10)

            # disable autorestart on scylla and scylla-jmx
            run(['bash', '-c',
                 f"docker exec {self.pid} bash -c 'echo \"autorestart=false\" >> /etc/supervisord.conf.d/scylla-server.conf'"],
                stdout=PIPE, stderr=PIPE)
            run(['bash', '-c',
                 f"docker exec {self.pid} bash -c 'echo \"autorestart=false\" >> /etc/supervisord.conf.d/scylla-jmx.conf'"],
                stdout=PIPE, stderr=PIPE)
            reread = run(['bash', '-c', f"docker exec {self.pid} supervisorctl update"], stdout=PIPE,
                         stderr=PIPE)

            LOGGER.debug(reread)

        if not self.log_thread:
            self.log_thread = DockerLogger(self, os.path.join(self.get_path(), 'logs', 'system.log'))
            self.log_thread.start()

        # replace addresses
        network = run(['bash', '-c', f"docker inspect --format='{{{{ .NetworkSettings.IPAddress }}}}' {self.pid}"], stdout=PIPE, stderr=PIPE)
        address = network.stdout.decode('utf-8').strip() if network.stdout else None
        self.network_interfaces = {k: (address, v[1]) for k, v in self.network_interfaces.items()}

    def service_start(self, service_name):
        res = run(['bash', '-c', f'docker exec {self.pid} /bin/bash -c "supervisorctl start {service_name}"'],
                  stdout=PIPE, stderr=PIPE)
        if res.returncode != 0:
            LOGGER.debug(res.stdout)
            LOGGER.error(f'service {service_name} failed to start with error\n{res.stderr}')

    def service_stop(self, service_name):
        res = run(['bash', '-c', f'docker exec {self.pid} /bin/bash -c "supervisorctl stop {service_name}"'],
                  stdout=PIPE, stderr=PIPE)
        if res.returncode != 0:
            LOGGER.debug(res.stdout)
            LOGGER.error(f'service {service_name} failed to stop with error\n{res.stderr}')

    def service_status(self, service_name):
        res = run(['bash', '-c', f'docker exec {self.pid} /bin/bash -c "supervisorctl status {service_name}"'],
                  stdout=PIPE, stderr=PIPE)
        if res.returncode != 0:
            LOGGER.debug(res.stdout)
            LOGGER.error(f'service {service_name} failed to stop with error\n{res.stderr}')
            return "DOWN"
        else:
            return res.stdout.decode('utf-8').split()[1]

    def show(self, only_status=False, show_cluster=True):
        """
        Print infos on this node configuration.
        """
        self.__update_status()
        indent = ''.join([" " for i in range(0, len(self.name) + 2)])
        print("%s: %s" % (self.name, self.__get_status_string()))
        if not only_status:
            if show_cluster:
                print("%s%s=%s" % (indent, 'cluster', self.cluster.name))
            print("%s%s=%s" % (indent, 'auto_bootstrap', self.auto_bootstrap))
            print("%s%s=%s" % (indent, 'thrift', self.network_interfaces['thrift']))
            if self.network_interfaces['binary'] is not None:
                print("%s%s=%s" % (indent, 'binary', self.network_interfaces['binary']))
            print("%s%s=%s" % (indent, 'storage', self.network_interfaces['storage']))
            print("%s%s=%s" % (indent, 'jmx_port', self.jmx_port))
            print("%s%s=%s" % (indent, 'remote_debug_port', self.remote_debug_port))
            print("%s%s=%s" % (indent, 'initial_token', self.initial_token))
            if self.pid:
                print("%s%s=%s" % (indent, 'pid', self.pid))

    def __get_status_string(self):
        if self.status == Status.UNINITIALIZED:
            return "%s (%s)" % (Status.DOWN, "Not initialized")
        else:
            return self.status

    def _update_config(self):
        dir_name = self.get_path()
        if not os.path.exists(dir_name):
            return
        filename = os.path.join(dir_name, 'node.conf')
        docker_id = self.pid if self.pid else None
        docker_name = self.docker_name if self.docker_name else ''
        values = {
            'name': self.name,
            'status': self.status,
            'auto_bootstrap': self.auto_bootstrap,
            'interfaces': self.network_interfaces,
            'jmx_port': self.jmx_port,
            'docker_id': docker_id,
            'docker_name': docker_name,
            'install_dir': '',
        }
        if self.initial_token:
            values['initial_token'] = self.initial_token
        if self.remote_debug_port:
            values['remote_debug_port'] = self.remote_debug_port
        if self.data_center:
            values['data_center'] = self.data_center
        if self.workload is not None:
            values['workload'] = self.workload
        with open(filename, 'w') as f:
            yaml.safe_dump(values, f)

    def _start_scylla(self, args, marks, update_pid, wait_other_notice,
                      wait_for_binary_proto, ext_env):
        self.create_docker()

        scylla_status = self.service_status('scylla')
        if scylla_status and scylla_status.upper() != 'RUNNING':
            self.service_start('scylla')

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

    def do_stop(self, gently=True):
        """
        Stop the node.
          - gently: Let Scylla and Scylla JMX clean up and shut down properly.
            Otherwise do a 'kill -9' which shuts down faster.
        """
        if gently:
            self.service_stop('scylla-jmx')
            self.service_stop('scylla')
        else:
            res = run(['bash', '-c', f"docker exec {self.pid} bash -c 'kill -9 `supervisorctl pid scylla`'"],
                      stdout=PIPE, stderr=PIPE)
            LOGGER.debug(res)
            res = run(['bash', '-c', f"docker exec {self.pid} bash -c 'kill -9 `supervisorctl pid scylla-jmx`'"],
                      stdout=PIPE, stderr=PIPE)
            LOGGER.debug(res)

    def clear(self, *args, **kwargs):
        # change file permissions so it can be deleted
        run(['bash', '-c', f'docker run -v {self.get_path()}:/node busybox chmod -R 777 /node'], stdout=PIPE, stderr=PIPE)

        run(['bash', '-c', f'docker rm --volumes -f {self.pid}'], stdout=PIPE, stderr=PIPE)
        if self.log_thread:
            self.log_thread.stop(10)
        super(ScyllaDockerNode, self).clear(*args, **kwargs)

    def _start_jmx(self, data):
        jmx_status = self.service_status('scylla-jmx')
        if not jmx_status and jmx_status.upper() == 'RUNNING':
            self.service_start('scylla-jmx')

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

    def __update_status(self):
        if self.pid is None:
            if self.status == Status.UP or self.status == Status.DECOMMISSIONED:
                self.status = Status.DOWN
            return

        old_status = self.status

        scylla_status = self.service_status('scylla')
        if scylla_status and scylla_status.upper() == 'RUNNING':
            self.status = Status.UP
        else:
            self.status = Status.DOWN
        self._update_config()

    def _wait_java_up(self, ip_addr, jmx_port):
        return True

    def _update_pid(self, process):
        pass

    def get_tool(self, toolname):
        return ['docker',  'exec', '-i',  f'{self.pid}', f'{toolname}']

    def _find_cmd(self, command_name):
        return self.get_tool(command_name)

    def get_sstables(self, keyspace, column_family):
        files = super(ScyllaDockerNode, self).get_sstables(keyspace=keyspace, column_family=column_family)
        return [f.replace(self.get_path(), '/usr/lib/scylla') for f in files]

    def get_env(self):
        return os.environ.copy()

    def copy_config_files(self):
        # no need to copy any config file, since we are running in docker, and everything is available inside it
        pass

    def import_config_files(self):
        # no need to import any config file, since we are running in docker, and everything is available inside it
        pass

    def unlink(self, file_path):
        run(['bash', '-c', f'docker run -v {self.get_path()}:/node busybox chmod -R 777 /node'], stdout=PIPE, stderr=PIPE)
        super(ScyllaDockerNode, self).unlink(file_path)

    def chmod(self, file_path, permissions):
        path_inside_docker = file_path.replace(self.get_path(), self.base_data_path)
        run(['bash', '-c', f'docker run -v {file_path}:{path_inside_docker} busybox chmod -R {permissions} {path_inside_docker}'],
            stdout=PIPE, stderr=PIPE)


import subprocess
from threading import Thread, Event as ThreadEvent


class DockerLogger:
    _child_process = None

    def __init__(self, node, target_log_file: str):
        self._node = node
        self._target_log_file = target_log_file
        self._thread = Thread(target=self._thread_body, daemon=True)
        self._termination_event = ThreadEvent()

    @property
    def _logger_cmd(self) -> str:
        return f'docker logs -f {self._node.pid} >>{self._target_log_file} 2>&1'

    def _thread_body(self):
        while not self._termination_event.wait(0.1):
            try:
                self._child_process = subprocess.Popen(self._logger_cmd, shell=True)
                self._child_process.wait()
            except Exception as ex:  # pylint: disable=bare-except
                print(ex)
                raise

    def start(self):
        self._termination_event.clear()
        self._thread.start()

    def stop(self, timeout=None):
        self._termination_event.set()
        if self._child_process:
            self._child_process.kill()
        self._thread.join(timeout)
