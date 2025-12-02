import os
from shutil import copyfile
from subprocess import check_call, run, PIPE
import logging

from ruamel.yaml import YAML

from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_node import ScyllaNode
from ccmlib.node import Status
from ccmlib import common

LOGGER = logging.getLogger("ccm")


class ScyllaDockerCluster(ScyllaCluster):
    def __init__(self, *args, **kwargs):
        super(ScyllaDockerCluster, self).__init__(*args, **kwargs)
        self.docker_image = kwargs['docker_image']

    def get_install_dir(self):
        return None

    def get_node_ip(self, nodeid):
        try:
            return self.nodelist()[nodeid-1].address()
        except IndexError:
            # HACK:
            # in case we don't have the node yet in the nodelist, we fallback to the formatted ip address,
            # even that it's not correct one, but we need this for `Cluster.add_node()`
            return super().get_node_ip(nodeid)

    def remove(self, node=None, wait_other_notice=False, other_nodes=None):
        super(ScyllaDockerCluster, self).remove(node=node, wait_other_notice=wait_other_notice, other_nodes=other_nodes)
        for node in list(self.nodes.values()):
            node.remove()

    def create_node(self, name, auto_bootstrap,
                    storage_interface, jmx_port, remote_debug_port,
                    initial_token, save=True, binary_interface=None):

        return ScyllaDockerNode(name, self, auto_bootstrap,
                                storage_interface, jmx_port, remote_debug_port,
                                initial_token, save=save, binary_interface=binary_interface,
                                scylla_manager=self._scylla_manager)

    def _update_config(self, install_dir=None):
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

    def remove_dir_with_retry(self, path):
        run(['bash', '-c', f'docker run --rm -v {path}:/node busybox chmod -R 777 /node'], stdout=PIPE, stderr=PIPE)
        super(ScyllaDockerCluster, self).remove_dir_with_retry(path)

    @staticmethod
    def is_docker():
        return True


class ScyllaDockerNode(ScyllaNode):
    def __init__(self, *args, **kwargs):
        kwargs['save'] = False
        # This line should appear before the "super" method
        self.share_directories = ['data', 'commitlogs', 'hints', 'view_hints', 'saved_caches', 'keys', 'logs']
        super(ScyllaDockerNode, self).__init__(*args, **kwargs)
        self.base_data_path = '/usr/lib/scylla'
        self.local_base_data_path = os.path.join(self.get_path(), 'data')
        self.local_yaml_path = os.path.join(self.get_path(), 'conf')
        dir_name = self.cluster.get_path().split("/")[-2].lstrip('.')
        self.docker_name = f'{dir_name}-{self.cluster.name}-{self.name}'
        self.jmx_port = "7199"  # The old CCM code expected to get a string and not int
        self.log_thread = None

    def _get_directories(self):
        dirs = {}
        for dir_name in self.share_directories + ['conf']:  # conf dir is handle in other way in `update_yaml()`
            dirs[dir_name] = os.path.join(self.get_path(), dir_name)
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
            # copy all the content of /etc/scylla out of the image without actually running it
            run(['bash', '-c', f"""
                    ID=$(docker run --rm -d {self.cluster.docker_image} tail -f /dev/null) ; 
                    docker container cp -a "${{ID}}:/etc/scylla/" - | tar --keep-old-files -x --strip-components=1 -C {self.local_yaml_path} ;
                    docker stop ${{ID}}
                """], stdout=PIPE, stderr=PIPE, universal_newlines=True)
        super(ScyllaDockerNode, self).update_yaml()

        conf_file = os.path.join(self.get_conf_dir(), common.SCYLLA_CONF)
        data = self.read_scylla_yaml()

        data['api_address'] = '0.0.0.0'
        if 'alternator_port' in data or 'alternator_https_port' in data:
            data['alternator_address'] = "0.0.0.0"

        data['data_file_directories'] = [os.path.join(self.base_data_path, 'data')]
        data[f'commitlog_directory'] = os.path.join(self.base_data_path, 'commitlogs')
        for directory in ['hints', 'view_hints', 'saved_caches']:
            data[f'{directory}_directory'] = os.path.join(self.base_data_path, directory)

        server_encryption_options = data.get("server_encryption_options", {})
        if server_encryption_options:
            keys_dir_path = os.path.join(self.get_path(), "keys")
            for key, file_path in list(server_encryption_options.items()):
                if os.path.isfile(file_path):
                    file_name = os.path.split(file_path)[1]
                    copyfile(src=file_path, dst=os.path.join(keys_dir_path, file_name))
                    server_encryption_options[key] = os.path.join(self.base_data_path, "keys", file_name)
        with open(conf_file, 'w') as f:
            YAML().dump(data, f)

    def create_docker(self, args):
        # TODO: handle smp correctly via the correct param/api (or only via commandline params)
        # TODO: mount of the data dir
        # TODO: pass down the full command line params, since the docker ones doesn't support all of them ?
        # TODO: pass down a unique tag, with the cluster name, or id, if we have such in ccm, like test_id in SCT ?
        # TODO: add volume map to: hints, ...
        # TODO: improve support for passing args to scylla docker.

        if not self.pid:
            node1 = self.cluster.nodelist()[0]
            if not self.name == node1.name:
                seeds = f"--seeds {node1.network_interfaces['storage'][0]}"
            else:
                seeds = ''
            scylla_yaml = self.read_scylla_yaml()
            ports = ""
            if 'alternator_port' in scylla_yaml:
                ports += f" -v {scylla_yaml['alternator_port']}"
            if 'alternator_https_port' in scylla_yaml:
                ports += f" -v {scylla_yaml['alternator_https_port']}"

            mount_points = [f'-v {self.local_yaml_path}:/etc/scylla',
                            '-v /tmp:/tmp']
            mount_points += [
                f'-v {os.path.join(self.get_path(),directory)}:{os.path.join(self.base_data_path, directory)}' for directory in self.share_directories
            ]
            mount_points = ' '.join(mount_points)

            res = run(['bash', '-c', f"docker run {ports} "
                                     f"{mount_points} --name {self.docker_name}  "
                                     f"-d {self.cluster.docker_image} {seeds} {' '.join(args)}"], stdout=PIPE, stderr=PIPE, universal_newlines=True)
            self.pid = res.stdout.strip()

            if not res.returncode == 0:
                LOGGER.error(res)
                raise BaseException(f'failed to create docker {self.docker_name}')

            if not self.log_thread:
                self.log_thread = DockerLogger(self, os.path.join(self.get_path(), 'logs', 'system.log'))
                self.log_thread.start()

            self.watch_log_for("supervisord started with", from_mark=0, timeout=10)

            # HACK: need to echo cause: https://github.com/scylladb/scylla-tools-java/issues/213
            run(['bash', '-c',
                 f"docker exec {self.pid} bash -c 'find /opt/ -iname cassandra.in.sh | xargs sed -i -e \\'/echo.*as the config file\"/d\\'"],
                stdout=PIPE, stderr=PIPE)

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
        network = run(['bash', '-c', f"docker inspect --format='{{{{ .NetworkSettings.IPAddress }}}}' {self.pid}"], stdout=PIPE, stderr=PIPE, universal_newlines=True)
        address = network.stdout.strip() if network.stdout else None
        self.network_interfaces = {k: (address, v[1]) for k, v in list(self.network_interfaces.items())}

    def service_start(self, service_name):
        res = run(['bash', '-c', f'docker exec {self.pid} /bin/bash -c "supervisorctl start {service_name}"'],
                  stdout=PIPE, stderr=PIPE)
        if res.returncode != 0:
            LOGGER.debug(res.stdout)
            LOGGER.error(f'service {service_name} failed to start with error\n{res.stderr}')

    def service_stop(self, service_name):
        res = run(['bash', '-c', f'docker exec {self.pid} /bin/bash -c "supervisorctl stop {service_name}"'],
                  stdout=PIPE, stderr=PIPE, universal_newlines=True)
        if res.returncode != 0:
            LOGGER.debug(res.stdout)
            LOGGER.error(f'service {service_name} failed to stop with error\n{res.stderr}')

    def service_status(self, service_name):
        res = run(['bash', '-c', f'docker exec {self.pid} /bin/bash -c "supervisorctl status {service_name}"'],
                  stdout=PIPE, stderr=PIPE, universal_newlines=True)
        if res.returncode != 0:
            LOGGER.debug(res.stdout)
            LOGGER.error(f'service {service_name} failed to get status with error\n{res.stderr}')
            return "DOWN"
        else:
            return res.stdout.split()[1]

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

    def __get_status_string(self):
        if self.status == Status.UNINITIALIZED:
            return f"{Status.DOWN} ({'Not initialized'})"
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

    @staticmethod
    def filter_args(args):
        cleaned_args = []
        # boolean args that are handled different in docker commandline parser
        if '--overprovisioned' in args:
            args.remove('--overprovisioned')
            args += ['--overprovisioned', '1']

        for arg, value in common.grouper(2, args[3:], padvalue=''):
            if arg == '--developer-mode' and value == 'true':
                value = '1'

            # handle duplicates in  https://github.com/scylladb/scylla/pull/7458 was merged
            if arg in ['--log-to-stdout', '--default-log-level']:
                continue

            # handle code before https://github.com/scylladb/scylla/pull/7458 was merged
            if arg in ['--experimental', '--seeds', '--cpuset', '--smp', '--reserve-memory', '--overprovisioned',
                       '--io-setup', '--listen-address', '--rpc-address', '--broadcast-address',
                       '--broadcast-rpc-address', '--api-address', '--alternator-address', '--alternator-port',
                       '--alternator-https-port', '--alternator-write-isolation', '--disable-version-check',
                       '--authenticator', '--authorizer', '--cluster-name', '--endpoint-snitch',
                       '--replace-address-first-boot']:
                cleaned_args.append(arg)
                cleaned_args.append(value)
        return cleaned_args

    def _start_scylla(self, args, marks, update_pid, wait_other_notice,
                      wait_for_binary_proto, ext_env):

        args = self.filter_args(args)
        # TODO: handle the cases args are changing between reboots (if there are any like that)
        self.create_docker(args)

        scylla_status = self.service_status('scylla-server')
        if scylla_status and scylla_status.upper() != 'RUNNING':
            self.service_start('scylla-server')
            # self.service_start('scylla-jmx')

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
            self.service_stop('scylla-server')
        else:
            res = run(['bash', '-c', f"docker exec {self.pid} bash -c 'kill -9 `supervisorctl pid scylla`'"],
                      stdout=PIPE, stderr=PIPE)
            LOGGER.debug(res)
            res = run(['bash', '-c', f"docker exec {self.pid} bash -c 'kill -9 `supervisorctl pid scylla-jmx`'"],
                      stdout=PIPE, stderr=PIPE)
            LOGGER.debug(res)

    def clear(self, *args, **kwargs):
        # change file permissions so it can be deleted
        run(['bash', '-c', f'docker run --rm -v {self.get_path()}:/node busybox chmod -R 777 /node'], stdout=PIPE, stderr=PIPE)
        super(ScyllaDockerNode, self).clear(*args, **kwargs)

    def remove(self):
        run(['bash', '-c', f'docker rm --volumes -f {self.pid}'], stdout=PIPE, stderr=PIPE)

    def _start_jmx(self, data):
        jmx_status = self.service_status('scylla-jmx')
        if jmx_status and jmx_status.upper() != 'RUNNING':
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

        scylla_status = self.service_status('scylla-server')
        if scylla_status and scylla_status.upper() == 'RUNNING':
            self.status = Status.UP
        else:
            self.status = Status.DOWN
        self._update_config()

    def _wait_java_up(self, ip_addr, jmx_port):
        # TODO: do a better implementation of it
        return True

    def _update_pid(self, process):
        pass

    def get_tool(self, toolname):
        return ['docker', 'exec', '-i', f'{self.pid}', f'{toolname}']

    def _find_cmd(self, command_name):
        return self.get_tool(command_name)

    def get_sstables(self, *args, **kwargs):
        files = super(ScyllaDockerNode, self).get_sstables(*args, **kwargs)
        return [f.replace(self.get_path(), '/usr/lib/scylla') for f in files]

    def get_env(self):
        return os.environ.copy()

    def copy_config_files(self):
        # no need to copy any config file, since we are running in docker, and everything is available inside it
        pass

    def import_config_files(self):
        self.update_yaml()

    def kill(self, __signal):
        run(['bash', '-c', f"docker exec {self.pid} bash -c 'kill -{__signal} `supervisorctl pid scylla`'"],
            stdout=PIPE, stderr=PIPE)

    def unlink(self, file_path):
        run(['bash', '-c', f'docker run --rm -v {file_path}:{file_path} busybox rm {file_path}'], stdout=PIPE, stderr=PIPE)

    def chmod(self, file_path, permissions):
        path_inside_docker = file_path.replace(self.get_path(), self.base_data_path)
        run(['bash', '-c', f'docker run --rm -v {file_path}:{path_inside_docker} busybox chmod -R {permissions} {path_inside_docker}'],
            stdout=PIPE, stderr=PIPE)

    def rmtree(self, path):
        run(['bash', '-c', f'docker run --rm -v {self.get_path()}:/node busybox chmod -R 777 /node'], stdout=PIPE, stderr=PIPE)
        super(ScyllaDockerNode, self).rmtree(path)


class DockerLogger:
    def __init__(self, node, target_log_file: str):
        self._node = node
        self._target_log_file = target_log_file

    @property
    def _logger_cmd(self) -> str:
        return f'docker logs -f {self._node.pid} >> {self._target_log_file} 2>&1 &'

    def start(self):
        check_call(['bash', '-c', self._logger_cmd])
