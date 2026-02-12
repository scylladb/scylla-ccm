import os
import warnings
from shutil import copyfile
from subprocess import check_call, run, PIPE
import logging

from ruamel.yaml import YAML

from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_node import ScyllaNode
from ccmlib.node import Status
from ccmlib import common
from ccmlib.container_client import (
    get_container_client,
    ContainerClientError,
    ContainerImageNotFoundError,
)

LOGGER = logging.getLogger("ccm")


class ScyllaDockerCluster(ScyllaCluster):
    def __init__(self, *args, **kwargs):
        super(ScyllaDockerCluster, self).__init__(*args, **kwargs)
        self.docker_image = kwargs['docker_image']
        self.container_runtime = kwargs.get('container_runtime', None)
        self._container_client = None
        self.cluster_network = None
        
        # Validate Docker image
        self._validate_docker_image()
        
        # Setup cluster network for isolation
        self._setup_cluster_network()
    
    def get_container_client(self):
        """Get or create the container client instance."""
        if self._container_client is None:
            try:
                self._container_client = get_container_client(self.container_runtime)
                LOGGER.info(f"Using container runtime: {self._container_client.runtime_name}")
            except ContainerClientError as e:
                LOGGER.error(f"Failed to initialize container client: {e}")
                raise
        return self._container_client
    
    def _validate_docker_image(self):
        """Validate that the Docker image exists or can be pulled."""
        try:
            client = self.get_container_client()
            if not client.image_exists(self.docker_image):
                LOGGER.info(f"Docker image '{self.docker_image}' not found locally, attempting to pull...")
                if not client.pull_image(self.docker_image):
                    raise ContainerImageNotFoundError(
                        f"Failed to pull Docker image '{self.docker_image}'. "
                        "Please check the image name and your network connection."
                    )
        except ContainerClientError as e:
            LOGGER.warning(f"Could not validate Docker image: {e}")
            # Continue anyway - the error will surface when trying to run containers
    
    def _setup_cluster_network(self):
        """Create an isolated network for this cluster."""
        try:
            client = self.get_container_client()
            # Use cluster name as network name (sanitized)
            network_name = f"ccm-{self.name}"
            # Replace invalid characters
            network_name = network_name.replace('_', '-').lower()
            
            if client.create_network(network_name):
                self.cluster_network = network_name
                LOGGER.info(f"Created cluster network: {network_name}")
            else:
                LOGGER.warning(f"Could not create cluster network, will use default network")
        except ContainerClientError as e:
            LOGGER.warning(f"Failed to setup cluster network: {e}")
            # Continue without custom network

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
        
        # Clean up cluster network
        if self.cluster_network:
            try:
                client = self.get_container_client()
                client.remove_network(self.cluster_network)
            except ContainerClientError as e:
                LOGGER.warning(f"Failed to remove cluster network: {e}")

    def create_node(self, name, auto_bootstrap,
                    storage_interface, jmx_port, remote_debug_port,
                    initial_token, save=True, binary_interface=None, thrift_interface=None):
        if thrift_interface is not None:
            warnings.warn("thrift_interface is deprecated and will be removed in a future version", DeprecationWarning, stacklevel=2)

        return ScyllaDockerNode(name, self, auto_bootstrap,
                                storage_interface, jmx_port, remote_debug_port,
                                initial_token, save=save, binary_interface=binary_interface,
                                scylla_manager=self._scylla_manager)

    def _update_config(self, install_dir=None):
        node_list = [node.name for node in list(self.nodes.values())]
        seed_list = [node.name for node in self.seeds]
        filename = os.path.join(self.get_path(), 'cluster.conf')
        docker_image = self.docker_image
        yaml = YAML()
        with open(filename, 'w') as f:
            config_data = {
                'name': self.name,
                'nodes': node_list,
                'seeds': seed_list,
                'partitioner': self.partitioner,
                'config_options': self._config_options,
                'id': self.id,
                'ipprefix': self.ipprefix,
                'docker_image': docker_image
            }
            # Save container runtime if specified
            if self.container_runtime:
                config_data['container_runtime'] = self.container_runtime
            if self.cluster_network:
                config_data['cluster_network'] = self.cluster_network
            yaml.dump(config_data, f)

    def remove_dir_with_retry(self, path):
        """Remove directory with retry, handling Docker file permissions."""
        try:
            client = self.get_container_client()
            # Use busybox container to fix permissions
            client.run_container(
                image='busybox',
                name=f'ccm-chmod-{os.path.basename(path)}',
                volumes={path: '/node'},
                command=['chmod', '-R', '777', '/node'],
                detach=False,
                remove=True,
            )
        except ContainerClientError as e:
            LOGGER.warning(f"Failed to fix permissions using container: {e}")
            # Fallback to old method
            run(['bash', '-c', f'docker run --rm -v {path}:/node busybox chmod -R 777 /node'], 
                stdout=PIPE, stderr=PIPE)
        
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

    def get_container_client(self):
        """Get the container client from the cluster."""
        return self.cluster.get_container_client()

    def _get_directories(self):
        dirs = {}
        for dir_name in self.share_directories + ['conf']:  # conf dir is handle in other way in `update_yaml()`
            dirs[dir_name] = os.path.join(self.get_path(), dir_name)
        return dirs

    @staticmethod
    def get_docker_name():
        # Deprecated - keeping for backwards compatibility
        return run(["docker", "ps", "-a"], stdout=PIPE).stdout.decode('utf-8').split()[-1]

    def is_scylla(self):
        return True

    @staticmethod
    def is_docker():
        return True

    def read_scylla_yaml(self):
        conf_file = os.path.join(self.get_conf_dir(), common.SCYLLA_CONF)
        yaml = YAML()
        with open(conf_file, 'r') as f:
            return yaml.load(f)

    def update_yaml(self):
        """Extract and update scylla.yaml configuration."""
        if not os.path.exists(f'{self.local_yaml_path}/scylla.yaml'):
            # Copy all the content of /etc/scylla out of the image using container client
            try:
                client = self.get_container_client()
                
                # Start a temporary container to extract config
                temp_name = f'ccm-extract-config-{self.docker_name}'
                container_id = client.run_container(
                    image=self.cluster.docker_image,
                    name=temp_name,
                    command=['tail', '-f', '/dev/null'],
                    detach=True,
                )
                
                # Copy config files from container
                # Use docker cp command for now (TODO: add to container client)
                run(['bash', '-c', 
                     f'docker container cp -a "{container_id}:/etc/scylla/" - | '
                     f'tar --keep-old-files -x --strip-components=1 -C {self.local_yaml_path}'],
                    stdout=PIPE, stderr=PIPE, universal_newlines=True)
                
                # Stop and remove temporary container
                client.stop_container(container_id)
                client.remove_container(container_id)
                
            except ContainerClientError as e:
                LOGGER.error(f"Failed to extract config from Docker image: {e}")
                # Fallback to old method
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
        """Create and start the Docker container for this node."""
        if not self.pid:
            try:
                client = self.get_container_client()
                
                # Determine seeds
                node1 = self.cluster.nodelist()[0]
                if not self.name == node1.name:
                    seeds = f"--seeds {node1.network_interfaces['storage'][0]}"
                else:
                    seeds = ''
                
                # Read scylla.yaml to get ports
                scylla_yaml = self.read_scylla_yaml()
                
                # Prepare port mappings
                ports = {}
                if 'alternator_port' in scylla_yaml:
                    port = str(scylla_yaml['alternator_port'])
                    ports[port] = port
                if 'alternator_https_port' in scylla_yaml:
                    port = str(scylla_yaml['alternator_https_port'])
                    ports[port] = port

                # Prepare volume mounts
                volumes = {
                    self.local_yaml_path: '/etc/scylla',
                    '/tmp': '/tmp'
                }
                for directory in self.share_directories:
                    host_path = os.path.join(self.get_path(), directory)
                    container_path = os.path.join(self.base_data_path, directory)
                    volumes[host_path] = container_path

                # Build command
                command = []
                if seeds:
                    command.extend(seeds.split())
                command.extend(args)

                # Run container
                self.pid = client.run_container(
                    image=self.cluster.docker_image,
                    name=self.docker_name,
                    volumes=volumes,
                    ports=ports if ports else None,
                    network=self.cluster.cluster_network,
                    command=command,
                    detach=True,
                )
                
                LOGGER.info(f"Started Docker container for node '{self.name}': {self.pid[:12]}")

            except ContainerClientError as e:
                LOGGER.error(f"Failed to create Docker container: {e}")
                raise BaseException(f'Failed to create docker {self.docker_name}: {e}')

            # Start log thread
            if not self.log_thread:
                self.log_thread = DockerLogger(self, os.path.join(self.get_path(), 'logs', 'system.log'))
                self.log_thread.start()

            self.watch_log_for("supervisord started with", from_mark=0, timeout=10)

            # Configuration fixes
            client = self.get_container_client()
            
            # HACK: need to echo cause: https://github.com/scylladb/scylla-tools-java/issues/213
            client.exec_command(
                self.pid,
                ['bash', '-c', 
                 'find /opt/ -iname cassandra.in.sh | xargs sed -i -e \'/echo.*as the config file"/d\'']
            )

            # Disable autorestart on scylla and scylla-jmx
            client.exec_command(
                self.pid,
                ['bash', '-c', 'echo "autorestart=false" >> /etc/supervisord.conf.d/scylla-server.conf']
            )
            client.exec_command(
                self.pid,
                ['bash', '-c', 'echo "autorestart=false" >> /etc/supervisord.conf.d/scylla-jmx.conf']
            )
            
            returncode, stdout, stderr = client.exec_command(self.pid, ['supervisorctl', 'update'])
            LOGGER.debug(f"supervisorctl update: {stdout}")

        if not self.log_thread:
            self.log_thread = DockerLogger(self, os.path.join(self.get_path(), 'logs', 'system.log'))
            self.log_thread.start()

        # Get container IP address
        client = self.get_container_client()
        address = client.get_container_ip(self.pid)
        if address:
            self.network_interfaces = {k: (address, v[1]) for k, v in list(self.network_interfaces.items())}
        else:
            LOGGER.warning(f"Could not get IP address for container {self.pid}")

    def service_start(self, service_name):
        """Start a service inside the container using supervisorctl."""
        try:
            client = self.get_container_client()
            returncode, stdout, stderr = client.exec_command(
                self.pid,
                ['supervisorctl', 'start', service_name]
            )
            if returncode != 0:
                LOGGER.debug(stdout)
                LOGGER.error(f'Service {service_name} failed to start with error\n{stderr}')
        except ContainerClientError as e:
            LOGGER.error(f'Failed to start service {service_name}: {e}')

    def service_stop(self, service_name):
        """Stop a service inside the container using supervisorctl."""
        try:
            client = self.get_container_client()
            returncode, stdout, stderr = client.exec_command(
                self.pid,
                ['supervisorctl', 'stop', service_name]
            )
            if returncode != 0:
                LOGGER.debug(stdout)
                LOGGER.error(f'Service {service_name} failed to stop with error\n{stderr}')
        except ContainerClientError as e:
            LOGGER.error(f'Failed to stop service {service_name}: {e}')

    def service_status(self, service_name):
        """Get the status of a service inside the container."""
        try:
            client = self.get_container_client()
            returncode, stdout, stderr = client.exec_command(
                self.pid,
                ['supervisorctl', 'status', service_name]
            )
            if returncode != 0:
                LOGGER.debug(stdout)
                LOGGER.error(f'Service {service_name} failed to get status with error\n{stderr}')
                return "DOWN"
            else:
                # Parse supervisorctl output (format: "service_name STATUS ...")
                parts = stdout.split()
                return parts[1] if len(parts) > 1 else "UNKNOWN"
        except ContainerClientError as e:
            LOGGER.error(f'Failed to get status of service {service_name}: {e}')
            return "DOWN"

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
            try:
                client = self.get_container_client()
                # Get scylla PID and kill it
                returncode, stdout, stderr = client.exec_command(
                    self.pid,
                    ['bash', '-c', 'kill -9 `supervisorctl pid scylla`']
                )
                LOGGER.debug(f"Kill scylla: {stdout}")
                
                # Get scylla-jmx PID and kill it
                returncode, stdout, stderr = client.exec_command(
                    self.pid,
                    ['bash', '-c', 'kill -9 `supervisorctl pid scylla-jmx`']
                )
                LOGGER.debug(f"Kill scylla-jmx: {stdout}")
            except ContainerClientError as e:
                LOGGER.error(f"Failed to kill processes: {e}")

    def clear(self, *args, **kwargs):
        """Clear node data, handling Docker file permissions."""
        try:
            client = self.get_container_client()
            # Use busybox container to fix permissions
            client.run_container(
                image='busybox',
                name=f'ccm-clear-{self.docker_name}',
                volumes={self.get_path(): '/node'},
                command=['chmod', '-R', '777', '/node'],
                detach=False,
                remove=True,
            )
        except ContainerClientError as e:
            LOGGER.warning(f"Failed to fix permissions: {e}")
            # Fallback to old method
            run(['bash', '-c', f'docker run --rm -v {self.get_path()}:/node busybox chmod -R 777 /node'], 
                stdout=PIPE, stderr=PIPE)
        
        super(ScyllaDockerNode, self).clear(*args, **kwargs)

    def remove(self):
        """Remove the Docker container."""
        if self.pid:
            try:
                client = self.get_container_client()
                client.remove_container(self.pid, force=True, volumes=True)
            except ContainerClientError as e:
                LOGGER.warning(f"Failed to remove container: {e}")
                # Fallback to old method
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
        """Get command to run a tool in the Docker container."""
        # Keep backward compatibility with exec approach
        client = self.get_container_client()
        return [client.runtime_name, 'exec', '-i', f'{self.pid}', f'{toolname}']

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
        """Send a signal to the Scylla process in the container."""
        try:
            client = self.get_container_client()
            client.exec_command(
                self.pid,
                ['bash', '-c', f'kill -{__signal} `supervisorctl pid scylla`']
            )
        except ContainerClientError as e:
            LOGGER.error(f"Failed to send signal {__signal}: {e}")

    def unlink(self, file_path):
        """Unlink a file using a temporary container."""
        try:
            client = self.get_container_client()
            client.run_container(
                image='busybox',
                name=f'ccm-unlink-{os.path.basename(file_path)}',
                volumes={file_path: file_path},
                command=['rm', file_path],
                detach=False,
                remove=True,
            )
        except ContainerClientError as e:
            LOGGER.warning(f"Failed to unlink {file_path}: {e}")
            # Fallback
            run(['bash', '-c', f'docker run --rm -v {file_path}:{file_path} busybox rm {file_path}'], 
                stdout=PIPE, stderr=PIPE)

    def chmod(self, file_path, permissions):
        """Change file permissions using a temporary container."""
        path_inside_docker = file_path.replace(self.get_path(), self.base_data_path)
        try:
            client = self.get_container_client()
            client.run_container(
                image='busybox',
                name=f'ccm-chmod-{os.path.basename(file_path)}',
                volumes={file_path: path_inside_docker},
                command=['chmod', '-R', str(permissions), path_inside_docker],
                detach=False,
                remove=True,
            )
        except ContainerClientError as e:
            LOGGER.warning(f"Failed to chmod {file_path}: {e}")
            # Fallback
            run(['bash', '-c', 
                 f'docker run --rm -v {file_path}:{path_inside_docker} busybox chmod -R {permissions} {path_inside_docker}'],
                stdout=PIPE, stderr=PIPE)

    def rmtree(self, path):
        """Remove a directory tree, handling Docker permissions."""
        try:
            client = self.get_container_client()
            client.run_container(
                image='busybox',
                name=f'ccm-rmtree-{os.path.basename(path)}',
                volumes={self.get_path(): '/node'},
                command=['chmod', '-R', '777', '/node'],
                detach=False,
                remove=True,
            )
        except ContainerClientError as e:
            LOGGER.warning(f"Failed to fix permissions before rmtree: {e}")
            # Fallback
            run(['bash', '-c', f'docker run --rm -v {self.get_path()}:/node busybox chmod -R 777 /node'], 
                stdout=PIPE, stderr=PIPE)
        
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
