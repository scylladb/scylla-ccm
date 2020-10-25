import time
import os
from subprocess import run, PIPE

import yaml

from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_node import ScyllaNode
from ccmlib.node import Status
from ccmlib import common

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


class ScyllaDockerNode(ScyllaNode):
    def __init__(self, *args, **kwargs):
        kwargs['save'] = False
        super(ScyllaDockerNode, self).__init__(*args, **kwargs)
        self.docker_id = None
        self.local_yaml_path = os.path.join(self.get_path(), 'conf')
        self.docker_name = f'{self.cluster.name}-{self.name}'
        self.jmx_port = 7199

    def _get_directories(self):
        dirs = {}
        for i in ['data', 'commitlogs', 'conf', 'logs', 'hints', 'view_hints']:
            dirs[i] = os.path.join(self.get_path(), i)
        return dirs

    @staticmethod
    def get_docker_name():
        return run(["docker", "ps", "-a"], stdout=PIPE).stdout.decode('utf-8').split()[-1]

    def update_yaml(self):
        if not os.path.exists(f'{self.local_yaml_path}/scylla.yaml'):
            run(['bash', '-c', f'docker run --rm --entrypoint cat {self.cluster.docker_image} /etc/scylla/scylla.yaml > {self.local_yaml_path}/scylla.yaml'])
        super(ScyllaDockerNode, self).update_yaml()

        conf_file = os.path.join(self.get_conf_dir(), common.SCYLLA_CONF)
        with open(conf_file, 'r') as f:
            data = yaml.safe_load(f)
        # HACK: to enable correct order of seed, if we are the first, we set it as seed
        seed_address, _ = self.cluster.nodelist()[0].network_interfaces['thrift']
        data['seed_provider'][0]['parameters'][0]['seeds'] = [seed_address]

        data['api_address'] = '127.0.0.1'
        with open(conf_file, 'w') as f:
            yaml.safe_dump(data, f, default_flow_style=False)

    def create_docker(self):
        # TODO: handle smp correctly via the correct param/api (or only via commandline params)
        # TODO: mount of the data dir
        # TODO: pass down the full command line params, since the docker ones doesn't support all of them ?
        # TODO: pass down a unique tag, with the cluster name, or id, if we have such in ccm, like test_id in SCT ?
        node1 = self.cluster.nodelist()[0]
        if not self.name == node1.name:
            seeds = f"--seeds {node1.network_interfaces['thrift'][0]}"
        else:
            seeds = ''
        res = run(['bash', '-c', f"docker run -v {self.local_yaml_path}/scylla.yaml:/etc/scylla/scylla.yaml:ro --name {self.docker_name} -d {self.cluster.docker_image} --smp 1 {seeds}"], stdout=PIPE, stderr=PIPE)
        self.pid = res.stdout.decode('utf-8').strip() if res.stdout else None
        self.log_thread = DockerLogger(self,  os.path.join(self.get_path(), 'logs', 'system.log'))
        self.log_thread.start()

        # replace addresses
        network = run(['bash', '-c', f"docker inspect --format='{{{{ .NetworkSettings.IPAddress }}}}' {self.pid}"], stdout=PIPE, stderr=PIPE)
        address = network.stdout.decode('utf-8').strip() if res.stdout else None
        self.network_interfaces = {k: (address, v[1]) for k, v in self.network_interfaces.items()}

        return res.returncode == 0, res.stdout, res.stderr

    def service_start(self, service_name):
        res = run(['bash', '-c', f'docker exec {self.pid} /bin/bash -c "supervisorctl start {service_name}"'],
                  stdout=PIPE, stderr=PIPE)
        if res.returncode != 0:
            print(res.stdout)
            print(f'service {service_name} failed to start with error\n{res.stderr}')

    def service_stop(self, service_name):
        res = run(['bash', '-c', f'docker exec {self.pid} /bin/bash -c "supervisorctl stop {service_name}"'],
                  stdout=PIPE, stderr=PIPE)
        if res.returncode != 0:
            print(res.stdout)
            print(f'service {service_name} failed to stop with error\n{res.stderr}')

    def service_status(self, service_name):
        res = run(['bash', '-c', f'docker exec {self.pid} /bin/bash -c "supervisorctl status {service_name}"'],
                  stdout=PIPE, stderr=PIPE)
        if res.returncode != 0:
            print(res.stdout)
            print(f'service {service_name} failed to stop with error\n{res.stderr}')
            return "DOWN"
        else:
            return res.stdout.decode('utf-8').split()[1]

    # def start(self, *args, **kwargs):
    #    return super(ScyllaDockerNode, self).start(*args, **kwargs)

    def _start_scylla(self, args, marks, update_pid, wait_other_notice,
                      wait_for_binary_proto, ext_env):
        rc, out, err = self.create_docker()
        if not rc:
            raise BaseException(f'failed to create docker {self.docker_name}')

        time.sleep(5)
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
            raise NotImplementedError()

    def clear(self, *args, **kwargs):
        res = run(['bash', '-c', f'docker rm -f {self.pid}'], stdout=PIPE, stderr=PIPE)
        self.log_thread.stop()
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

    def _wait_java_up(self, ip_addr, jmx_port):
        return True

    def _update_pid(self, process):
        pass

    def get_tool(self, toolname):
        return ['docker',  'exec', '-i',  f'{self.pid}', f'{toolname}']

    def get_env(self):
        return os.environ.copy()


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
