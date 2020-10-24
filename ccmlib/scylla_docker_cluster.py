import os
import random
from subprocess import run, PIPE

from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_node import ScyllaNode


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

    def _get_directories(self):
        dirs = {}
        for i in ['data', 'commitlogs', 'conf', 'logs', 'hints', 'view_hints']:
            dirs[i] = os.path.join(self.get_path(), i)
        return dirs

    @staticmethod
    def get_docker_name():
        return run(["docker", "ps", "-a"], stdout=PIPE).stdout.decode('utf-8').split()[-1]

    def update_yaml(self):
        pass  # TODO: handle as mount point ?

    def create_docker(self):
        res = run(['bash', '-c', f"docker run -v {self.local_yaml_path}:/etc/scylla/ --name {self.docker_name} -d {self.cluster.docker_image} --smp 1"], stdout=PIPE, stderr=PIPE)
        self.docker_id = res.stdout.decode('utf-8').strip() if res.stdout else None
        return res.returncode == 0, res.stdout, res.stderr

    def service_start(self, service_name):
        res = run(['docker', 'exec', self.docker_name, 'supervisorctl', 'start', service_name],
                  stdout=PIPE, stderr=PIPE)
        if res.returncode != 0:
            print(res.stdout)
            print(f'service {service_name} failed to start with error\n{res.stderr}')

    def service_stop(self, service_name):
        res = run(['docker', 'exec', self.docker_name, 'supervisorctl', 'stop', service_name],
                  stdout=PIPE, stderr=PIPE)
        if res.returncode != 0:
            print(res.stdout)
            print(f'service {service_name} failed to stop with error\n{res.stderr}')

    def service_status(self, service_name):
        res = run(['docker', 'exec', self.docker_name, 'supervisorctl', 'status', service_name],
                  stdout=PIPE, stderr=PIPE)
        if res.returncode != 0:
            print(res.stdout)
            print(f'service {service_name} failed to stop with error\n{res.stderr}')
        else:
            return res.stdout.decode('utf-8').split()[1]

    def start(self, *args, **kwargs):
        # TODO: find a better place to do this trick, at initializing maybe ? only if file doesn't exist in the test cluster dir yet ? as part as `update_yaml` ?
        # get scylla.yaml out of the docker
        run(['bash', '-c', f'docker run --rm --entrypoint cat {self.cluster.docker_image}  /etc/scylla/scylla.yaml > {self.local_yaml_path}/scylla.yaml'])
        # res = run([f'docker run --rm --entrypoint cat {self.cluster.docker_image}  '
        #            f'/etc/scylla/scylla.yaml > {self.local_yaml_path}'],
        #           stdout=PIPE, stderr=PIPE)
        # if not res.returncode:
        #     print(f'Failed to copy scylla.yaml to {self.local_yaml_path}\n{res.stderr}')
        rc, out, err = self.create_docker()
        if not rc:
            raise BaseException(f'failed to create docker {self.docker_name}')
        super(ScyllaDockerNode, self).start(*args, **kwargs)
        return random.randint(100, 10000)

    def _start_scylla(self, args, marks, update_pid, wait_other_notice,
                      wait_for_binary_proto, ext_env):
        # TODO: handle smp correctly via the correct param/api (or only via commandline params)
        # TODO: mount of the data dir
        # TODO: pass down the full command line params, since the docker ones doesn't support all of them ?
        # TODO: pass down a unique tag, with the cluster name, or id, if we have such in ccm, like test_id in SCT ?
        scylla_status = self.service_status('scylla')
        if scylla_status and scylla_status.upper() != 'RUNNING':
            self.service_start('scylla')
        return random.randint(100, 10000)

    def _start_jmx(self, data):
        jmx_status = self.service_status('scylla-jmx')
        if not jmx_status and jmx_status.upper() == 'RUNNING':
            self.service_start('scylla-jmx')
