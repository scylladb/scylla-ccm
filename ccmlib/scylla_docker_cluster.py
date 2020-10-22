import os
from subprocess import run

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

    def _get_directories(self):
        dirs = {}
        for i in ['data', 'commitlogs', 'conf', 'logs', 'hints', 'view_hints']:
            dirs[i] = os.path.join(self.get_path(), i)
        return dirs

    def update_yaml(self):
        pass # TODO: handle as mount point ?

    def start(self, *args, **kwargs):
        # TODO: find a better place to do this trick, at initializing maybe ? only if file doesn't exist in the test cluster dir yet ? as part as `update_yaml` ?
        # get scylla.yaml out of the docker
        local_yaml_path = os.path.join(self.get_path(), 'conf', 'scylla.yaml')
        run(['bash', '-c',  f'docker run --rm --entrypoint cat {self.cluster.docker_image}  /etc/scylla/scylla.yaml > {local_yaml_path}'])
        super(ScyllaDockerNode, self).start(*args, **kwargs)

    def _start_scylla(self, args, marks, update_pid, wait_other_notice,
                      wait_for_binary_proto, ext_env):
        local_yaml_path = os.path.join(self.get_path(), 'conf', 'scylla.yaml')
        # TODO: handle smp correctly via the correct param/api (or only via commandline params)
        # TODO: mount of the data dir
        # TODO: pass down the full command line params, since the docker ones doesn't support all of them ?
        # TODO: pass down a unique tag, with the cluster name, or id, if we have such in ccm, like test_id in SCT ?
        run(['bash', '-c', f"docker run -v {local_yaml_path}:/etc/scylla/scylla.yaml -d {self.cluster.docker_image} --smp 1"])

        raise NotImplementedError()
