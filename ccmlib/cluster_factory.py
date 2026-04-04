import logging
import os

from ruamel.yaml import YAML

from ccmlib import common, repository
from ccmlib.cluster import Cluster
from ccmlib.dse_cluster import DseCluster
from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_docker_cluster import ScyllaDockerCluster
from ccmlib.scylla_podman_cluster import ScyllaPodmanCluster, PodmanNetworkTopology
from ccmlib.node import Node


class ClusterFactory():

    @staticmethod
    def load(path, name):
        cluster_path = os.path.join(path, name)
        filename = os.path.join(cluster_path, 'cluster.conf')
        with open(filename, 'r') as f:
            data = YAML().load(f)
        try:
            install_dir = None
            scylla_manager_install_path = data.get('scylla_manager_install_path')
            if 'install_dir' in data and 'docker_image' not in data:
                install_dir = data['install_dir']
                repository.validate(install_dir)
            if install_dir is None and 'cassandra_dir' in data:
                install_dir = data['cassandra_dir']
                repository.validate(install_dir)
            if 'network_topology' in data:
                net_topo_data = data['network_topology']
                cluster = ScyllaPodmanCluster(
                    path, data['name'],
                    docker_image=data.get('docker_image'),
                    inter_rack_delay_ms=net_topo_data.get('inter_rack_delay_ms', 1),
                    inter_dc_delay_ms=net_topo_data.get('inter_dc_delay_ms', 50),
                    packet_loss_percent=net_topo_data.get('packet_loss_percent', 0.0),
                    pinning=data.get('pinning', False),
                    create_directory=False,
                )
                cluster.network_topology = PodmanNetworkTopology.from_dict(data['name'], net_topo_data)
            elif 'docker_image' in data and data['docker_image']:
                cluster = ScyllaDockerCluster(path, data['name'], docker_image=data['docker_image'],
                                              install_dir=install_dir, create_directory=False)
            elif common.isScylla(install_dir):
                cluster = ScyllaCluster(path, data['name'], install_dir=install_dir, create_directory=False,
                                        manager=scylla_manager_install_path, cassandra_version=data.get('scylla_version', None))
                # Restore scylla_mode if it was saved (for non-relocatable builds)
                if 'scylla_mode' in data:
                    cluster.scylla_mode = data['scylla_mode']
            elif common.isDse(install_dir):
                cluster = DseCluster(path, data['name'], install_dir=install_dir, create_directory=False)
            else:
                cluster = Cluster(path, data['name'], install_dir=install_dir, create_directory=False)
            node_list = data['nodes']
            seed_list = data['seeds']
            if 'partitioner' in data:
                cluster.partitioner = data['partitioner']
            if 'config_options' in data:
                cluster._config_options = data['config_options']
            if 'log_level' in data:
                cluster._Cluster__log_level = data['log_level']
            if 'use_vnodes' in data:
                cluster.use_vnodes = data['use_vnodes']
            if 'ipprefix' in data:
                cluster.ipprefix = data['ipprefix']


        except KeyError as k:
            raise common.LoadError("Error Loading " + filename + ", missing property:" + str(k))

        for node_name in node_list:
            cluster.nodes[node_name] = Node.load(cluster_path, node_name, cluster)
        for seed_name in seed_list:
            seed_node = cluster.nodes.get(seed_name)
            if seed_node:
                cluster.seeds.append(seed_node)
            else:
                logging.warning("Seed node %s not found in cluster %s" % (seed_name, cluster.name))


        return cluster
