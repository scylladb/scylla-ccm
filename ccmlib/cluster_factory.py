import logging
import os

from ruamel.yaml import YAML

from ccmlib import common, repository
from ccmlib.cluster import Cluster
from ccmlib.dse_cluster import DseCluster
from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_docker_cluster import ScyllaDockerCluster
from ccmlib import repository
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
            if 'docker_image' in data and data['docker_image']:
                cluster = ScyllaDockerCluster(path, data['name'], docker_image=data['docker_image'],
                                              install_dir=install_dir, create_directory=False)
            elif common.isScylla(install_dir):
                cluster = ScyllaCluster(path, data['name'], install_dir=install_dir, create_directory=False,
                                        manager=scylla_manager_install_path, cassandra_version=data.get('scylla_version', None))
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
                cluster.__log_level = data['log_level']
            if 'use_vnodes' in data:
                cluster.use_vnodes = data['use_vnodes']
            if 'sni_proxy_docker_ids' in data and data['sni_proxy_docker_ids']:
                cluster.sni_proxy_docker_ids = data['sni_proxy_docker_ids']
            if 'sni_proxy_listen_port' in data and data['sni_proxy_listen_port']:
                cluster.sni_proxy_listen_port = data['sni_proxy_listen_port']
            if 'sni_generate_ssl_automatic' in data and data['sni_generate_ssl_automatic']:
                cluster.sni_generate_ssl_automatic = data['sni_generate_ssl_automatic']


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
