import logging
import os
import subprocess

from ruamel.yaml import YAML
from ccmlib import common
from ccmlib.cluster_factory import ClusterFactory

LOGGER = logging.getLogger(__name__)


class CCMCluster:

    def __init__(self, test_id, use_scylla=True, relocatable_version=None, docker_image=None, scylla_manager_package=None):

        if scylla_manager_package:
            os.environ['SCYLLA_MANAGER_PACKAGE'] = ''
        else:
            try:
                del os.environ['SCYLLA_MANAGER_PACKAGE']
            except Exception:
                pass

        self.name = f"{self.__class__.__name__}-{test_id}"
        self.ccm_bin = os.path.join(os.curdir, "ccm")
        self.cluster_dir = os.path.join(common.get_default_path(), self.name)
        self.use_scylla = use_scylla
        self.relocatable_version = relocatable_version
        self.docker_image = docker_image
        self._process = None

    def get_create_cmd(self, args=None):
        cmd_args = [self.ccm_bin, 'create', self.name]
        if self.use_scylla and self.relocatable_version:
            cmd_args += ["--scylla", "-v", self.relocatable_version]
        elif self.use_scylla and self.docker_image:
            cmd_args += ["--scylla", "--docker", self.docker_image]
        elif not self.use_scylla and self.docker_image:
            cmd_args = ["--docker", self.docker_image]
        else:
            cmd_args += ["-v", "3.11.4"]

        if args:
            cmd_args += args

        return cmd_args

    def get_ccm_cluster(self):
        return ClusterFactory.load(common.get_default_path(), self.name)

    def get_populate_cmd(self, *args):
        return [self.ccm_bin, "populate", self.name, *args]

    def get_remove_cmd(self):
        return [self.ccm_bin, "remove", self.name]

    def get_stop_cmd(self):
        return [self.ccm_bin, "stop", self.name]

    def get_list_cmd(self):
        return [self.ccm_bin, "list"]

    def get_status_cmd(self):
        return [self.ccm_bin, "status"]

    def get_start_cmd(self, *args):
        return [self.ccm_bin, "start", "--wait-for-binary-proto", *args]

    def get_start_sni_proxy_cmd(self):
        return [self.ccm_bin, "start", "--sni-proxy", "--sni-port", "8443", "--wait-for-binary-proto"]

    def get_add_cmd(self, node_name, *args):
        cmd_args = args
        if self.use_scylla:
            cmd_args += ("--scylla", )
        return [self.ccm_bin, "add", "-b", node_name, *cmd_args]

    def get_node_start_cmd(self, node_name, *args):
        return [self.ccm_bin, node_name, "start", *args]

    def get_cluster_config(self):
        with open(os.path.join(self.cluster_dir, "cluster.conf")) as f:
            return YAML(typ='rt').load(f)

    def get_nodes_cassandra_rackdc_properties(self, node_name):
        rackdc_file_name = os.path.join(self.cluster_dir, node_name, "conf", "cassandra-rackdc.properties")
        if not os.path.exists(rackdc_file_name):
            return []
        with open(rackdc_file_name) as f:
            res = {}
            for line in f.readlines():
                if line.startswith("#"):
                    continue
                chunks = line.strip().split("=", 2)
                if len(chunks) == 2:
                    key, value = chunks
                    res[key.strip()] = value.strip()
            return res

    def get_updateconf_cmd(self):
        return [self.ccm_bin, "updateconf",
                'read_request_timeout_in_ms:10000',
                'range_request_timeout_in_ms:10000',
                'write_request_timeout_in_ms:10000',
                'truncate_request_timeout_in_ms:10000',
                'request_timeout_in_ms:10000'
        ]

    def run_command(self, cmd):
        LOGGER.info(cmd)
        self.process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        return self.process

    def validate_command_result(self, expected_status_code=0):
        stdout, stderr = self.process.communicate(timeout=600)
        status_code = self.process.wait()
        try:
            stdout = stdout.strip()
            stderr = stderr.strip()

            LOGGER.debug(f"[stdout] {stdout}")
            LOGGER.debug(f"[stderr] {stderr}")
            assert status_code == expected_status_code
            return stdout, stderr
        except AssertionError:
            LOGGER.error(f"[ERROR] {stderr.strip()}")
            raise

    def parse_cluster_status(self, stdout):
        """
            Output:

            Cluster: 'CCMCluster-reloc'
            ---------------------------
            node1: UP

            return:
            [(node1, UP)]
        """
        nodes_status = []
        for line in stdout.split("\n")[2:]:
            chunks = line.split(":")
            if len(chunks) != 2:
                continue
            node, status = chunks
            nodes_status.append((node.strip(), status.strip()))

        return nodes_status

    def get_nodetool_cmd(self, node, subcmd, args=None):
        cmd = [self.ccm_bin, node, 'nodetool', subcmd]
        if args:
            cmd += args
        return cmd

    def nodelist(self):
        self.run_command(self.get_status_cmd())
        stdout, _ = self.validate_command_result()
        return [node for node, _ in self.parse_cluster_status(stdout)]
