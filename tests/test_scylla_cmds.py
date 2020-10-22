import os
import logging
import time

import pytest

from ccmlib import common
# from .test_config import SCYLLA_DOCKER_IMAGE, SCYLLA_RELOCATABLE_VERSION
from .test_scylla_docker_cluster import TestScyllaDockerCluster

LOGGER = logging.getLogger(__file__)

cluster_params = pytest.mark.parametrize(
    'cluster_under_test',
    (pytest.param('ccm_docker_cluster', marks=pytest.mark.docker),
     pytest.param('ccm_reloc_cluster', marks=pytest.mark.reloc)),
    indirect=True
)


@cluster_params
class TestCCMCreateCluster:
    cluster = None

    @classmethod
    @pytest.fixture(autouse=True)
    def base_setup(cls, cluster_under_test):
        setattr(TestCCMClusterStatus, "cluster", cluster_under_test)
        try:
            yield
        finally:
            cls.cluster.run_command(cls.cluster.get_remove_cmd())
            cls.cluster.process.wait()
            if os.path.exists(cls.cluster.cluster_dir):
                common.rmdirs(cls.cluster.cluster_dir)

    def validate_cluster_dir(self):
        assert os.path.exists(self.cluster.cluster_dir)

    def test_create_cluster(self):
        create_cmd = self.cluster.get_create_cmd()
        self.cluster.run_command(create_cmd)
        self.cluster.validate_command_result()
        self.validate_cluster_dir()

    def test_create_cluster_with_nodes(self):
        create_cmd = self.cluster.get_create_cmd(args=['-n', '1'])
        self.cluster.run_command(create_cmd)
        self.cluster.validate_command_result()
        self.validate_cluster_dir()


@cluster_params
class TestCCMClusterStatus:

    @classmethod
    @pytest.fixture(scope="class", autouse=True)
    def base_setup(cls, cluster_under_test):
        setattr(TestCCMClusterStatus, "cluster", cluster_under_test)
        try:
            cls.cluster.run_command(cls.cluster.get_create_cmd(args=['-n', '1']))
            cls.cluster.validate_command_result()
            yield
        finally:
            cls.cluster.run_command(cls.cluster.get_remove_cmd())
            cls.cluster.process.wait()
            if os.path.exists(cls.cluster.cluster_dir):
                common.rmdirs(cls.cluster.cluster_dir)

    def test_list_of_cluster(self):
        self.cluster.run_command(self.cluster.get_list_cmd())
        stdout, stderr = self.cluster.validate_command_result()
        assert self.cluster.name in stdout.decode().strip()

    def test_status_cluster(self):
        self.cluster.run_command(self.cluster.get_status_cmd())
        stdout, stderr = self.cluster.validate_command_result()
        LOGGER.info(stdout.split())


@cluster_params
class TestCCMClusterStart:

    @classmethod
    @pytest.fixture(autouse=True)
    def base_setup(cls, cluster_under_test):
        setattr(TestCCMClusterStart, "cluster", cluster_under_test)
        try:
            yield
        finally:
            cls.cluster.run_command(cls.cluster.get_remove_cmd())
            cls.cluster.process.wait()
            if os.path.exists(cls.cluster.cluster_dir):
                common.rmdirs(cls.cluster.cluster_dir)

    def test_create_and_start_cluster_without_nodes(self):
        self.cluster.run_command(self.cluster.get_create_cmd())
        self.cluster.validate_command_result()
        self.cluster.run_command(self.cluster.get_start_cmd())
        stdout, stderr = self.cluster.validate_command_result()
        assert "No node in this cluster yet. Use the populate command before starting" in stdout

    def test_create_and_start_cluster_with_nodes(self):
        self.cluster.run_command(self.cluster.get_create_cmd(args=['-n', '1']))
        self.cluster.validate_command_result()
        self.cluster.run_command(self.cluster.get_start_cmd())
        self.cluster.validate_command_result()
        self.cluster.run_command(self.cluster.get_status_cmd())
        stdout, stderr = self.cluster.validate_command_result()
        for node, status in self.cluster.parse_cluster_status(stdout):
            LOGGER.info("%s: %s", node, status)
            assert "UP" in status, f"{node} was not started and have status {status}"


@cluster_params
class TestCCMClusterNodetool:

    @classmethod
    @pytest.fixture(scope="class", autouse=True)
    def base_setup_with_2_nodes(cls, cluster_under_test):
        setattr(TestCCMClusterNodetool, "cluster", cluster_under_test)
        try:
            cls.cluster.run_command(cls.cluster.get_create_cmd(args=['-n', '2']))
            cls.cluster.validate_command_result()
            cls.cluster.run_command(cls.cluster.get_start_cmd())
            cls.cluster.validate_command_result()
            time.sleep(30)
            yield
        finally:
            cls.cluster.run_command(cls.cluster.get_remove_cmd())
            cls.cluster.process.wait()
            if os.path.exists(cls.cluster.cluster_dir):
                common.rmdirs(cls.cluster.cluster_dir)

    def test_ccm_status(self):
        self.cluster.run_command(self.cluster.get_status_cmd())
        stdout, stderr = self.cluster.validate_command_result()
        for node, status in self.cluster.parse_cluster_status(stdout):
            LOGGER.info("%s: %s", node, status)
            assert "UP" in status, f"{node} was not started and have status {status}"

    def test_nodetool_status(self):
        for node in self.cluster.nodelist():
            nodetool_cmd = self.cluster.get_nodetool_cmd(node, "status")
            self.cluster.run_command(nodetool_cmd)
            stdout, _ = self.cluster.validate_command_result()
            node_statuses = TestScyllaDockerCluster.parse_nodetool_status(stdout.split("\n"))
            assert node_statuses
            LOGGER.info(node_statuses)
            for node in node_statuses:
                assert node['status'] == 'UN'
