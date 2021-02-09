import os
import logging
import shutil
import subprocess
import getpass
from datetime import datetime

import pytest
from _pytest.fixtures import FixtureRequest
from ccmlib import common
from .test_scylla_docker_cluster import TestScyllaDockerCluster

LOGGER = logging.getLogger(__name__)

cluster_params = pytest.mark.parametrize(
    'cluster_under_test',
    (pytest.param('ccm_docker_cluster', marks=pytest.mark.docker),
     pytest.param('ccm_reloc_cluster', marks=pytest.mark.reloc),
     pytest.param('ccm_cassandra_cluster', marks=pytest.mark.cassandra)
     ),
    indirect=True
)


def copy_cluster_data(request: FixtureRequest):
    cluster_dir = request.getfixturevalue('cluster_under_test').cluster_dir
    test_dir = request.getfixturevalue('test_dir')
    scope_id = f'-{str(datetime.now().strftime("%H%M%S%s"))}' if request.scope == 'class' else ''
    test_name = f'{request.node.name}{scope_id}'
    user = getpass.getuser()
    subprocess.run(["/bin/bash", "-c", f"sudo chown -R {user}:{user} {cluster_dir}"])
    shutil.copytree(cluster_dir, test_dir / test_name)


@cluster_params
class TestCCMCreateCluster:
    @staticmethod
    @pytest.fixture(scope="function", autouse=True)
    def base_setup(request, cluster_under_test):
        try:
            yield
        finally:
            copy_cluster_data(request=request)
            cluster_under_test.run_command(cluster_under_test.get_remove_cmd())
            cluster_under_test.process.wait()
            if os.path.exists(cluster_under_test.cluster_dir):
                common.rmdirs(cluster_under_test.cluster_dir)

    def validate_cluster_dir(self, cluster_under_test):
        assert os.path.exists(cluster_under_test.cluster_dir)

    def test_create_cluster(self, cluster_under_test):
        create_cmd = cluster_under_test.get_create_cmd()
        cluster_under_test.run_command(create_cmd)
        cluster_under_test.validate_command_result()
        self.validate_cluster_dir(cluster_under_test)

    def test_create_cluster_with_nodes(self, cluster_under_test):
        create_cmd = cluster_under_test.get_create_cmd(args=['-n', '1'])
        cluster_under_test.run_command(create_cmd)
        cluster_under_test.validate_command_result()
        self.validate_cluster_dir(cluster_under_test)


@cluster_params
class TestCCMClusterStatus:

    @staticmethod
    @pytest.fixture(scope="class", autouse=True)
    def base_setup(request, cluster_under_test):
        try:
            cluster_under_test.run_command(cluster_under_test.get_create_cmd(args=['-n', '1']))
            cluster_under_test.validate_command_result()
            yield
        finally:
            copy_cluster_data(request)
            cluster_under_test.run_command(cluster_under_test.get_remove_cmd())
            cluster_under_test.process.wait()
            if os.path.exists(cluster_under_test.cluster_dir):
                common.rmdirs(cluster_under_test.cluster_dir)

    def test_list_of_cluster(self, cluster_under_test):
        cluster_under_test.run_command(cluster_under_test.get_list_cmd())
        stdout, stderr = cluster_under_test.validate_command_result()
        assert cluster_under_test.name in stdout.strip()

    def test_status_cluster(self, cluster_under_test):
        cluster_under_test.run_command(cluster_under_test.get_status_cmd())
        stdout, stderr = cluster_under_test.validate_command_result()
        LOGGER.info(stdout.split())


@cluster_params
class TestCCMClusterStart:

    @staticmethod
    @pytest.fixture(autouse=True)
    def base_setup(request, cluster_under_test):
        try:
            yield
        finally:
            copy_cluster_data(request)
            cluster_under_test.run_command(cluster_under_test.get_remove_cmd())
            cluster_under_test.process.wait()
            if os.path.exists(cluster_under_test.cluster_dir):
                common.rmdirs(cluster_under_test.cluster_dir)

    def test_create_and_start_cluster_without_nodes(self, cluster_under_test):
        cluster_under_test.run_command(cluster_under_test.get_create_cmd())
        cluster_under_test.validate_command_result()
        cluster_under_test.run_command(cluster_under_test.get_start_cmd())
        stdout, stderr = cluster_under_test.validate_command_result(expected_status_code=1)
        assert "No node in this cluster yet. Use the populate command before starting" in stdout

    def test_create_and_start_cluster_with_nodes(self, cluster_under_test):
        cluster_under_test.run_command(cluster_under_test.get_create_cmd(args=['-n', '1']))
        cluster_under_test.validate_command_result()

        cluster_under_test.run_command(cluster_under_test.get_updateconf_cmd())
        cluster_under_test.validate_command_result()

        cluster_under_test.run_command(cluster_under_test.get_start_cmd())
        cluster_under_test.validate_command_result()

        cluster_under_test.run_command(cluster_under_test.get_status_cmd())
        stdout, stderr = cluster_under_test.validate_command_result()
        for node, status in cluster_under_test.parse_cluster_status(stdout):
            LOGGER.info("%s: %s", node, status)
            assert "UP" in status, f"{node} was not started and have status {status}"


@cluster_params
class TestCCMClusterNodetool:

    @staticmethod
    @pytest.fixture(scope="class", autouse=True)
    def base_setup_with_2_nodes(request, cluster_under_test):
        try:
            cluster_under_test.run_command(cluster_under_test.get_create_cmd(args=['-n', '2']))
            cluster_under_test.validate_command_result()

            cluster_under_test.run_command(cluster_under_test.get_updateconf_cmd())
            cluster_under_test.validate_command_result()

            cluster_under_test.run_command(cluster_under_test.get_start_cmd())
            cluster_under_test.validate_command_result()
            yield
        finally:
            copy_cluster_data(request)
            cluster_under_test.run_command(cluster_under_test.get_remove_cmd())
            cluster_under_test.process.wait()
            if os.path.exists(cluster_under_test.cluster_dir):
                common.rmdirs(cluster_under_test.cluster_dir)

    def test_ccm_status(self, cluster_under_test):
        cluster_under_test.run_command(cluster_under_test.get_status_cmd())
        stdout, stderr = cluster_under_test.validate_command_result()
        for node, status in cluster_under_test.parse_cluster_status(stdout):
            LOGGER.info("%s: %s", node, status)
            assert "UP" in status, f"{node} was not started and have status {status}"

    def test_nodetool_status(self, cluster_under_test):
        for node in cluster_under_test.nodelist():
            nodetool_cmd = cluster_under_test.get_nodetool_cmd(node, "status")
            cluster_under_test.run_command(nodetool_cmd)
            stdout, _ = cluster_under_test.validate_command_result()
            node_statuses = TestScyllaDockerCluster.parse_nodetool_status(stdout.split("\n"))
            assert node_statuses
            LOGGER.info(node_statuses)
            for node in node_statuses:
                assert node['status'] == 'UN'


@pytest.mark.parametrize(
    'cluster_under_test',
    (pytest.param('ccm_docker_cluster', marks=[pytest.mark.docker, pytest.mark.skipif(True, reason="docker doesn't yet support it")]),
     pytest.param('ccm_reloc_with_manager_cluster', marks=pytest.mark.reloc),
     ),
    indirect=True
)
class TestCCMClusterManagerSctool:
    @staticmethod
    @pytest.fixture(scope="class", autouse=True)
    def base_setup(request, cluster_under_test):
        try:
            cluster_under_test.run_command(cluster_under_test.get_create_cmd(args=['-n', '3',
                '--scylla-manager-package',
                'http://downloads.scylladb.com/manager/rpm/unstable/centos/branch-2.2/latest/scylla-manager/x86_64/']))
            cluster_under_test.validate_command_result()

            cluster_under_test.run_command(cluster_under_test.get_updateconf_cmd())
            cluster_under_test.validate_command_result()

            cluster_under_test.run_command(cluster_under_test.get_start_cmd())
            cluster_under_test.validate_command_result()
            yield
        finally:
            copy_cluster_data(request)
            cluster_under_test.run_command(cluster_under_test.get_remove_cmd())
            cluster_under_test.process.wait()
            if os.path.exists(cluster_under_test.cluster_dir):
                common.rmdirs(cluster_under_test.cluster_dir)

    def test_sctool(self, cluster_under_test):
        cluster_under_test.run_command(['./ccm', 'sctool', 'status'])
        cluster_under_test.validate_command_result()
