import logging
import os
from datetime import datetime
from pathlib import Path

import pytest
from tests.test_config import RESULTS_DIR, TEST_ID, SCYLLA_DOCKER_IMAGE, SCYLLA_RELOCATABLE_VERSION

from ccmlib.scylla_docker_cluster import ScyllaDockerCluster
from .ccmcluster import CCMCluster


LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def results_dir():
    LOGGER.info("Creating test directory...")
    dir_name = "tests" / Path(RESULTS_DIR)
    dir_name.mkdir(parents=True, exist_ok=True)
    return dir_name


@pytest.fixture(scope="session")
def test_id():
    ccm_test_id = TEST_ID or datetime.now().strftime("%Y%m%d-%H%M%S")
    LOGGER.info("Using test id: '{}'".format(ccm_test_id))
    return ccm_test_id


@pytest.fixture(scope="session")
def test_dir(test_id, results_dir):
    max_test_dirs, dir_count = 100, 1
    test_dir = results_dir / Path("ccm-" + test_id)
    while test_dir.exists() and dir_count <= max_test_dirs:
        test_dir = results_dir / Path("ccm-{}-{}".format(test_id, dir_count))
        dir_count += 1

    if dir_count >= max_test_dirs:
        LOGGER.critical("Number of test directories is '{}'. Max allowed: '{}'".format(dir_count, max_test_dirs))
        assert dir_count >= max_test_dirs
    test_dir = os.getcwd() / test_dir
    test_dir.mkdir()
    LOGGER.info("Test directory '{}' created.".format(test_dir))
    return test_dir


@pytest.fixture(scope="session")
def docker_cluster(test_dir, test_id):
    cluster_name = f"regular_cluster_{test_id}"
    cluster = ScyllaDockerCluster(str(test_dir), name=cluster_name, docker_image=SCYLLA_DOCKER_IMAGE)
    timeout = 10000
    cluster.set_configuration_options(values={
        'read_request_timeout_in_ms': timeout,
        'range_request_timeout_in_ms': timeout,
        'write_request_timeout_in_ms': timeout,
        'truncate_request_timeout_in_ms': timeout,
        'request_timeout_in_ms': timeout
    })
    cluster.populate(3)
    cluster.start(wait_for_binary_proto=True)
    try:
        yield cluster
    finally:
        cluster.clear()


@pytest.fixture(scope="session")
def ccm_docker_cluster():
    cluster = CCMCluster(test_id="docker", docker_image=SCYLLA_DOCKER_IMAGE)
    return cluster


@pytest.fixture(scope="session")
def ccm_reloc_cluster():
    cluster = CCMCluster(test_id="reloc", relocatable_version=SCYLLA_RELOCATABLE_VERSION)
    return cluster


@pytest.fixture(scope="session")
def ccm_reloc_with_manager_cluster():
    cluster = CCMCluster(test_id="reloc_manager", relocatable_version=SCYLLA_RELOCATABLE_VERSION)
    return cluster


@pytest.fixture(scope="session")
def ccm_cassandra_cluster():
    cluster = CCMCluster(test_id="cassandra")
    return cluster


@pytest.fixture(scope="session")
def cluster_under_test(request):
    cluster = request.getfixturevalue(request.param)
    return cluster
