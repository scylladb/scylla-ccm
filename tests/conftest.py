import logging
import os
from datetime import datetime
from pathlib import Path

import pytest
from tests.test_config import RESULTS_DIR, TEST_ID, SCYLLA_DOCKER_IMAGE

from ccmlib.scylla_docker_cluster import ScyllaDockerCluster

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def results_dir():
    LOGGER.info("Creating test directory...")
    dir_name = "tests" / Path(RESULTS_DIR)
    dir_name.mkdir(exist_ok=True)
    return dir_name


@pytest.fixture(scope="session")
def test_id():
    ccm_test_id = TEST_ID or datetime.now().strftime("%Y%m%d-%H%M%S")
    LOGGER.info("Using test id: '{}'".format(ccm_test_id))
    return ccm_test_id


@pytest.fixture(scope="session")
def test_dir(test_id, results_dir):
    max_test_dirs, dir_count = 1, 100
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
    cluster.populate(3)
    cluster.start(wait_for_binary_proto=True)
    try:
        yield cluster
    finally:
        cluster.clear()
