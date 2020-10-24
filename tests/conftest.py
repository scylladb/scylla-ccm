import logging
from datetime import datetime
from pathlib import Path

import pytest

from ccmlib.scylla_docker_cluster import ScyllaDockerCluster
from tests.test_config import RESULTS_DIR, TEST_ID, SCYLLA_DOCKER_IMAGE

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
        test_dir = results_dir / Path("ccm-%s-%s" % (test_id, dir_count))
        dir_count += 1

    if dir_count >= max_test_dirs:
        LOGGER.critical(f"Number of test directories is '%s'. Max allowed: '%s'", dir_count, max_test_dirs)
        assert dir_count >= max_test_dirs
    test_dir.mkdir()
    LOGGER.info("Testdir '%s' created.", test_dir)
    return test_dir


@pytest.fixture(scope="session")
def regular_cluster(test_dir, test_id):
    cluster_name = f"regular_cluster_{test_id}"
    cluster = ScyllaDockerCluster(path=str(test_dir), name=cluster_name, docker_image=SCYLLA_DOCKER_IMAGE)
    cluster.populate(1)
    cluster.start(no_wait=True)
    yield cluster
