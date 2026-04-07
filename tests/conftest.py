import json
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from subprocess import DEVNULL, PIPE, run

import pytest
from tests.test_config import RESULTS_DIR, TEST_ID, SCYLLA_DOCKER_IMAGE, SCYLLA_RELOCATABLE_VERSION, SCYLLA_PODMAN_IMAGE

from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_docker_cluster import ScyllaDockerCluster
from ccmlib.scylla_podman_cluster import ScyllaPodmanCluster
from .ccmcluster import CCMCluster


LOGGER = logging.getLogger(__name__)


def _pid_is_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _resource_owner_pid(labels):
    if not isinstance(labels, dict):
        return None
    owner_pid = labels.get("org.scylladb.ccm-owner-pid")
    if owner_pid is None:
        return None
    try:
        return int(owner_pid)
    except (TypeError, ValueError):
        return None


def _prune_stale_ccm_podman_resources():
    """Remove any leftover CCM podman containers and networks from previous test runs.

    Only resources owned by a dead pytest session are removed. This avoids
    tearing down a concurrently running CCM podman session on the same host.
    """
    CCM_CONTAINER_PREFIX = "ccm-"
    CCM_NETWORK_PREFIX = "ccm-"

    stale_container_names = []
    try:
        res = run(
            ["podman", "ps", "-a", "--format", "json"],
            stdout=PIPE,
            stderr=PIPE,
            text=True,
        )
        if res.returncode == 0 and res.stdout.strip():
            containers = json.loads(res.stdout)
            for c in containers:
                names = c.get("Names", [])
                name = names[0] if names else c.get("Name", "")
                state = c.get("State", "")
                owner_pid = _resource_owner_pid(c.get("Labels", {}))
                if (
                    name.startswith(CCM_CONTAINER_PREFIX)
                    and owner_pid is not None
                    and not _pid_is_alive(owner_pid)
                ):
                    LOGGER.info(
                        "Pruning stale CCM container: %s (state=%s)", name, state
                    )
                    rm_res = run(["podman", "rm", "-f", name], stdout=DEVNULL, stderr=DEVNULL)
                    if rm_res.returncode == 0:
                        stale_container_names.append(name)
    except Exception:
        LOGGER.debug("Failed to prune stale CCM containers", exc_info=True)

    stale_container_names = set(stale_container_names)
    try:
        res = run(
            ["podman", "network", "ls", "--format", "json"],
            stdout=PIPE,
            stderr=PIPE,
            text=True,
        )
        if res.returncode == 0 and res.stdout.strip():
            networks = json.loads(res.stdout)
            for net in networks:
                name = net.get("name", "")
                owner_pid = _resource_owner_pid(net.get("labels", {}))
                if not name.startswith(CCM_NETWORK_PREFIX):
                    continue
                if owner_pid is None or _pid_is_alive(owner_pid):
                    continue
                inspect_res = run(
                    ["podman", "network", "inspect", name],
                    stdout=PIPE,
                    stderr=PIPE,
                    text=True,
                )
                if inspect_res.returncode != 0 or not inspect_res.stdout.strip():
                    continue
                inspect_data = json.loads(inspect_res.stdout)
                containers_on_net = (
                    inspect_data[0].get("containers", {}) if inspect_data else {}
                )
                attached_names = {
                    details.get("name")
                    for details in containers_on_net.values()
                    if isinstance(details, dict) and details.get("name")
                }
                if attached_names - stale_container_names:
                    continue
                LOGGER.info("Pruning stale CCM network: %s", name)
                run(
                    ["podman", "network", "rm", "-f", name],
                    stdout=DEVNULL,
                    stderr=DEVNULL,
                )
    except Exception:
        LOGGER.debug("Failed to prune stale CCM networks", exc_info=True)


@pytest.fixture(scope="session", autouse=True)
def prune_stale_podman_resources():
    """Auto-use fixture: prune stale CCM podman containers/networks before and after the session."""
    if not shutil.which("podman"):
        yield
        return
    _prune_stale_ccm_podman_resources()
    yield
    _prune_stale_ccm_podman_resources()


@pytest.fixture(scope="session")
def results_dir():
    LOGGER.info("Creating test directory...")
    dir_name = Path(__file__).parent / Path(RESULTS_DIR)
    dir_name.mkdir(parents=True, exist_ok=True)
    return dir_name


@pytest.fixture(scope="session")
def test_id():
    ccm_test_id = TEST_ID or datetime.now().strftime("%Y%m%d-%H%M%S")
    LOGGER.info(f"Using test id: '{ccm_test_id}'")
    return ccm_test_id


@pytest.fixture(scope="session")
def test_dir(test_id, results_dir):
    max_test_dirs, dir_count = 100, 1
    test_dir = results_dir / Path("ccm-" + test_id)
    while test_dir.exists() and dir_count <= max_test_dirs:
        test_dir = results_dir / Path(f"ccm-{test_id}-{dir_count}")
        dir_count += 1

    if dir_count >= max_test_dirs:
        LOGGER.critical(f"Number of test directories is '{dir_count}'. Max allowed: '{max_test_dirs}'")
        assert dir_count < max_test_dirs
    test_dir = os.getcwd() / test_dir
    test_dir.mkdir()
    LOGGER.info(f"Test directory '{test_dir}' created.")
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
def relocatable_cluster(test_dir, test_id):
    cluster_name = f"relocatable_cluster_{test_id}"
    cluster = ScyllaCluster(str(test_dir), name=cluster_name, version=SCYLLA_RELOCATABLE_VERSION)
    timeout = 10000
    cluster.set_configuration_options(values={
        'read_request_timeout_in_ms': timeout,
        'range_request_timeout_in_ms': timeout,
        'write_request_timeout_in_ms': timeout,
        'truncate_request_timeout_in_ms': timeout,
        'request_timeout_in_ms': timeout,
        'skip_wait_for_gossip_to_settle': 0,
        'ring_delay_ms': 0,
    })
    cluster.populate(1)
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
    cluster = CCMCluster(use_scylla=False, test_id="cassandra")
    return cluster


@pytest.fixture(scope="session")
def ccm_reloc_latest_cluster():
    cluster = CCMCluster(test_id="reloc_master", relocatable_version="unstable/master:latest")
    return cluster


@pytest.fixture(scope="session")
def cluster_under_test(request):
    cluster = request.getfixturevalue(request.param)
    return cluster


@pytest.fixture(scope="session")
def podman_cluster(test_dir, test_id):
    if not shutil.which("podman"):
        pytest.skip("podman binary not found")
    cluster_name = f"podman_cluster_{test_id}"
    cluster = None
    try:
        cluster = ScyllaPodmanCluster(
            str(test_dir),
            name=cluster_name,
            podman_image=SCYLLA_PODMAN_IMAGE,
            inter_dc_delay_ms=40,
            inter_rack_delay_ms=1,
        )
        cluster.populate({"dc1": {"rack1": 1, "rack2": 1}, "dc2": {"rack1": 1}})
        cluster.set_configuration_options(
            values={
                "read_request_timeout_in_ms": 10000,
                "range_request_timeout_in_ms": 10000,
                "write_request_timeout_in_ms": 10000,
                "truncate_request_timeout_in_ms": 10000,
                "request_timeout_in_ms": 10000,
            }
        )
        cluster.start(wait_for_binary_proto=True)
        yield cluster
    finally:
        if cluster is not None:
            cluster.remove()
