import os
from functools import lru_cache

from ccmlib.utils.scylla_versions import get_latest_scylla_release


RESULTS_DIR = "test_results"
TEST_ID = os.environ.get("CCM_TEST_ID", None)
SCYLLA_DOCKER_IMAGE = os.environ.get(
    "SCYLLA_DOCKER_IMAGE", "scylladb/scylla:latest")


@lru_cache(maxsize=None)
def get_scylla_relocatable_version():
    """
    the relocatable version tests run against: $SCYLLA_VERSION if set (CI sets it),
    otherwise the latest scylla release, so we never test on a version that isn't supported anymore
    """
    return os.environ.get("SCYLLA_VERSION") or f"release:{get_latest_scylla_release()}"

# Feb/8 comment to refresh the action cache
