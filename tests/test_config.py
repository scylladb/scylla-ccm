import distutils.util
import os

DEV_MODE = bool(distutils.util.strtobool(os.environ.get("DEV_MODE", "False")))
RESULTS_DIR = "test_results"
TEST_ID = os.environ.get("CCM_TEST_ID", None)
SCYLLA_DOCKER_IMAGE = os.environ.get(
    "SCYLLA_DOCKER_IMAGE", "scylladb/scylla-nightly:666.development-0.20201015.8068272b466")
SCYLLA_RELOCATABLE_VERSION = os.environ.get(
    "SCYLLA_VERSION", "unstable/master:2023-04-13T13:31:00Z")

# Feb/8 comment to refresh the action cache
