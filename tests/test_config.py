import distutils.util
import os

DEV_MODE = bool(distutils.util.strtobool(os.environ.get("DEV_MODE", "False")))
RESULTS_DIR = "test_results"
TEST_ID = os.environ.get("CCM_TEST_ID", None)
SCYLLA_DOCKER_IMAGE = os.environ.get(
    "SCYLLA_DOCKER_IMAGE", "scylladb/scylla-nightly:666.development-0.20201015.8068272b466")
