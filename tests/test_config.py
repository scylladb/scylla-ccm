import os


RESULTS_DIR = "test_results"
TEST_ID = os.environ.get("CCM_TEST_ID", None)
SCYLLA_DOCKER_IMAGE = os.environ.get(
    "SCYLLA_DOCKER_IMAGE", "scylladb/scylla:latest")
SCYLLA_RELOCATABLE_VERSION = "release:2024.2.3"

# Feb/8 comment to refresh the action cache
