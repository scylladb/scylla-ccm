import time
import subprocess

import pytest

from ccmlib.common import get_scylla_full_version, get_scylla_version
from ccmlib.node import Node


@pytest.mark.reloc
class TestScyllaRelocatableCluster:
    def test_get_scylla_full_version(self, relocatable_cluster):
        install_dir = relocatable_cluster.get_install_dir()
        assert get_scylla_full_version(install_dir) == '3.0-0.20200829.f1255cb2d02'

    def test_get_scylla_version(self, relocatable_cluster):
        install_dir = relocatable_cluster.get_install_dir()
        assert get_scylla_version(install_dir) == '3.0'

    def test_nodetool_timeout(self, relocatable_cluster):
        node1: Node = relocatable_cluster.nodelist()[0]
        node1._update_jmx_pid(wait=True)
        with pytest.raises(subprocess.TimeoutExpired):
            node1.nodetool("cfstats", timeout=0.0001)
        with pytest.raises(subprocess.TimeoutExpired):
            node1.nodetool("cfstats", capture_output=True, timeout=0.0001)
        time.sleep(5)
