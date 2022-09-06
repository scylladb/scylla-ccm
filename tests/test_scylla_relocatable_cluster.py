import time
import subprocess

import pytest

from ccmlib.common import get_scylla_full_version, get_scylla_version
from ccmlib.node import Node, ToolError


@pytest.mark.reloc
class TestScyllaRelocatableCluster:
    def test_get_scylla_full_version(self, relocatable_cluster):
        install_dir = relocatable_cluster.get_install_dir()
        assert get_scylla_full_version(install_dir) == '4.7.dev-0.20211118.4b1bb26d5'

    def test_get_scylla_version(self, relocatable_cluster):
        install_dir = relocatable_cluster.get_install_dir()
        assert get_scylla_version(install_dir) == '4.7.dev'

    def test_nodetool_timeout(self, relocatable_cluster):
        node1: Node = relocatable_cluster.nodelist()[0]
        node1._update_jmx_pid(wait=True)
        with pytest.raises(subprocess.TimeoutExpired):
            node1.nodetool("cfstats", timeout=0.0001)
        with pytest.raises(subprocess.TimeoutExpired):
            node1.nodetool("cfstats", capture_output=True, timeout=0.0001)
        time.sleep(5)

    def test_node_stress(self, relocatable_cluster):
        node1, *_ = relocatable_cluster.nodelist()
        node1: Node
        ret = node1.stress(['write', 'n=10'])
        assert '10 [WRITE: 10]' in ret.stdout
        assert 'END' in ret.stdout

        ret = node1.stress_object(['write', 'n=10'])
        assert list(ret.keys()) == ['op rate:write',
                                    'partition rate:write',
                                    'row rate:write',
                                    'latency mean:write',
                                    'latency median:write',
                                    'latency 95th percentile:write',
                                    'latency 99th percentile:write',
                                    'latency 99.9th percentile:write',
                                    'latency max:write', 'total partitions',
                                    'total partitions:write', 'total errors',
                                    'total errors:write', 'total gc count',
                                    'total gc memory', 'total gc time',
                                    'avg gc time', 'stddev gc time', 'total operation time']

        with pytest.raises(ToolError):
            node1.stress_object(['abc', 'n=10'])
