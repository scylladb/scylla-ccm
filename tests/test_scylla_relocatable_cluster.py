import pytest

from ccmlib.common import get_scylla_full_version, get_scylla_version


@pytest.mark.reloc
class TestScyllaRelocatableCluster:
    def test_get_scylla_full_version(self, relocatable_cluster):
        install_dir = relocatable_cluster.get_install_dir()
        assert get_scylla_full_version(install_dir) == '3.0-0.20200829.f1255cb2d02'

    def test_get_scylla_version(self, relocatable_cluster):
        install_dir = relocatable_cluster.get_install_dir()
        assert get_scylla_version(install_dir) == '3.0'

