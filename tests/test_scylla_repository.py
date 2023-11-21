import typing

import pytest

from ccmlib.scylla_repository import setup as scylla_setup
from ccmlib.scylla_repository import (
    get_manager_release_url,
    get_manager_latest_reloc_url,
    Architecture,
)


@pytest.mark.repo_tests
@pytest.mark.skip("slow integration test")
class TestScyllaRepository:
    @pytest.mark.parametrize(argnames=['version', 'expected_version_string'], argvalues=[
        ("release:2020.1", '2020.1'),
        ("release:2021.1", '2021.1'),
        ("release:2022.1", '2022.1'),
        ("release:2022.2", '2022.2'),
        ("release:2023.1", '2023.1'),
        ("release:5.1", '5.1'),
        ("release:5.0", '5.0'),
        ("release:4.6", '4.6'),
        ("release:4.5", '4.5'),
        ("release:4.4", '4.4'),
        ("release:4.3", '4.3'),
        ("release:4.2", '4.2'),
        ("release:4.1", '4.1'),
        ("release:4.0", '4.0'),
    ])
    def test_setup_release_oss(self, version, expected_version_string):
        cdir, version = scylla_setup(version=version, verbose=True)
        assert expected_version_string in version

    def test_setup_unstable_master_new_url(self):
        cdir, version = scylla_setup(version="unstable/master:2023-09-05T13:59:07Z", verbose=True)
        assert version == '5.4.0-dev'

    def test_setup_unstable_enterprise_new_url(self):
        cdir, version = scylla_setup(version="unstable/enterprise:2023-06-15T06:53:32Z", verbose=True)
        assert version == '2023.3.0-dev'



class TestScyllaRepositoryRelease:
    @pytest.mark.parametrize(argnames=['version', 'expected_cdir'], argvalues=[
        ("release:5.1", 'release/5.1'),
        ("release:5.1~rc1", '5.1.0~rc1'),
        ("release:5.1.rc1", '5.1.0~rc1'),
        ("release:5.1-rc1", '5.1.0~rc1'),
        ("release:5.0", '5.0'),
        ("release:5.0~rc2", '5.0.rc2'),
        ("release:5.0.rc2", '5.0.rc2'),
        ("release:5.0-rc2", '5.0.rc2'),
        ("release:5.0.3", '5.0.3'),
        ("release:4.6", '4.6'),
        ("release:4.5", '4.5'),
        ("release:4.4", '4.4'),
        ("release:4.3", '4.3'),
    ])
    def test_setup_release_oss(self, version, expected_cdir):
        cdir, packages = scylla_setup(version=version, verbose=True, skip_downloads=True)
        assert expected_cdir in cdir
        assert packages.scylla_unified_package

    @pytest.mark.parametrize(argnames=['version', 'expected_cdir'], argvalues=[
        ("release:4.2", '4.2'),
        ("release:4.1", '4.1'),
        ("release:4.0", '4.0'),
    ])
    def test_setup_release_oss_no_unified_package(self, version, expected_cdir):
        cdir, packages = scylla_setup(version=version, verbose=True, skip_downloads=True)
        assert expected_cdir in cdir
        assert packages.scylla_unified_package is None
        assert packages.scylla_package
        assert packages.scylla_tools_package
        assert packages.scylla_jmx_package

    @pytest.mark.parametrize(argnames=['version', 'expected_cdir'], argvalues=[
        ("release:2023.1.0~rc0", '2023.1.0~rc0'),
        ("release:2022.2", '2022.2'),
        ("release:2022.2~rc1", '2022.2.0~rc1'),
        ("release:2022.2.rc1", '2022.2.0~rc1'),
        ("release:2022.2-rc1", '2022.2.0~rc1'),
        ("release:2022.1", '2022.1'),
        ("release:2022.1~rc5", '2022.1.rc5'),
        ("release:2022.1.rc5", '2022.1.rc5'),
        ("release:2022.1-rc5", '2022.1.rc5'),
        ("release:2021.1", '2021.1'),
        ("release:2021.1.10", '2021.1.10'),
    ])
    def test_setup_release_enterprise(self, version, expected_cdir):
        cdir, packages = scylla_setup(version=version, verbose=True, skip_downloads=True)
        assert expected_cdir in cdir
        assert packages.scylla_unified_package

    @pytest.mark.parametrize(argnames=['version', 'expected_cdir'], argvalues=[
        ("release:2020.1", '2020.1'),
        ("release:2020.1.10", '2020.1.10'),
    ])
    def test_setup_release_enterprise_no_unified_package(self, version, expected_cdir):
        cdir, packages = scylla_setup(version=version, verbose=True, skip_downloads=True)
        assert expected_cdir in cdir
        assert packages.scylla_unified_package is None
        assert packages.scylla_package
        assert packages.scylla_tools_package
        assert packages.scylla_jmx_package

    def test_setup_unstable_master_new_url(self):
        cdir, packages = scylla_setup(version="unstable/master:2021-01-18T15:48:13Z", verbose=True, skip_downloads=True)
        assert '2021-01-18T15_48_13Z' in cdir
        assert packages.scylla_unified_package is None
        assert packages.scylla_package == 'https://s3.amazonaws.com/downloads.scylladb.com/unstable/scylla/master/relocatable/2021-01-18T15:48:13Z/scylla-package.tar.gz'
        assert packages.scylla_tools_package == 'https://s3.amazonaws.com/downloads.scylladb.com/unstable/scylla/master/relocatable/2021-01-18T15:48:13Z/scylla-tools-package.tar.gz'
        assert packages.scylla_jmx_package == 'https://s3.amazonaws.com/downloads.scylladb.com/unstable/scylla/master/relocatable/2021-01-18T15:48:13Z/scylla-jmx-package.tar.gz'


@pytest.mark.parametrize('architecture', argvalues=typing.get_args(Architecture))
class TestGetManagerFunctions:
    def test_get_manager_latest_reloc_url(self, architecture):
        master_version = get_manager_latest_reloc_url(architecture=architecture)
        # the URL looks like
        # https://s3.amazonaws.com/downloads.scylladb.com/manager/
        #   relocatable/unstable/master/2023-11-20T21:55:21Z/scylla-manager_3.2.3-0.20231115.6f5dc312-SNAPSHOT_linux_x86_64.tar.gz
        assert 'relocatable/unstable/master' in master_version
        assert '-SNAPSHOT' in master_version
        assert architecture in master_version

        branch_version = get_manager_latest_reloc_url('branch-3.1', architecture=architecture)
        assert 'relocatable/unstable/branch-3.1' in branch_version
        assert architecture in branch_version

    def test_get_manager_release_url(self, architecture):
        specific_version = get_manager_release_url('3.1.1', architecture=architecture)
        assert specific_version == 'https://s3.amazonaws.com/downloads.scylladb.com/downloads/scylla-manager/' \
                                   f'relocatable/scylladb-manager-3.1/scylla-manager_3.1.1-0.20230612.401edeb8_linux_{architecture}.tar.gz'
