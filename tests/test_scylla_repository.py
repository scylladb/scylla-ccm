import time
import typing
from pathlib import Path
import random
import re

import pytest

from ccmlib.scylla_repository import setup as scylla_setup, CORE_PACKAGE_DIR_NAME, SOURCE_FILE_NAME
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
        ("release:2024.2", '2024.2'),
        ("release:2024.2:debug", '2024.2'),
        ("release:5.1", '5.1'),
        ("release:5.0", '5.0'),
        ("release:4.6", '4.6'),
        ("release:4.5", '4.5'),
        ("release:4.4", '4.4'),
        ("release:4.3", '4.3'),
        ("release:4.2", '4.2'),
        ("release:4.1", '4.1'),
        ("release:4.0", '4.0'),
        ("release:6.1", '6.1'),
        ("release:6.1:debug", '6.1'),
    ])
    def test_setup_release_oss(self, version, expected_version_string):
        cdir, version = scylla_setup(version=version, verbose=True)
        assert expected_version_string in version

    @pytest.mark.parametrize(argnames=['version', 'expected_version_string'], argvalues=[
        ("unstable/master:2023-09-05T13:59:07Z", '5.4.0-dev'),
        ("unstable/master:2023-09-05T13:59:07Z:debug", '5.4.0-dev'),
    ])
    def test_setup_unstable_master_new_url(self, version, expected_version_string):
        cdir, version = scylla_setup(version=version, verbose=True)
        assert version == expected_version_string

    @pytest.mark.parametrize(argnames=['version', 'expected_version_string'], argvalues=[
        ("unstable/enterprise:2023-06-15T06:53:32Z", '2023.3.0-dev'),
        ("unstable/enterprise:2023-06-15T06:53:32Z:debug", '2023.3.0-dev'),
    ])
    def test_setup_unstable_enterprise_new_url(self, version, expected_version_string):
        cdir, version = scylla_setup(version=version, verbose=True)
        assert version == expected_version_string


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
        ("release:6.1", '6.1'),
        ("release:6.1:debug", '6.1'),
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
        ("release:2024.2~rc0", '2024.2.0~rc0'),
        ("release:2024.2", '2024.2'),
        ("release:2024.2:debug", '2024.2'),
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

    @pytest.mark.parametrize(argnames=['version', 'expected_cdir', 'scylla_debug'], argvalues=[
        ("unstable/master:2021-01-18T15:48:13Z", "2021-01-18T15:48:13Z", False),
        ("unstable/master:2021-01-18T15:48:13Z:debug", "2021-01-18T15:48:13Z", True),
    ])
    def test_setup_unstable_master_new_url(self, version, expected_cdir, scylla_debug):
        cdir, packages = scylla_setup(version=version, verbose=True, skip_downloads=True)
        assert re.sub(":", "_", expected_cdir) in cdir
        assert packages.scylla_unified_package == f'http://s3.amazonaws.com/downloads.scylladb.com/unstable/scylla/master/relocatable/{expected_cdir}/scylla{"-debug" if scylla_debug else ""}-unified-package-4.4.dev.0.20210118.df3ef800c.tar.gz'

    @pytest.mark.parametrize(argnames=['version', 'expected_cdir', 'scylla_debug'], argvalues=[
        ("unstable/enterprise:2023-09-10T20:18:18Z", "2023-09-10T20:18:18Z", False),
        ("unstable/enterprise:2023-09-10T20:18:18Z:debug", "2023-09-10T20:18:18Z", True),
    ])
    def test_setup_unstable_enterprise_new_url(self, version, expected_cdir, scylla_debug):
        cdir, packages = scylla_setup(version=version, verbose=True, skip_downloads=True)
        assert re.sub(":", "_", expected_cdir) in cdir
        assert packages.scylla_unified_package == f'http://s3.amazonaws.com/downloads.scylladb.com/unstable/scylla-enterprise/enterprise/relocatable/{expected_cdir}/scylla-enterprise{"-debug" if scylla_debug else ""}-unified-2023.3.0~dev-0.20230910.4629201aceec.x86_64.tar.gz'

class TestReinstallPackages:
    @staticmethod
    def corrupt_hash_value(source_file):
        file_text = source_file.read_text()
        file_text = file_text.replace("hash=", "hash=123")
        source_file.write_text(file_text)

    @pytest.mark.skip("no more version with no unified package, TODO: remove when rest of related code is removed")
    def test_setup_no_unified_packages_reinstall(self):
        """
        Validate that if package hash is changed, new package will be downloaded.
        - download the Scylla packages. Packages hash will be saved in the "source.txt" file under relevant package folder
        - change the hash to be wrong for one of the packages (choose the package randomly). No matter hash of which package is wrong -
        all packages should be re-downloaded
        - run setup again. It expected that the packages will be downloaded again. The download time should be not short.
        Actually time without download should be around 5 ms, and with download about 35 ms. I put here more than 20
        """
        cdir, version = scylla_setup(version="unstable/master:2021-01-18T15:48:13Z", verbose=True, skip_downloads=False)
        assert '2021-01-18T15_48_13Z' in cdir
        assert version == '4.4.dev'

        package_to_corrupt = random.choice([CORE_PACKAGE_DIR_NAME, "scylla-tools-java", "scylla-jmx"])
        self.corrupt_hash_value(Path(cdir) / package_to_corrupt / SOURCE_FILE_NAME)

        start_time = time.time()
        cdir, version = scylla_setup(version="unstable/master:2021-01-18T15:48:13Z", verbose=True, skip_downloads=False)
        end_time = time.time()
        assert (end_time - start_time) > 20

        assert '2021-01-18T15_48_13Z' in cdir
        assert version == '4.4.dev'

    def test_setup_unified_package_reinstall(self):
        """
        Validate that if package hash is changed, new package will be downloaded.
        - download the unified package. Package hash will be saved in the "source.txt" file
        - change the hash to be wrong
        - run setup again. It expected that the package will be downloaded again. The download time should be not short.
        Actually time without download should be less than 3 ms, and with download about 9 ms. I put here more than 20
        """
        cdir, version = scylla_setup(version="unstable/master:2025-01-19T09:39:05Z", verbose=True, skip_downloads=False)
        assert '2025-01-19T09_39_05Z' in cdir
        assert version == '2025.1.0-dev'

        self.corrupt_hash_value(Path(cdir) / CORE_PACKAGE_DIR_NAME / SOURCE_FILE_NAME)

        scylla_setup.cache_clear()
        
        start_time = time.time()
        cdir, version = scylla_setup(version="unstable/master:2025-01-19T09:39:05Z", verbose=True, skip_downloads=False)
        end_time = time.time()
        assert (end_time - start_time) > 5
        assert '2025-01-19T09_39_05Z' in cdir
        assert version == '2025.1.0-dev'


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
