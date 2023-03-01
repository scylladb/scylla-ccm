from unittest.mock import patch

import pytest

from ccmlib.scylla_repository import setup as scylla_setup


@pytest.mark.repo_tests
@pytest.mark.skip("slow integration test")
class TestScyllaRepository:
    @pytest.mark.parametrize(argnames=['version', 'expected_version_string'], argvalues=[
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

    @patch.dict('os.environ', {'SCYLLA_PRODUCT': 'scylla-enterprise'})
    def test_setup_release_enterprise(self):
        cdir, version = scylla_setup(version="release:2020.1.5", verbose=True)
        assert version == '2020.1.5'

    def test_setup_unstable_master_new_url(self):
        cdir, version = scylla_setup(version="unstable/master:2021-01-18T15:48:13Z", verbose=True)
        assert version == '4.4.dev'

    def test_setup_unstable_master_old_url(self):
        cdir, version = scylla_setup(version="unstable/master:2020-08-29T22:24:05Z", verbose=True)
        assert version == '3.0'
