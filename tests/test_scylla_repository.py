from unittest.mock import patch

import pytest

from ccmlib.scylla_repository import setup


@pytest.mark.repo_tests
@pytest.mark.skip("slow integration test")
class TestScyllaRepository:
    def test_setup_release_oss(self):
        cdir, version = setup(version="release:4.2.1", verbose=True)
        assert version == '4.2.1'

    @patch.dict('os.environ', {'SCYLLA_PRODUCT': 'scylla-enterprise'})
    def test_setup_release_enterprise(self):
        cdir, version = setup(version="release:2020.1.5", verbose=True)
        assert version == '2020.1.5'

    def test_setup_unstable_master_new_url(self):
        cdir, version = setup(version="unstable/master:2021-01-18T15:48:13Z", verbose=True)
        assert version == '4.4.dev'

    def test_setup_unstable_master_old_url(self):
        cdir, version = setup(version="unstable/master:2020-08-29T22:24:05Z", verbose=True)
        assert version == '3.0'
