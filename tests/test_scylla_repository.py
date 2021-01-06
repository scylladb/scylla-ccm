from unittest.mock import patch

import pytest

from ccmlib.scylla_repository import setup


@pytest.mark.repo_tests
class TestScyllaRepository:
    @pytest.mark.skip("slow integration test")
    def test_setup_release_oss(self):
        cdir, version = setup(version="release:4.2.1", verbose=True)
        assert version == '4.2.1'

    @pytest.mark.skip("slow integration test")
    @patch.dict('os.environ', {'SCYLLA_PRODUCT': 'scylla-enterprise'})
    def test_setup_release_enterprise(self):
        cdir, version = setup(version="release:2020.1.5", verbose=True)
        assert version == '2020.1.5'