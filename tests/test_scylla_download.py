import pathlib

import pytest
import requests

from ccmlib.utils.download import download_file, download_version_from_s3, get_url_hash


@pytest.mark.repo_tests
class TestUtilsDownload:
    def test_download_version_from_s3(self, tmpdir):
        target_path = str(pathlib.Path(tmpdir) / 'scylla-manager.repo')
        res = download_version_from_s3("https://s3.amazonaws.com/downloads.scylladb.com/manager/rpm/unstable/centos/master/latest/scylla-manager.repo",
                                       target_path=target_path)
        assert res == target_path

    def test_download_version_from_s3_non_exist_file(self, tmpdir):
        res = download_version_from_s3("https://s3.amazonaws.com/downloads.scylladb.com/abcdefg",
                                       target_path=pathlib.Path(tmpdir) / 'scylla-manager.repo')
        assert res is None

    def test_download_file(self, tmpdir):
        target_path = str(pathlib.Path(tmpdir) / 'scylla-manager.repo')
        res = download_file("https://s3.amazonaws.com/downloads.scylladb.com/manager/rpm/unstable/centos/master/latest/scylla-manager.repo",
                            target_path=target_path)
        assert res == target_path

    def test_download_file_non_exist_file(self, tmpdir):
        with pytest.raises(requests.exceptions.HTTPError, match='Not Found'):
            download_file("https://s3.amazonaws.com/downloads.scylladb.com/abcdefg",
                          target_path=pathlib.Path(tmpdir) / 'scylla-manager.repo')

    def test_get_local_tarball_hash(self):
        this_path = pathlib.Path(__file__).parent
        url_hash = get_url_hash(url=str(this_path / "tests" / "test_data" / "scylla_unified_master_2023_04_03.tar.gz"))
        assert url_hash == 'd2be7852b8c65f74c1da8c9efbc7e408'
