import os

import pytest

from ccmlib import common, scylla_repository
from ccmlib.cluster_factory import ClusterFactory
from ccmlib.scylla_cluster import ScyllaCluster


def load_cluster(tmp_path, mkdir=True, version='release:2026.1'):
    install_dir = tmp_path / 'release' / '2026.1.9'
    if mkdir:
        install_dir.mkdir(parents=True, exist_ok=True)
    return ScyllaCluster(str(tmp_path), 'test', install_dir=str(install_dir), create_directory=False,
                         cassandra_version=version)


@pytest.fixture
def setup_calls(tmp_path, monkeypatch):
    """fake scylla_repository.setup(), returns the versions it was called with"""
    calls = []
    def setup(version, verbose):
        calls.append(version)
        return str(tmp_path / 'release' / '2026.1.9'), '2026.1.9'
    monkeypatch.setattr(scylla_repository, 'setup', setup)
    return calls


def test_load_existing_cluster_skips_repository_setup(tmp_path, monkeypatch, setup_calls):
    """#771: loading an existing cluster must not re-resolve the version against S3"""
    monkeypatch.setattr(common, 'validate_install_dir', lambda install_dir: None)
    monkeypatch.setattr(common, 'get_scylla_version', lambda install_dir: '2026.1.9')

    cluster = load_cluster(tmp_path)
    assert cluster.scylla_mode == 'release'
    assert setup_calls == []


@pytest.mark.parametrize('mkdir', [True, False], ids=['empty', 'deleted'])
def test_load_damaged_install_falls_back_to_setup(tmp_path, setup_calls, mkdir):
    """an empty or deleted install recorded in cluster.conf is repaired by setup(), like before #771"""
    load_cluster(tmp_path, mkdir=mkdir)
    assert setup_calls == ['release:2026.1']


def test_load_install_without_version_file_falls_back_to_setup(tmp_path, monkeypatch, setup_calls):
    """get_scylla_version() raises CCMError when SCYLLA-VERSION-FILE is missing"""
    monkeypatch.setattr(common, 'validate_install_dir', lambda install_dir: None)
    (tmp_path / 'release' / '2026.1.9' / 'bin').mkdir(parents=True)
    (tmp_path / 'release' / '2026.1.9' / 'bin' / 'scylla').touch()

    load_cluster(tmp_path)
    assert setup_calls == ['release:2026.1']


def test_load_unstable_cluster_keeps_hash_validation(tmp_path, monkeypatch, setup_calls):
    """a valid unstable install still goes through setup(), which re-downloads a changed package"""
    monkeypatch.setattr(common, 'validate_install_dir', lambda install_dir: None)
    monkeypatch.setattr(common, 'get_scylla_version', lambda install_dir: '2026.1.9')

    load_cluster(tmp_path, version='unstable/master:latest')
    assert setup_calls == ['unstable/master:latest']


def test_factory_load_deleted_install_falls_back_to_setup(tmp_path, monkeypatch, setup_calls):
    """ccm commands repair a relocatable cluster whose install dir was deleted, and save the repaired dir"""
    monkeypatch.setenv('CCM_CONFIG_DIR', str(tmp_path))
    (tmp_path / 'test').mkdir()
    (tmp_path / 'test' / 'cluster.conf').write_text(
        f"name: test\nnodes: []\nseeds: []\n"
        f"install_dir: {tmp_path / 'deleted'}\nscylla_version: release:2026.1\n")
    (tmp_path / 'test' / 'cluster.conf').chmod(0o640)

    cluster = ClusterFactory.load(str(tmp_path), 'test')
    assert isinstance(cluster, ScyllaCluster)
    assert setup_calls == ['release:2026.1']
    assert os.listdir(tmp_path / 'test') == ['cluster.conf']
    assert (tmp_path / 'test' / 'cluster.conf').stat().st_mode & 0o777 == 0o640

    # setup() restored a newer patch dir, the next command reuses it without another repair
    (tmp_path / 'release' / '2026.1.9').mkdir(parents=True)
    monkeypatch.setattr(common, 'validate_install_dir', os.listdir)  # raises OSError for the deleted dir
    monkeypatch.setattr(common, 'get_scylla_version', lambda install_dir: '2026.1.9')
    ClusterFactory.load(str(tmp_path), 'test')
    assert setup_calls == ['release:2026.1']
