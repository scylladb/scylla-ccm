from ccmlib import common, scylla_repository
from ccmlib.scylla_cluster import ScyllaCluster


def load_cluster(tmp_path):
    install_dir = tmp_path / 'release' / '2026.1.9'
    install_dir.mkdir(parents=True, exist_ok=True)
    return ScyllaCluster(str(tmp_path), 'test', install_dir=str(install_dir), create_directory=False,
                         cassandra_version='release:2026.1')


def test_load_existing_cluster_skips_repository_setup(tmp_path, monkeypatch):
    """#771: loading an existing cluster must not re-resolve the version against S3"""
    def fail(*args, **kwargs):
        raise AssertionError("scylla_repository.setup() called on cluster load")
    monkeypatch.setattr(scylla_repository, 'setup', fail)
    monkeypatch.setattr(common, 'validate_install_dir', lambda install_dir: None)
    monkeypatch.setattr(common, 'get_scylla_version', lambda install_dir: '2026.1.9')

    cluster = load_cluster(tmp_path)
    assert cluster.scylla_mode == 'release'


def test_load_damaged_install_falls_back_to_setup(tmp_path, monkeypatch):
    """a damaged install recorded in cluster.conf is repaired by setup(), like before #771"""
    calls = []
    def setup(version, verbose):
        calls.append(version)
        return str(tmp_path / 'release' / '2026.1.9'), '2026.1.9'
    def invalid(install_dir):
        raise common.ArgumentError(f"{install_dir} is damaged")
    monkeypatch.setattr(scylla_repository, 'setup', setup)
    monkeypatch.setattr(common, 'validate_install_dir', invalid)

    load_cluster(tmp_path)
    assert calls == ['release:2026.1']
