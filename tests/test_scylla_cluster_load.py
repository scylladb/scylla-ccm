from ccmlib import scylla_repository
from ccmlib.scylla_cluster import ScyllaCluster


def test_load_existing_cluster_skips_repository_setup(tmp_path, monkeypatch):
    """#771: loading an existing cluster must not re-resolve the version against S3"""
    def fail(*args, **kwargs):
        raise AssertionError("scylla_repository.setup() called on cluster load")
    monkeypatch.setattr(scylla_repository, 'setup', fail)
    monkeypatch.setattr('ccmlib.common.get_scylla_version', lambda install_dir: '2026.1.9')

    install_dir = tmp_path / 'release' / '2026.1.9'
    install_dir.mkdir(parents=True)
    cluster = ScyllaCluster(str(tmp_path), 'test', install_dir=str(install_dir), create_directory=False,
                            cassandra_version='release:2026.1')
    assert cluster.scylla_mode == 'release'
