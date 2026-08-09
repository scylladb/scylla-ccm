from unittest.mock import patch

import pytest

from ccmlib import cluster as cluster_module
from ccmlib.cluster import Cluster


def _bare_cluster():
    with patch.object(Cluster, '__init__', lambda self, *a, **kw: None):
        return Cluster(None)


def test_remove_dir_with_retry_removes_existing_dir(tmp_path):
    target = tmp_path / "somedir"
    target.mkdir()
    (target / "file.txt").write_text("data")

    _bare_cluster().remove_dir_with_retry(str(target))

    assert not target.exists()


def test_remove_dir_with_retry_raises_instead_of_silently_leaking(tmp_path, monkeypatch):
    """common.rmdirs() used to default to ignore_errors=True, so a persistent
    removal failure (e.g. rootless-podman subuid-owned files) was silently
    swallowed and remove_dir_with_retry() reported success without actually
    removing anything. It must now retry and finally raise."""
    target = tmp_path / "somedir"
    target.mkdir()

    calls = []

    def fake_rmdirs(path, ignore_errors=True):
        calls.append(ignore_errors)
        raise PermissionError(f"denied: {path}")

    monkeypatch.setattr(cluster_module.common, "rmdirs", fake_rmdirs)
    monkeypatch.setattr(cluster_module.time, "sleep", lambda _s: None)

    with pytest.raises(PermissionError):
        _bare_cluster().remove_dir_with_retry(str(target))

    assert calls == [False] * 5
