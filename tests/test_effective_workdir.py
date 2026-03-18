"""
Tests for the workdir symlink logic that keeps Unix domain socket paths
(maintenance socket cql.m) under the sun_path limit.
"""

import hashlib
import os
import tempfile
from unittest.mock import MagicMock

import pytest

from ccmlib.scylla_node import ScyllaNode, UNIX_SOCKET_PATH_MAX


def _make_node(node_path):
    """Create a ScyllaNode without full __init__, setting only what
    _get_effective_workdir needs (get_path and debug)."""
    node = ScyllaNode.__new__(ScyllaNode)
    node.name = os.path.basename(node_path)
    node.cluster = MagicMock()
    node.cluster.get_path.return_value = os.path.dirname(node_path)
    return node


@pytest.fixture
def symlink_tracker():
    """Tracks symlinks created during a test and cleans them up afterwards."""
    created = []
    yield created
    for path in created:
        try:
            if os.path.islink(path):
                os.unlink(path)
        except OSError:
            pass  # Best-effort cleanup; not fatal if the symlink is already gone.


class TestGetEffectiveWorkdir:

    def test_short_path_returns_unchanged(self, tmp_path):
        """When the path is short enough, it is returned as-is."""
        node_path = str(tmp_path / "node1")
        os.makedirs(node_path, exist_ok=True)
        node = _make_node(node_path)

        result = node._get_effective_workdir()

        assert result == node_path
        symlink_path = ScyllaNode._workdir_symlink_path(node_path)
        assert not os.path.islink(symlink_path)

    def test_long_path_creates_symlink(self, tmp_path, symlink_tracker):
        """When the path is too long, a symlink is created in /tmp."""
        long_dir_name = "a" * 120
        node_path = str(tmp_path / long_dir_name / "node1")
        os.makedirs(node_path, exist_ok=True)
        assert len(os.path.join(node_path, 'cql.m')) > UNIX_SOCKET_PATH_MAX

        node = _make_node(node_path)
        result = node._get_effective_workdir()
        symlink_tracker.append(result)

        assert result != node_path
        assert os.path.islink(result)
        assert os.readlink(result) == node_path
        assert result.startswith(tempfile.gettempdir())
        assert len(os.path.join(result, 'cql.m')) <= UNIX_SOCKET_PATH_MAX

    def test_long_path_reuses_existing_symlink(self, tmp_path, symlink_tracker):
        """Calling _get_effective_workdir twice returns the same symlink."""
        long_dir_name = "b" * 120
        node_path = str(tmp_path / long_dir_name / "node1")
        os.makedirs(node_path, exist_ok=True)

        node = _make_node(node_path)
        result1 = node._get_effective_workdir()
        symlink_tracker.append(result1)
        result2 = node._get_effective_workdir()

        assert result1 == result2
        assert os.path.islink(result1)

    def test_long_path_replaces_stale_symlink(self, tmp_path, symlink_tracker):
        """If symlink points to a different path, it is replaced."""
        long_dir_name = "c" * 120
        node_path = str(tmp_path / long_dir_name / "node1")
        os.makedirs(node_path, exist_ok=True)

        symlink_path = ScyllaNode._workdir_symlink_path(node_path)
        os.symlink("/some/other/path", symlink_path)
        symlink_tracker.append(symlink_path)

        node = _make_node(node_path)
        result = node._get_effective_workdir()

        assert result == symlink_path
        assert os.readlink(result) == node_path

    def test_long_path_raises_if_non_symlink_exists(self, tmp_path):
        """If a directory exists at the target, raise RuntimeError."""
        long_dir_name = "d" * 120
        node_path = str(tmp_path / long_dir_name / "node1")
        os.makedirs(node_path, exist_ok=True)

        symlink_path = ScyllaNode._workdir_symlink_path(node_path)
        os.makedirs(symlink_path, exist_ok=True)

        try:
            node = _make_node(node_path)
            with pytest.raises(RuntimeError, match="cannot be replaced"):
                node._get_effective_workdir()
        finally:
            os.rmdir(symlink_path)

    def test_cleanup_removes_symlink(self, tmp_path):
        """_cleanup_workdir_symlink removes the symlink when it matches."""
        long_dir_name = "e" * 120
        node_path = str(tmp_path / long_dir_name / "node1")
        os.makedirs(node_path, exist_ok=True)

        node = _make_node(node_path)
        result = node._get_effective_workdir()
        assert os.path.islink(result)

        node._cleanup_workdir_symlink()
        assert not os.path.islink(result)

    def test_cleanup_ignores_short_path(self, tmp_path):
        """_cleanup_workdir_symlink does nothing for short paths."""
        node_path = str(tmp_path / "node1")
        os.makedirs(node_path, exist_ok=True)
        node = _make_node(node_path)

        # Should not raise
        node._cleanup_workdir_symlink()

    def test_hash_uses_12_hex_chars(self, tmp_path):
        """Symlink name uses 12 hex chars from the MD5 hash."""
        long_dir_name = "f" * 120
        node_path = str(tmp_path / long_dir_name / "node1")
        os.makedirs(node_path, exist_ok=True)

        symlink_path = ScyllaNode._workdir_symlink_path(node_path)
        expected_hash = hashlib.md5(node_path.encode()).hexdigest()[:12]
        assert symlink_path.endswith(f'ccm-{expected_hash}')

        node = _make_node(node_path)
        result = node._get_effective_workdir()
        assert result == symlink_path
        os.unlink(result)
