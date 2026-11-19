from unittest.mock import patch

import pytest

from ccmlib.cluster import Cluster
from ccmlib.common import CCMError
from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.utils.ssl_utils import generate_ssl_stores


def _bare_scylla_cluster(cluster_dir):
    """A ScyllaCluster instance with just enough state for enable_internode_ssl()."""
    with patch.object(ScyllaCluster, '__init__', lambda self, *a, **kw: None):
        cluster = ScyllaCluster(None)
    cluster._config_options = {}
    cluster._update_config = lambda *a, **kw: None
    cluster.get_path = lambda: str(cluster_dir)
    return cluster


def test_enable_internode_ssl_succeeds_with_generated_stores(tmp_path):
    ssl_dir = tmp_path / "ssl"
    ssl_dir.mkdir()
    generate_ssl_stores(str(ssl_dir))

    cluster_dir = tmp_path / "cluster"
    cluster_dir.mkdir()
    cluster = _bare_scylla_cluster(cluster_dir)

    cluster.enable_internode_ssl(str(ssl_dir))

    assert (cluster_dir / "internode-trust.pem").exists()
    assert (cluster_dir / "internode-ccm_node.pem").exists()
    assert (cluster_dir / "internode-ccm_node.key").exists()
    ssl_options = cluster._config_options["server_encryption_options"]
    assert ssl_options["internode_encryption"] == "all"


def test_enable_internode_ssl_raises_clear_error_when_trust_pem_missing(tmp_path):
    ssl_dir = tmp_path / "ssl"
    ssl_dir.mkdir()
    (ssl_dir / "ccm_node.pem").write_text("cert")
    (ssl_dir / "ccm_node.key").write_text("key")

    cluster_dir = tmp_path / "cluster"
    cluster_dir.mkdir()
    cluster = _bare_scylla_cluster(cluster_dir)

    with pytest.raises(FileNotFoundError, match="trust.pem"):
        cluster.enable_internode_ssl(str(ssl_dir))


@pytest.mark.parametrize("method", ["enable_ssl", "enable_internode_ssl"])
def test_cassandra_cluster_ssl_raises_clear_error_for_pem_only_dir(tmp_path, method):
    generate_ssl_stores(str(tmp_path))
    with patch.object(Cluster, '__init__', lambda self, *a, **kw: None):
        cluster = Cluster(None)
    args = (str(tmp_path), False) if method == "enable_ssl" else (str(tmp_path),)

    with pytest.raises(CCMError, match=r"keystore\.jks.*Scylla PEM"):
        getattr(cluster, method)(*args)


def test_cassandra_enable_ssl_requires_truststore_for_client_auth(tmp_path):
    (tmp_path / "keystore.jks").write_text("ks")
    (tmp_path / "cassandra.crt").write_text("crt")
    with patch.object(Cluster, '__init__', lambda self, *a, **kw: None):
        cluster = Cluster(None)

    with pytest.raises(CCMError, match=r"truststore\.jks"):
        cluster.enable_ssl(str(tmp_path), True)
