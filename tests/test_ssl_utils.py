import filecmp
import os

from ccmlib.utils.ssl_utils import generate_ssl_stores


def test_generate_ssl_stores_creates_trust_pem(tmp_path):
    """enable_internode_ssl() (scylla_cluster.py) requires a 'trust.pem' file;
    generate_ssl_stores() must produce one alongside the other cert/key files."""
    generate_ssl_stores(str(tmp_path))

    trust_pem = tmp_path / "trust.pem"
    ccm_node_pem = tmp_path / "ccm_node.pem"
    assert trust_pem.exists()
    assert ccm_node_pem.exists()
    assert filecmp.cmp(trust_pem, ccm_node_pem, shallow=False)


def test_generate_ssl_stores_is_noop_when_keystore_exists(tmp_path):
    (tmp_path / "keystore.jks").write_bytes(b"existing")
    generate_ssl_stores(str(tmp_path))

    assert not os.path.exists(tmp_path / "trust.pem")
