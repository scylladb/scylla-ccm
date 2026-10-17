import subprocess

from ccmlib.utils.ssl_utils import generate_ssl_stores


def test_generate_ssl_stores_creates_scylla_pem_set(tmp_path):
    """enable_internode_ssl() (scylla_cluster.py) requires 'trust.pem'; no keytool/Java needed."""
    generate_ssl_stores(str(tmp_path))

    for name in ("trust.pem", "ccm_node.pem", "ccm_node.key", "ccm_node.cer"):
        assert (tmp_path / name).exists()
    assert not (tmp_path / "keystore.jks").exists()
    subprocess.check_call(["openssl", "verify", "-CAfile", tmp_path / "trust.pem", tmp_path / "ccm_node.pem"])


def test_generate_ssl_stores_is_noop_when_ccm_node_pem_exists(tmp_path):
    (tmp_path / "ccm_node.pem").write_bytes(b"existing")
    generate_ssl_stores(str(tmp_path))

    assert (tmp_path / "ccm_node.pem").read_bytes() == b"existing"
    assert not (tmp_path / "trust.pem").exists()
