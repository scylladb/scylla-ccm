import subprocess

import pytest

from ccmlib.utils.ssl_utils import generate_ssl_stores_openssl


@pytest.mark.parametrize("key_type,algo", [("secp384r1", "secp384r1"), ("rsa:2048", "rsaEncryption"), ("prime256v1", "prime256v1")])
def test_generate_ssl_stores_openssl(tmp_path, key_type, algo):
    generate_ssl_stores_openssl(str(tmp_path), key_type=key_type)
    for f in ("ccm_node.pem", "ccm_node.key", "ccm_node.cer", "trust.pem"):
        assert (tmp_path / f).exists()
    subprocess.check_call(["openssl", "verify", "-CAfile", tmp_path / "trust.pem", tmp_path / "ccm_node.pem"])
    text = subprocess.check_output(["openssl", "x509", "-in", tmp_path / "ccm_node.pem", "-noout", "-text"], text=True)
    assert algo in text
    assert "DNS:any.cluster-id.scylla.com" in text
    # CA must be RFC 5280 strict-mode clean, or Python 3.13's default verifier rejects it
    ca = subprocess.check_output(["openssl", "x509", "-in", tmp_path / "trust.pem", "-noout", "-text"], text=True)
    assert "X509v3 Basic Constraints: critical" in ca
    assert "Certificate Sign, CRL Sign" in ca
