import os
import shutil
import subprocess
import logging

logger = logging.getLogger(__name__)


def generate_ssl_stores(base_dir, passphrase='cassandra', dns_names=None):
    """
    Generate the Scylla PEM set (ccm_node.pem, ccm_node.key, ccm_node.cer, trust.pem) in base_dir via openssl.
    No-op if the full set exists; a partial set is regenerated. `passphrase` is ignored (kept for API compatibility); no JKS stores are produced.
    """
    generate_ssl_stores_openssl(base_dir, dns_names)


def generate_ssl_stores_openssl(base_dir, dns_names=None, key_type='secp384r1'):
    """
    Generate a CA-signed node cert with openssl, following
    docs/operating-scylla/security/generate-certificate.rst in scylladb.
    Produces ccm_node.pem/ccm_node.key/ccm_node.cer plus trust.pem (the CA) for --node-ssl.
    No-op if all four exist; a partial set is regenerated (overwritten).

    @param key_type 'rsa:<bits>' or an EC curve name; default P-384 (CNSA-compliant, ~80x cheaper to sign than RSA-4096)
    """
    names = ('ccm_node.pem', 'ccm_node.key', 'ccm_node.cer', 'trust.pem')
    present = [n for n in names if os.path.exists(os.path.join(base_dir, n))]
    if len(present) == len(names):
        print("openssl certs already exist - skipping generation")
        return
    p = {n: os.path.join(base_dir, n) for n in names}
    # Only the CA copy is missing: reuse a valid bundle rather than rotating the CA under existing peers.
    if present == ['ccm_node.pem', 'ccm_node.key', 'trust.pem'] and \
            subprocess.call(['openssl', 'verify', '-CAfile', p['trust.pem'], p['ccm_node.pem']]) == 0:
        shutil.copyfile(p['trust.pem'], p['ccm_node.cer'])
        return
    if present:
        print(f"incomplete cert set in [{base_dir}] (found {', '.join(present)}) - regenerating all")
    os.makedirs(base_dir, exist_ok=True)
    san = ",".join(f"DNS:{n}" for n in dns_names or ['any.cluster-id.scylla.com'])

    def cfg(cn):
        return f"""[ req ]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no
[ req_distinguished_name ]
O = CCM
OU = CCMnode
CN = {cn}
[v3_ca]
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid:always,issuer:always
basicConstraints = critical,CA:true
keyUsage = critical, keyCertSign, cRLSign
[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
subjectAltName = {san}
"""

    def genkey(out):
        if key_type.startswith('rsa:'):
            subprocess.check_call(['openssl', 'genrsa', '-out', out, key_type[4:]])
        else:
            subprocess.check_call(['openssl', 'ecparam', '-name', key_type, '-genkey', '-noout', '-out', out])

    def path(n):
        return os.path.join(base_dir, n)

    # CN must differ between CA and node cert, or `openssl verify` fails.
    with open(path('ca.cfg'), 'w') as f:
        f.write(cfg('CCM CA'))
    with open(path('node.cfg'), 'w') as f:
        f.write(cfg('Cassandra Node'))
    print(f"generating openssl CA and node cert ({key_type}) in [{base_dir}]")
    genkey(path('trust.key'))
    subprocess.check_call(['openssl', 'req', '-x509', '-new', '-nodes', '-key', path('trust.key'), '-days', '3650',
                           '-config', path('ca.cfg'), '-extensions', 'v3_ca', '-out', path('trust.pem')])
    genkey(path('ccm_node.key'))
    subprocess.check_call(['openssl', 'req', '-new', '-key', path('ccm_node.key'), '-out', path('ccm_node.csr'),
                           '-config', path('node.cfg')])
    subprocess.check_call(['openssl', 'x509', '-req', '-in', path('ccm_node.csr'), '-CA', path('trust.pem'),
                           '-CAkey', path('trust.key'), '-CAcreateserial', '-out', path('ccm_node.pem'),
                           '-days', '365', '-sha256', '-extfile', path('node.cfg'), '-extensions', 'v3_req'])
    subprocess.check_call(['openssl', 'verify', '-CAfile', path('trust.pem'), path('ccm_node.pem')])
    # Truststore for require_client_auth: the CA, same as --node-ssl uses.
    shutil.copyfile(path('trust.pem'), path('ccm_node.cer'))


if __name__ == "__main__":
    generate_ssl_stores('/home/fruch/ccm_ssl', dns_names=['any.cluster-id.scylla.com'])
