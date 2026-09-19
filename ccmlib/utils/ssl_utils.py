import os
import shutil
import subprocess
import logging

logger = logging.getLogger(__name__)


def generate_ssl_stores(base_dir, passphrase='cassandra', dns_names=None):
    """
    Util for generating ssl stores using java keytool -- nondestructive method if stores already exist this method is
    a no-op.

    @param base_dir (str) directory where keystore.jks, truststore.jks and ccm_node.cer will be placed
    @param passphrase (Optional[str]) currently ccm expects a passphrase of 'cassandra' so it's the default but it can be
            overridden for failure testing
    @return None
    @throws CalledProcessError If the keytool fails during any step
    """

    if os.path.exists(os.path.join(base_dir, 'keystore.jks')):
        print("keystores already exists - skipping generation of ssl keystores")
        return

    legacy = ['-legacy'] if '-legacy' in subprocess.run(['openssl', 'pkcs12', '--help'],
                                                        universal_newlines=True, stderr=subprocess.PIPE).stderr else []
    dns_names = dns_names or ['any.cluster-id.scylla.com']
    ext = ",".join([f"dns:{name}" for name in dns_names])
    print(f"generating keystore.jks in [{base_dir}]")
    subprocess.check_call(['keytool', '-genkeypair', '-alias', 'ccm_node', '-keyalg', 'RSA', '-validity', '365',
                           '-keystore', os.path.join(base_dir, 'keystore.jks'), '-storepass', passphrase,
                           '-dname', 'cn=Cassandra Node,ou=CCMnode,o=DataStax,c=US', '-keypass', passphrase,
                           '-ext', f'san={ext}'])

    print(f"exporting cert from keystore.jks in [{base_dir}]")
    subprocess.check_call(['keytool', '-export', '-rfc', '-alias', 'ccm_node',
                           '-keystore', os.path.join(base_dir, 'keystore.jks'),
                           '-file', os.path.join(base_dir, 'ccm_node.cer'), '-storepass', passphrase])
    print(f"importing cert into truststore.jks in [{base_dir}]")
    subprocess.check_call(['keytool', '-import', '-file', os.path.join(base_dir, 'ccm_node.cer'),
                           '-alias', 'ccm_node', '-keystore', os.path.join(base_dir, 'truststore.jks'),
                           '-storepass', passphrase, '-noprompt'])
    # Added for scylla: Generate pem format cert/key
    print(f"exporting cert to pks12 from keystore.jks in [{base_dir}]")
    subprocess.check_call(['keytool', '-importkeystore', '-srckeystore', os.path.join(base_dir, 'keystore.jks'),
                           '-srcstorepass', passphrase, '-srckeypass', passphrase, '-destkeystore',
                           os.path.join(base_dir, 'ccm_node.p12'), '-deststoretype', 'PKCS12',
                           '-srcalias', 'ccm_node', '-deststorepass', passphrase, '-destkeypass', passphrase])
    print(f"Using openssl to split pks12 in [{base_dir}] to pem format")
    subprocess.check_call(['openssl', 'pkcs12', '-in', os.path.join(base_dir, 'ccm_node.p12'),
                           '-passin', f'pass:{passphrase}', '-nokeys',
                           '-out', os.path.join(base_dir, 'ccm_node.pem')] + legacy)
    # Key with password. We want without...
    subprocess.check_call(['openssl', 'pkcs12', '-in', os.path.join(base_dir, 'ccm_node.p12'),
                           '-passin', f'pass:{passphrase}',
                           '-passout', f'pass:{passphrase}', '-nocerts',
                           '-out', os.path.join(base_dir, 'ccm_node.tmp')] + legacy)
    subprocess.check_call(['openssl', 'pkcs8', '-in', os.path.join(base_dir, 'ccm_node.tmp'),
                           '-passin', f'pass:{passphrase}',
                           '-passout', f'pass:{passphrase}',
                           '-topk8',
                           '-out', os.path.join(base_dir, 'ccm_node.pkcs8')])
    subprocess.check_call(['openssl', 'rsa', '-in', os.path.join(base_dir, 'ccm_node.tmp'),
                           '-passin', f'pass:{passphrase}',
                           '-out', os.path.join(base_dir, 'ccm_node.key')])
    # enable_internode_ssl() (scylla_cluster.py) expects a 'trust.pem' trust
    # anchor; ccm_node.pem (cert only, no key) is exactly that.
    shutil.copyfile(os.path.join(base_dir, 'ccm_node.pem'), os.path.join(base_dir, 'trust.pem'))


def generate_ssl_stores_openssl(base_dir, dns_names=None, key_type='secp384r1'):
    """
    Generate a CA-signed node cert with openssl, following
    docs/operating-scylla/security/generate-certificate.rst in scylladb.
    Produces the same ccm_node.pem/ccm_node.key/ccm_node.cer as generate_ssl_stores(),
    plus trust.pem (the CA) for --node-ssl. No-op if ccm_node.pem exists.

    @param key_type 'rsa:<bits>' or an EC curve name; default P-384 (CNSA-compliant, ~80x cheaper to sign than RSA-4096)
    """
    if os.path.exists(os.path.join(base_dir, 'ccm_node.pem')):
        print("ccm_node.pem already exists - skipping generation of openssl certs")
        return
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
