import os
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


if __name__ == "__main__":
    generate_ssl_stores('/home/fruch/ccm_ssl', dns_names=['any.cluster-id.scylla.com'])
