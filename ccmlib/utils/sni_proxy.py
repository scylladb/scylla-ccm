import os
import string
import subprocess
import json
import base64
from contextlib import contextmanager
import tempfile
from textwrap import dedent
import distutils.dir_util

import yaml

from ccmlib.utils.ssl_utils import generate_ssl_stores


@contextmanager
def file_or_memory(path=None, data=None):
    # since we can't read keys/cert from memory yet
    # see https://github.com/python/cpython/pull/2449 which isn't accepted and PEP-543 that was withdrawn
    # so we use temporary file to load the key
    if data:
        with tempfile.NamedTemporaryFile(mode="wb") as f:
            d = base64.decodebytes(bytes(data, encoding='utf-8'))
            f.write(d)
            if not d.endswith(b"\n"):
                f.write(b"\n")

            f.flush()
            yield f.name

    if path:
        yield path


def create_cloud_config(ssl_dir, port, address, nodes_info, username='cassandra', password='cassandra'):

    def encode_base64(filename):
        return base64.b64encode(open(os.path.join(ssl_dir, filename), 'rb').read()).decode()

    cadata = encode_base64('ccm_node.cer')
    certificate_data = encode_base64('ccm_node.cer')
    key_data = encode_base64('ccm_node.key')
    _, _, _, data_center = list(nodes_info)[0]  # TODO: Add ability to create cloud config for multi-DC setup

    config = dict(datacenters={data_center: dict(certificateAuthorityData=cadata,
                                                 server=f'{address}:{port}',
                                                 nodeDomain='cql.cluster-id.scylla.com',
                                                 insecureSkipTlsVerify=False)},
                  authInfos={'default': dict(clientCertificateData=certificate_data,
                                             clientKeyData=key_data,
                                             username=username,
                                             password=password)},
                  contexts={'default': dict(datacenterName=data_center, authInfoName='default')},
                  currentContext='default')

    with open(os.path.join(ssl_dir, 'config_data.yaml'), 'w') as config_file:
        config_file.write(yaml.safe_dump(config, sort_keys=False))

    config = dict(datacenters={data_center: dict(certificateAuthorityPath=os.path.join(ssl_dir, 'ccm_node.cer'),
                                                 server=f'{address}:{port}',
                                                 nodeDomain='cql.cluster-id.scylla.com',
                                                 insecureSkipTlsVerify=False)},
                  authInfos={'default': dict(clientCertificatePath=os.path.join(ssl_dir, 'ccm_node.cer'),
                                             clientKeyPath=os.path.join(ssl_dir, 'ccm_node.key'),
                                             username=username,
                                             password=password)},
                  contexts={'default': dict(datacenterName=data_center, authInfoName='default')},
                  currentContext='default')

    with open(os.path.join(ssl_dir, 'config_path.yaml'), 'w') as config_file:
        config_file.write(yaml.safe_dump(config, sort_keys=False))

    return os.path.join(ssl_dir, 'config_data.yaml'), os.path.join(ssl_dir, 'config_path.yaml')


def reload_sni_proxy(docker_id):
    subprocess.check_output(['/bin/bash', '-c', f'docker kill --signal=SIGHUP {docker_id}'])


def stop_sni_proxy(docker_id):
    subprocess.check_output(['/bin/bash', '-c', f'docker rm -f {docker_id}'])


def configure_sni_proxy(conf_dir, nodes_info, listen_port=443):
    sniproxy_conf_tmpl = dedent("""
        user sniproxy
        pidfile /var/run/sniproxy/sniproxy.pid

        listener $FIRST_ADDRESS $listen_port {
          proto tls
        }

        table {
        $TABLES
        }
        """)
    tables = ""
    mapping = {}
    address, port, host_id, _ = list(nodes_info)[0]
    tables += f"  ^cql.cluster-id.scylla.com$ {address}:{port}\n"
    mapping['FIRST_ADDRESS'] = address
    mapping['listen_port'] = listen_port

    for address, port, host_id, _ in nodes_info:
        tables += f"  ^{host_id}.cql.cluster-id.scylla.com$ {address}:{port}\n"

    tmpl = string.Template(sniproxy_conf_tmpl)
    sniproxy_conf_path = os.path.join(conf_dir, 'sniproxy.conf')

    with open(sniproxy_conf_path, 'w') as fp:
        fp.write(tmpl.substitute(TABLES=tables, **mapping))

    return sniproxy_conf_path


def start_sni_proxy(conf_dir, nodes_info, listen_port=443):
    address, _, _, _ = list(nodes_info)[0]
    sniproxy_conf_path = configure_sni_proxy(conf_dir, nodes_info, listen_port=listen_port)
    sniproxy_dockerfile = os.path.join(os.path.dirname(__file__), '..', 'resources', 'docker', 'sniproxy')
    subprocess.check_output(['/bin/bash', '-c', f'docker build {sniproxy_dockerfile} -t sniproxy'], universal_newlines=True)
    docker_id = subprocess.check_output(['/bin/bash', '-c', f'docker run -d --network=host -v {sniproxy_conf_path}:/etc/sniproxy.conf:z -p {listen_port} -it sniproxy'], universal_newlines=True)

    return docker_id.strip(), address, listen_port


def get_cluster_info(cluster, port=9142):

    node1 = cluster.nodelist()[0]
    stdout, stderr = node1.run_cqlsh(cmds='select JSON host_id,broadcast_address,data_center from system.local ;',
                                     return_output=True, show_output=True)

    nodes_info = []
    for line in stdout.splitlines():
        try:
            host = json.loads(line)
        except json.decoder.JSONDecodeError:
            continue
        if 'broadcast_address' in host and 'host_id' in host:
            nodes_info.append((host['broadcast_address'], port, host['host_id'], host['data_center']))

    stdout, stderr = node1.run_cqlsh(cmds='select JSON peer,host_id,data_center from system.peers ;',
                                     return_output=True, show_output=True)

    for line in stdout.splitlines():
        try:
            host = json.loads(line)
        except json.decoder.JSONDecodeError:
            continue
        if 'peer' in host and 'host_id' in host:
            nodes_info.append((host['peer'], port, host['host_id'], host['data_center']))

    return nodes_info


def refresh_certs(cluster, nodes_info):
    with tempfile.TemporaryDirectory() as tmp_dir:
        dns_names = ['cql.cluster-id.scylla.com'] + \
                    ['{}.cql.cluster-id.scylla.com'.format(host_id) for _, _, host_id, _ in nodes_info]
        generate_ssl_stores(tmp_dir, dns_names=dns_names)
        distutils.dir_util.copy_tree(tmp_dir, cluster.get_path())


if __name__ == "__main__":
    from ccmlib.cmds.command import Cmd
    from ccmlib import common

    a = Cmd()
    a.path = common.get_default_path()
    a.cluster = a._load_current_cluster()
    nodes_info = get_cluster_info(a.cluster)
    conf_dir = a.cluster.get_path()
    docker_id, host, port = start_sni_proxy(conf_dir=conf_dir, nodes_info=nodes_info)
    print(create_cloud_config(conf_dir, host, port, nodes_info))
    stop_sni_proxy(docker_id)
