from __future__ import with_statement

import os
import tarfile
import tempfile
import shutil
import subprocess
import re
import gzip
import sys
import glob

from subprocess import Popen, PIPE, STDOUT

import hashlib
import requests

from six import print_
from six.moves import urllib
from six import BytesIO

from ccmlib.common import (
    ArgumentError, get_default_path, rmdirs, validate_install_dir, get_scylla_version)
from ccmlib.repository import __download

GIT_REPO = "http://github.com/scylladb/scylla.git"

RELOCATABLE_URLS_BASE = 'https://s3.amazonaws.com/downloads.scylladb.com/relocatable/{0}/{1}'


def run(cmd, cwd=None):
    subprocess.check_call(['bash', '-c', cmd], cwd=cwd,
                          stderr=None, stdout=None)


def setup(version, verbose=True):
    s3_url = ''
    type_n_version = version.split(':', 1)
    if len(type_n_version) == 2:
        s3_version = type_n_version[1]
        s3_url = RELOCATABLE_URLS_BASE.format(type_n_version[0], s3_version)
        version = os.path.join(*type_n_version)

    cdir = version_directory(version)

    if cdir is None:
        tmp_download = tempfile.mkdtemp()

        url = os.environ.get('SCYLLA_CORE_PACKAGE', os.path.join(
            s3_url, 'scylla-package.tar.gz'))
        download_version(version, verbose=verbose, url=url, target_dir=os.path.join(
            tmp_download, 'scylla-core-package'))
        # Try the old name for backward compatibility
        url = os.environ.get("SCYLLA_TOOLS_JAVA_PACKAGE") or \
            os.environ.get("SCYLLA_JAVA_TOOLS_PACKAGE") or \
            os.path.join(s3_url, 'scylla-tools-package.tar.gz')

        download_version(version, verbose=verbose, url=url, target_dir=os.path.join(
            tmp_download, 'scylla-tools-java'))

        url = os.environ.get('SCYLLA_JMX_PACKAGE', os.path.join(
            s3_url, 'scylla-jmx-package.tar.gz'))
        download_version(version, verbose=verbose, url=url,
                         target_dir=os.path.join(tmp_download, 'scylla-jmx'))

        cdir = directory_name(version)

        shutil.move(tmp_download, cdir)

        # install using scylla install.sh
        run_scylla_install_script(os.path.join(
            cdir, 'scylla-core-package'), cdir)
    setup_scylla_manager()

    return cdir, get_scylla_version(cdir)


def setup_scylla_manager():

    """
    download and cache scylla-manager RPMs,
    :return:
    """
    base_url = os.environ.get('SCYLLA_MANAGER_PACKAGE', None)
    scylla_ext_opts = os.environ.get('SCYLLA_EXT_OPTS', '')

    if base_url and '--scylla-manager' not in scylla_ext_opts:
        m = hashlib.md5()
        m.update(base_url.encode('utf-8'))

        # select a dir to change this version of scylla-manager based on the md5 of the path
        install_dir = directory_name(os.path.join('manager', m.hexdigest()))
        if not os.path.exists(install_dir):
            os.makedirs(install_dir)

            # download and traverse the repo data, for getting the url for the RPMs
            url = os.path.join(base_url, 'repodata/repomd.xml')
            page = requests.get(url).text

            primary_regex = re.compile(r'="(.*?primary.xml.gz)"')
            primary_path = primary_regex.search(page).groups()[0]

            url = os.path.join(base_url, primary_path)
            data = requests.get(url).content

            # unzip the repo primary listing
            zf = gzip.GzipFile(fileobj=BytesIO(data))
            data = zf.read()

            files_to_download = []
            for rpm_file in ['scylla-manager-client', 'scylla-manager-server', 'scylla-manager-agent']:
                try:
                    f_regex = re.compile(
                        r'="({}.*?x86_64.rpm)"'.format(rpm_file))
                    f_rpm = f_regex.search(data.decode('utf-8')).groups()[0]
                    files_to_download.append(f_rpm)
                except Exception:
                    pass

            # download the RPMs and extract them into place
            for rpm_file in files_to_download:
                url = os.path.join(base_url, rpm_file)
                rpm_data = requests.get(url).content

                p = Popen(['bash', '-c', 'rpm2cpio - | cpio -id'],
                          stdout=PIPE, stdin=PIPE, stderr=STDOUT, cwd=install_dir)
                grep_stdout = p.communicate(input=rpm_data)[0]
                print_(grep_stdout.decode())

            # TODO: remove this, and make the code the correct paths directly
            # final touch to align the files structure to how it in mermaid repo
            run('''
                    cp ./usr/bin/sctool .
                    cp ./usr/bin/scylla-manager .
                    
                    mkdir -p dist/etc
                    mkdir schema
                    cp -r ./etc/scylla-manager/* ./dist/etc/
                    cp -r ./etc/scylla-manager/cql ./schema/
                ''',
                cwd=install_dir)

        scylla_ext_opts += ' --scylla-manager={}'.format(install_dir)
        os.environ['SCYLLA_EXT_OPTS'] = scylla_ext_opts


min_attributes = ('scheme', 'netloc')


def is_valid(url, qualifying=None):
    qualifying = min_attributes if qualifying is None else qualifying
    token = urllib.parse.urlparse(url)
    return all([getattr(token, qualifying_attr)
                for qualifying_attr in qualifying])


def download_version(version, url=None, verbose=False, target_dir=None):
    """
    Download, scylla relocatable package tarballs.
    """
    try:
        if os.path.exists(url) and url.endswith('.tar.gz'):
            target = url
        elif is_valid(url):
            _, target = tempfile.mkstemp(suffix=".tar.gz", prefix="ccm-")
            __download(url, target, show_progress=verbose)
        else:
            raise ArgumentError(
                "unsupported url or file doesn't exist\n\turl={}".format(url))

        if verbose:
            print_("Extracting %s as version %s ..." % (target, version))
        tar = tarfile.open(target)
        tar.extractall(path=target_dir)
        tar.close()

        # if relocatable package format >= 2, need to extract files under subdir
        package_version_file = "{}/.relocatable_package_version".format(target_dir)
        if os.path.exists(package_version_file):
            with open(package_version_file) as f:
                package_version = f.read().strip()
            if package_version != '2':
                print('Unknown relocatable package format version: ' + package_version)
                sys.exit(1)
            print('Relocatable package format version 2 detected.')
            pkg_dir = glob.glob('{}/*/'.format(target_dir))[0]
            shutil.move(str(pkg_dir), target_dir + '.new')
            shutil.rmtree(target_dir)
            shutil.move(target_dir + '.new', target_dir)
        else:
            print('Legacy relocatable package format detected.')

        # add breadcrumb so we could list the origin of each part easily for debugging
        # for example listing all the version we have in ccm scylla-repository
        # find  ~/.ccm/scylla-repository/*/ -iname source.txt | xargs cat
        source_breadcrumb_file = os.path.join(target_dir, 'source.txt')
        with open(source_breadcrumb_file, 'w') as f:
            f.write("version=%s\n" % version)
            f.write("url=%s\n" % url)

    except urllib.error.URLError as e:
        msg = "Invalid version %s" % version if url is None else "Invalid url %s" % url
        msg = msg + " (underlying error is: %s)" % str(e)
        raise ArgumentError(msg)
    except tarfile.ReadError as e:
        raise ArgumentError(
            "Unable to uncompress downloaded file: %s" % str(e))


def directory_name(version):
    return os.path.join(__get_dir(), version).replace(':', '_')


def version_directory(version):
    dir = directory_name(version)
    if os.path.exists(dir):
        try:
            validate_install_dir(dir)
            return dir
        except ArgumentError:
            rmdirs(dir)
            return None
    else:
        return None


def __get_dir():
    repo = os.path.join(get_default_path(), 'scylla-repository')
    if not os.path.exists(repo):
        os.mkdir(repo)
    return repo


def run_scylla_install_script(install_dir, target_dir):
    scylla_target_dir = os.path.join(target_dir, 'scylla')

    # FIXME: remove this hack once scylladb/scylla#4949 is fixed and merged
    run('''sed 's|"$prefix|"$root/$prefix|' -i install.sh''', cwd=install_dir)

    # FIXME: remove systemctl command from install.sh, since they won't work inside docker, and they are not needed
    run('''sed -i '/systemctl --user/d' install.sh''', cwd=install_dir)

    run('''{0}/install.sh --root {1} --prefix {1} --prefix /opt/scylladb --nonroot'''.format(
        install_dir, scylla_target_dir), cwd=install_dir)
    run('''mkdir -p {0}/conf; cp ./conf/scylla.yaml {0}/conf'''.format(
        scylla_target_dir), cwd=install_dir)
    run('''ln -s {}/opt/scylladb/bin .'''.format(scylla_target_dir), cwd=target_dir)
    run('''ln -s {}/conf .'''.format(scylla_target_dir), cwd=target_dir)
