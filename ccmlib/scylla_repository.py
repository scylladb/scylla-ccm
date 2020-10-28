from __future__ import with_statement

from typing import NamedTuple

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

import packaging.version

from ccmlib.common import (
    ArgumentError, get_default_path, rmdirs, validate_install_dir, get_scylla_version, aws_bucket_ls)
from ccmlib.repository import __download

GIT_REPO = "http://github.com/scylladb/scylla.git"

RELOCATABLE_URLS_BASE = 'https://s3.amazonaws.com/downloads.scylladb.com/relocatable/{0}/{1}'
RELEASE_RELOCATABLE_URLS_BASE = 'https://s3.amazonaws.com/downloads.scylladb.com/downloads/scylla/relocatable/scylladb-{0}'
ENTERPRISE_RELEASE_RELOCATABLE_URLS_BASE = 'https://s3.amazonaws.com/downloads.scylladb.com/downloads/scylla-enterprise/relocatable/scylladb-{0}'
ENTERPRISE_RELOCATABLE_URLS_BASE = 'https://s3.amazonaws.com/downloads.scylladb.com/enterprise/relocatable/{0}/{1}'


def run(cmd, cwd=None):
    subprocess.check_call(['bash', '-c', cmd], cwd=cwd,
                          stderr=None, stdout=None)


class RelocatablePackages(NamedTuple):
    scylla_package: str
    scylla_tools_package: str
    scylla_jmx_package: str


def release_packages(s3_url, version):
    """
    Choose RELEASE relocatable packages for download.
    It covers 3 cases:
    1. Choose release packages for supplied version. If scylla_version includes minor version (like "4.3.1"),
       relocatable packages for this version will be selected.
    2. Choose release packages for latest release version. If scylla_version doesn't include minor version (like "4.3),
       relocatables for latest released version will be selected.
    3. If the version was not released yet, relocatables for latest release candidate (rc0..) will be selected.

    :param s3_url:
    :param version:
    :return:
    """
    major_version = version if version.count('.') == 1 else '.'.join(n for n in version.split('.')[:2])
    s3_url = s3_url.format(major_version)

    files_in_bucket = aws_bucket_ls(s3_url)

    if not files_in_bucket:
        raise RuntimeError(
            f"Failed to get release packages list for version {major_version}. URL: {s3_url}.")

    all_packages = [name for name in files_in_bucket if 'jmx' in name or 'tools' in name or 'scylla-package' in name]

    # Search for released version packages
    candidates = [package for package in all_packages if ".rc" not in package]

    if candidates:
        # If found packages for released version
        if version.count('.') == 1:
            # Choose relocatable packages for latest release version
            a = re.findall(f'-{version}\.(\d+)-', ','.join(candidates))
            latest_candidate = f"{version}.{max(set(a))}" if a else ''
        elif version.count('.') == 2:
             # Choose relocatable packages for supplied version
            latest_candidate = version
        else:
            raise ValueError(f"Not expected version number: {version}. S3_URL: {s3_url}")

        release_packages = [package for package in candidates if f"-{latest_candidate}-" in package]
    else:
        # Choose relocatables for latest release candidate
        a = re.findall('\.rc(\d+)-', ','.join(all_packages))
        latest_candidate = f"{version}.rc{max(set(a))}" if a else ''
        release_packages = [package for package in all_packages if f"{latest_candidate}-" in package]

    if not release_packages:
        raise ValueError(
            f"Release packages have not beed found.\nDebug info: all packages: {all_packages}; "
            f"candidates packages: {candidates}; last version: {latest_candidate}")

    packages = RelocatablePackages
    for package in release_packages:
        if 'jmx' in package:
            packages.scylla_jmx_package = os.path.join(s3_url, package)
        elif 'tool' in package:
            packages.scylla_tools_package = os.path.join(s3_url, package)
        else:
            packages.scylla_package = os.path.join(s3_url, package)

    return packages, latest_candidate

def setup(version, verbose=True):
    """
    :param version:
            Supported version values (examples):
            1. Unstable versions:
              - unstable/master:2020-12-20T00:11:59Z
              - unstable/enterprise:2020-08-18T14:49:18Z
              - unstable/branch-4.1:2020-05-30T08:27:59Z
            2. Official version (released):
              - release:4.3
              - release:4.2.1
              - release:2020.1 (SCYLLA_PRODUCT='enterprise')
              - release:2020.1.5 (SCYLLA_PRODUCT='enterprise')

    """
    s3_url = ''
    type_n_version = version.split(':', 1)
    scylla_product = os.environ.get('SCYLLA_PRODUCT', 'scylla')

    packages = None
    if len(type_n_version) == 2:
        s3_version = type_n_version[1]
        packages = None

        if type_n_version[0] == 'release':
            if 'enterprise' in scylla_product:
                s3_url = ENTERPRISE_RELEASE_RELOCATABLE_URLS_BASE
            else:
                s3_url = RELEASE_RELOCATABLE_URLS_BASE
            packages, type_n_version[1] = release_packages(s3_url=s3_url, version=s3_version)
        else:
            if 'enterprise' in scylla_product:
                s3_url = ENTERPRISE_RELOCATABLE_URLS_BASE.format(type_n_version[0], s3_version)
            else:
                s3_url = RELOCATABLE_URLS_BASE.format(type_n_version[0], s3_version)
            packages = RelocatablePackages(scylla_jmx_package=os.path.join(s3_url,
                                                                           f'{scylla_product}-jmx-package.tar.gz'),
                                           scylla_tools_package=os.path.join(s3_url,
                                                                             f'{scylla_product}-tools-package.tar.gz'),
                                           scylla_package=os.path.join(s3_url,
                                                                       f'{scylla_product}-package.tar.gz'))

        version = os.path.join(*type_n_version)

    cdir = version_directory(version)

    if cdir is None:
        if not packages and not s3_url:
            packages = RelocatablePackages(scylla_jmx_package=os.environ.get('SCYLLA_JMX_PACKAGE'),
                                           # Try the old name for backward compatibility
                                           scylla_tools_package=os.environ.get("SCYLLA_TOOLS_JAVA_PACKAGE") or
                                                                os.environ.get("SCYLLA_JAVA_TOOLS_PACKAGE"),
                                           scylla_package=os.environ.get('SCYLLA_CORE_PACKAGE')
                                           )

            if not packages:
                raise EnvironmentError("Not found environment parameters: 'SCYLLA_JMX_PACKAGE' and "
                                       "('SCYLLA_TOOLS_JAVA_PACKAGE' or 'SCYLLA_JAVA_TOOLS_PACKAGE) and"
                                       "'SCYLLA_CORE_PACKAGE'")

        tmp_download = tempfile.mkdtemp()

        package_version = download_version(version, verbose=verbose, url=packages.scylla_package, target_dir=os.path.join(
            tmp_download, 'scylla-core-package'))

        download_version(version, verbose=verbose, url=packages.scylla_tools_package, target_dir=os.path.join(
            tmp_download, 'scylla-tools-java'))

        download_version(version, verbose=verbose, url=packages.scylla_jmx_package,
                         target_dir=os.path.join(tmp_download, 'scylla-jmx'))

        cdir = directory_name(version)

        shutil.move(tmp_download, cdir)

        # install using scylla install.sh
        run_scylla_install_script(os.path.join(
            cdir, 'scylla-core-package'), cdir, package_version)

    scylla_ext_opts = os.environ.get('SCYLLA_EXT_OPTS', '')
    scylla_manager_package = os.environ.get('SCYLLA_MANAGER_PACKAGE')

    if not scylla_ext_opts:
        manager_install_dir = setup_scylla_manager(scylla_manager_package)
        scylla_ext_opts += ' --scylla-manager={}'.format(manager_install_dir)
        os.environ['SCYLLA_EXT_OPTS'] = scylla_ext_opts

    return cdir, get_scylla_version(cdir)


def setup_scylla_manager(scylla_manager_package=None):

    """
    download and cache scylla-manager RPMs,
    :return:
    """

    if scylla_manager_package and '--scylla-manager':
        m = hashlib.md5()
        m.update(scylla_manager_package.encode('utf-8'))

        # select a dir to change this version of scylla-manager based on the md5 of the path
        manager_install_dir = directory_name(os.path.join('manager', m.hexdigest()))
        if not os.path.exists(manager_install_dir):
            os.makedirs(manager_install_dir)

            # download and traverse the repo data, for getting the url for the RPMs
            url = os.path.join(scylla_manager_package, 'repodata/repomd.xml')
            page = requests.get(url).text

            primary_regex = re.compile(r'="(.*?primary.xml.gz)"')
            primary_path = primary_regex.search(page).groups()[0]

            url = os.path.join(scylla_manager_package, primary_path)
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
                url = os.path.join(scylla_manager_package, rpm_file)
                rpm_data = requests.get(url).content

                p = Popen(['bash', '-c', 'rpm2cpio - | cpio -id'],
                          stdout=PIPE, stdin=PIPE, stderr=STDOUT, cwd=manager_install_dir)
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
                cwd=manager_install_dir)

        return manager_install_dir


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
                package_version = packaging.version.parse(f.read().strip())
            if package_version > packaging.version.parse('2.1'):
                print(f'Unknown relocatable package format version: {package_version}')
                sys.exit(1)
            print(f'Relocatable package format version {package_version} detected.')
            pkg_dir = glob.glob('{}/*/'.format(target_dir))[0]
            shutil.move(str(pkg_dir), target_dir + '.new')
            shutil.rmtree(target_dir)
            shutil.move(target_dir + '.new', target_dir)
        else:
            package_version = packaging.version.parse('1')
            print('Legacy relocatable package format detected.')

        # add breadcrumb so we could list the origin of each part easily for debugging
        # for example listing all the version we have in ccm scylla-repository
        # find  ~/.ccm/scylla-repository/*/ -iname source.txt | xargs cat
        source_breadcrumb_file = os.path.join(target_dir, 'source.txt')
        with open(source_breadcrumb_file, 'w') as f:
            f.write("version=%s\n" % version)
            f.write("url=%s\n" % url)

        return package_version
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


def run_scylla_install_script(install_dir, target_dir, package_version):
    scylla_target_dir = os.path.join(target_dir, 'scylla')

    if package_version <= packaging.version.parse('2'):
        # leave this for compatibility
        run('''sed -i '/systemctl --user/d' install.sh''', cwd=install_dir)
        install_opt = ''
    else:
        # on relocatable package format 2.1 or later, use --packaging instead of sed
        install_opt = ' --packaging'

    run('''{0}/install.sh --prefix {1} --nonroot{2}'''.format(
        install_dir, scylla_target_dir, install_opt), cwd=install_dir)
    run('''mkdir -p {0}/conf; cp ./conf/scylla.yaml {0}/conf'''.format(
        scylla_target_dir), cwd=install_dir)
    run('''ln -s {}/bin .'''.format(scylla_target_dir), cwd=target_dir)
    run('''ln -s {}/conf .'''.format(scylla_target_dir), cwd=target_dir)
