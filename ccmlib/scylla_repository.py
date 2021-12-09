from __future__ import with_statement

import random
import time
from pathlib import Path
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
    ArgumentError, CCMError, get_default_path, rmdirs, validate_install_dir, get_scylla_version, aws_bucket_ls,
    DOWNLOAD_IN_PROGRESS_FILE, wait_for_parallel_download_finish)
from ccmlib.utils.download import download_file, download_version_from_s3

GIT_REPO = "http://github.com/scylladb/scylla.git"

CORE_PACKAGE_DIR_NAME = 'scylla-core-package'
SCYLLA_VERSION_FILE = 'SCYLLA-VERSION-FILE'

RELOCATABLE_URLS_BASE = ['https://s3.amazonaws.com/downloads.scylladb.com/unstable/scylla/{0}/relocatable/{1}',
                         'https://s3.amazonaws.com/downloads.scylladb.com/relocatable/unstable/{0}/{1}']
RELEASE_RELOCATABLE_URLS_BASE = 'https://s3.amazonaws.com/downloads.scylladb.com/downloads/scylla/relocatable/scylladb-{0}'
ENTERPRISE_RELEASE_RELOCATABLE_URLS_BASE = 'https://s3.amazonaws.com/downloads.scylladb.com/downloads/scylla-enterprise/relocatable/scylladb-{0}'
ENTERPRISE_RELOCATABLE_URLS_BASE = ['https://s3.amazonaws.com/downloads.scylladb.com/unstable/scylla-enterprise/{0}/relocatable/{1}',
                                    'https://s3.amazonaws.com/downloads.scylladb.com/enterprise/relocatable/unstable/{0}/{1}']


def run(cmd, cwd=None):
    subprocess.check_call(['bash', '-c', cmd], cwd=cwd,
                          stderr=None, stdout=None)


class RelocatablePackages(NamedTuple):
    scylla_package: str
    scylla_tools_package: str
    scylla_jmx_package: str


def release_packages(s3_url, version, arch='x86_64'):
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

    scylla_package_mark = 'scylla-package'

    for file_name in files_in_bucket:
        if f'scylla-{arch}-package' in file_name:
            scylla_package_mark = f'scylla-{arch}-package'
            break

    all_packages = [name for name in files_in_bucket if 'jmx' in name or 'tools' in name or scylla_package_mark in name]

    candidates = []
    # Search for released version packages
    if '.rc' not in version:
        candidates = [package for package in all_packages if ".rc" not in package]

    if candidates:
        # If found packages for released version
        if version.count('.') == 1:
            # Choose relocatable packages for latest release version
            a = re.findall(f'-{version}\.(\d+)(?:-|\.)', ','.join(candidates))
            latest_candidate = f"{version}.{max(set(a))}" if a else ''
        elif version.count('.') == 2:
             # Choose relocatable packages for supplied version
            latest_candidate = version
        else:
            raise ValueError(f"Not expected version number: {version}. S3_URL: {s3_url}")

        release_packages = [package for package in candidates if f"-{latest_candidate}" in package]
    else:
        # Choose relocatables for latest release candidate
        if '.rc' in version:
            latest_candidate = version
        else:
            a = re.findall('\.rc(\d+)(?:-|\.)', ','.join(all_packages))
            latest_candidate = f"{version}.rc{max(set(a))}" if a else ""

        release_packages = [package for package in all_packages if latest_candidate in package]

    if not release_packages:
        raise ValueError(
            f"Release packages have not beed found.\nDebug info: all packages: {all_packages}; "
            f"candidates packages: {candidates}; last version: {latest_candidate}")

    release_packages_dict = {}
    for package in release_packages:
        # Expected packages names (examples):
        #  'scylla-jmx-package-4.3.0-0.20210110.000585522.tar.gz'
        #  'scylla-package-4.3.0-0.20210110.000585522.tar.gz' or
        #   'scylla-x86_64-package-4.6.0-0.20211010.000585522.tar.gz'
        #  'scylla-tools-package-4.3.0-0.20210110.000585522.tar.gz'
        for package_type in ['jmx', 'tools', scylla_package_mark]:
            if package_type in package:
                release_packages_dict[package_type] = package

    packages = RelocatablePackages(scylla_package=os.path.join(s3_url, release_packages_dict[scylla_package_mark]),
                                   scylla_jmx_package=os.path.join(s3_url, release_packages_dict['jmx']),
                                   scylla_tools_package=os.path.join(s3_url, release_packages_dict['tools']))

    return packages, latest_candidate


def get_relocatable_s3_url(branch, s3_version, links):
    for reloc_url in links:
        s3_url = reloc_url.format(branch, s3_version)
        resp = aws_bucket_ls(s3_url)
        if resp:
            return s3_url
    raise CCMError(f"s3 url was not found for {branch}:{s3_version}")


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
    scylla_arch = os.environ.get('SCYLLA_ARCH', 'x86_64')

    packages = None
    if len(type_n_version) == 2:
        s3_version = type_n_version[1]
        packages = None

        if type_n_version[0] == 'release':
            if 'enterprise' in scylla_product:
                s3_url = ENTERPRISE_RELEASE_RELOCATABLE_URLS_BASE
            else:
                s3_url = RELEASE_RELOCATABLE_URLS_BASE
            packages, type_n_version[1] = release_packages(s3_url=s3_url, version=s3_version, arch=scylla_arch)
        else:
            _, branch = type_n_version[0].split("/")
            if 'enterprise' in scylla_product:
                s3_url = get_relocatable_s3_url(branch, s3_version, ENTERPRISE_RELOCATABLE_URLS_BASE)
            else:
                s3_url = get_relocatable_s3_url(branch, s3_version, RELOCATABLE_URLS_BASE)

            scylla_arch_package_path = f'{scylla_product}-{scylla_arch}-package.tar.gz'
            scylla_noarch_package_path = f'{scylla_product}-package.tar.gz'

            aws_files = aws_bucket_ls(s3_url)

            if scylla_arch_package_path in aws_files:
                scylla_package_path = scylla_arch_package_path
            elif scylla_noarch_package_path in aws_files:
                scylla_package_path = scylla_noarch_package_path
            else:
                raise RuntimeError("Can't find %s or %s in the %s",
                                   scylla_arch_package_path, scylla_noarch_package_path, s3_url)

            packages = RelocatablePackages(
                scylla_jmx_package=os.path.join(s3_url, f'{scylla_product}-jmx-package.tar.gz'),
                scylla_tools_package=os.path.join(s3_url, f'{scylla_product}-tools-package.tar.gz'),
                scylla_package=os.path.join(s3_url, scylla_package_path)
            )

        version = os.path.join(*type_n_version)

    version_dir = version_directory(version)

    if version_dir is None:
        # Create version folder and add placeholder file to prevent parallel downloading from another test.
        version_dir = directory_name(version)
        download_in_progress_file = Path(version_dir) / DOWNLOAD_IN_PROGRESS_FILE

        # Give a chance not to start few downloads in the exactly same second
        time.sleep(random.randint(0, 5))

        # If another parallel downloading has been started already, wait while it will be completed
        if download_in_progress_file.exists():
            print(f"Another download running into '{version_dir}'. Waiting for parallel downloading finished")
            wait_for_parallel_download_finish(placeholder_file=download_in_progress_file.absolute())
        else:
            try:
                os.makedirs(version_dir)
            except FileExistsError as exc:
                # If parallel process created the folder first, let to the parallel download to finish
                print(f"Another download running into '{version_dir}'. Waiting for parallel downloading finished")
                wait_for_parallel_download_finish(placeholder_file=download_in_progress_file.absolute())
            else:
                download_in_progress_file.touch()
                try:
                    package_version = download_packages(version_dir=version_dir, packages=packages, s3_url=s3_url,
                                                        scylla_product=scylla_product, version=version, verbose=verbose)
                except requests.HTTPError as err:
                    if '404' in err.args[0]:
                        packages_x86_64 = RelocatablePackages(
                            scylla_jmx_package=os.path.join(s3_url, f'{scylla_product}-jmx-package.tar.gz'),
                            scylla_tools_package=os.path.join(s3_url, f'{scylla_product}-tools-package.tar.gz'),
                            scylla_package=os.path.join(s3_url, f'{scylla_product}-x86_64-package.tar.gz')
                        )
                        package_version = download_packages(version_dir=version_dir, packages=packages_x86_64,
                                                            s3_url=s3_url, scylla_product=scylla_product,
                                                            version=version, verbose=verbose)
                    else:
                        raise

                download_in_progress_file.touch()

                # install using scylla install.sh
                run_scylla_install_script(install_dir=os.path.join(version_dir, CORE_PACKAGE_DIR_NAME),
                                          target_dir=version_dir,
                                          package_version=package_version)
                print(f"Completed to install Scylla in the folder '{version_dir}'")
                download_in_progress_file.unlink()

    scylla_ext_opts = os.environ.get('SCYLLA_EXT_OPTS', '')
    scylla_manager_package = os.environ.get('SCYLLA_MANAGER_PACKAGE')
    if scylla_manager_package:
        manager_install_dir = setup_scylla_manager(scylla_manager_package)
        scylla_ext_opts += ' --scylla-manager={}'.format(manager_install_dir)
        os.environ['SCYLLA_EXT_OPTS'] = scylla_ext_opts

    return version_dir, get_scylla_version(version_dir)


def download_packages(version_dir, packages, s3_url, scylla_product, version, verbose):
    if not packages and not s3_url:
        packages = RelocatablePackages(scylla_jmx_package=os.environ.get('SCYLLA_JMX_PACKAGE'),
                                       scylla_tools_package=os.environ.get("SCYLLA_TOOLS_JAVA_PACKAGE") or
                                                            os.environ.get("SCYLLA_JAVA_TOOLS_PACKAGE"),
                                       scylla_package=os.environ.get('SCYLLA_CORE_PACKAGE')
                                       )

        if not packages:
            raise EnvironmentError("Not found environment parameters: 'SCYLLA_JMX_PACKAGE' and "
                                   "('SCYLLA_TOOLS_JAVA_PACKAGE' or 'SCYLLA_JAVA_TOOLS_PACKAGE) and"
                                   "'SCYLLA_CORE_PACKAGE'")

    tmp_download = tempfile.mkdtemp()

    package_version = download_version(version=version, verbose=verbose, url=packages.scylla_package,
                                       target_dir=os.path.join(tmp_download, 'scylla-core-package'))

    download_version(version=version, verbose=verbose, url=packages.scylla_tools_package,
                     target_dir=os.path.join(tmp_download, 'scylla-tools-java'))

    download_version(version=version, verbose=verbose, url=packages.scylla_jmx_package,
                     target_dir=os.path.join(tmp_download, 'scylla-jmx'))

    shutil.rmtree(version_dir)
    shutil.move(tmp_download, version_dir)

    return package_version


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
            tar_data = requests.get(scylla_manager_package, stream=True)
            destination_file = os.path.join(manager_install_dir, "manager.tar.gz")
            with open(destination_file, mode="wb") as f:
                f.write(tar_data.raw.read())
            run(f"""
                tar -xvf {destination_file} -C {manager_install_dir}
                rm -f {destination_file}
                """)

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
            res = download_version_from_s3(url=url, target_path=target,verbose=verbose)
            if not res:
                download_file(url=url, target_path=target,verbose=verbose)
        else:
            raise ArgumentError(
                "unsupported url or file doesn't exist\n\turl={}".format(url))

        if verbose:
            print_(f"Extracting {target} ({url}, {target_dir}) as version {version} ...")
        tar = tarfile.open(target)
        tar.extractall(path=target_dir)
        tar.close()

        # if relocatable package format >= 2, need to extract files under subdir
        package_version_file = "{}/.relocatable_package_version".format(target_dir)
        if os.path.exists(package_version_file):
            with open(package_version_file) as f:
                package_version = packaging.version.parse(f.read().strip())
            if package_version > packaging.version.parse('3'):
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
