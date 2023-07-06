import os
import tarfile
import tempfile
import shutil
import subprocess
import re
import sys
import glob
import urllib
import logging
import random
import time
from pathlib import Path
from typing import NamedTuple, Literal

import requests
import yaml
import packaging.version

from ccmlib.common import (
    ArgumentError, CCMError, get_default_path, rmdirs, validate_install_dir, get_scylla_version, aws_bucket_ls,
    DOWNLOAD_IN_PROGRESS_FILE, print_if_standalone, LockFile)
from ccmlib.utils.download import download_file, download_version_from_s3, get_url_hash
from ccmlib.utils.version import parse_version

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
    try:
        subprocess.run(['bash', '-c', cmd], cwd=cwd, check=True, capture_output=True)
    except subprocess.CalledProcessError as exp:
        print_if_standalone(str(exp), debug_callback=logging.error)
        print_if_standalone("stdout:\n%s" % exp.stdout, debug_callback=logging.error)
        print_if_standalone("stderr:\n%s" % exp.stderr, debug_callback=logging.error)
        raise


class RelocatablePackages(NamedTuple):
    scylla_package: str = None
    scylla_tools_package: str = None
    scylla_jmx_package: str = None
    scylla_unified_package: str = None


def release_packages(s3_url, version, arch='x86_64', scylla_product='scylla'):
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

    major_version = extract_major_version(version)
    s3_url = s3_url.format(major_version)
    
    files_in_bucket = aws_bucket_ls(s3_url)

    if not files_in_bucket:
        raise RuntimeError(
            f"Failed to get release packages list for version {major_version}. URL: {s3_url}.")
    # examples of unified packages file names:
    # 'scylla-x86_64-unified-package-5.1.2-0.20221225.4c0f7ea09893-5.1.2.0.20221225.4c0f7ea09893.tar.gz'
    # 'scylla-x86_64-unified-package-5.1.3-0.20230112.addc4666d502-5.1.3.0.20230112.addc4666d502.tar.gz'
    all_unified_packages = [name for name in files_in_bucket if f'{scylla_product}-{arch}-unified-package' in name or
                            (f'{scylla_product}-unified' in name and arch in name) or
                            f'{scylla_product}-unified' in name]
    release_unified_packages = [package for package in all_unified_packages if version in package]

    def extract_version(filename):
        """
        extract the version out of a unified package filename
        from 'scylla-x86_64-unified-package-5.1.0~rc0-0.20220810.86a6c1fb2b79-5.1.0-rc0.0.20220810.86a6c1fb2b79.tar.gz'
        would extract '5.1.0~rc'
        from 'scylla-x86_64-unified-package-5.1.3-0.20230112.addc4666d502-5.1.3.0.20230112.addc4666d502.tar.gz'
        would extract '5.1.3'
        """
        version_regex = re.compile(r'-(\d+!)?\d+([.-]\d+)?([.-]\d+)?([a-z]+\d*)?([.~-](post|dev|rc)\d*)*')
        match = version_regex.search(filename)
        if match:
            return filename[match.span()[0]+1:match.span()[1]]
        logging.warning(f"file: {filename}, doesn't have version in it...")
        return '0.0'

    if release_unified_packages:
        version_and_files = [(extract_version(release), release) for release in release_unified_packages]
        version_and_files.sort(key=lambda x:  parse_version(x[0]))
        packages = RelocatablePackages(scylla_unified_package=os.path.join(s3_url, version_and_files[-1][1]))

        return packages, version_and_files[-1][0]

    scylla_package_mark = 'scylla-package'

    for file_name in files_in_bucket:
        if f'{scylla_product}-{arch}-package' in file_name:
            scylla_package_mark = f'{scylla_product}-{arch}-package'
            break
        elif f'{arch}.tar.gz' in file_name:
            scylla_package_mark = list(filter(re.compile(f'{scylla_product}-[0-9].*.{arch}.tar.gz').match, files_in_bucket))[0]
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

    if not release_packages or ( {'jmx', 'tools', scylla_package_mark} != set(release_packages_dict.keys())):
        raise ValueError(
            f"Release packages have not been found.\nDebug info: all packages: {all_packages}; "
            f"candidates packages: {candidates}; last version: {latest_candidate}")

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


def read_build_manifest(url):
    build_url = f'{url}/00-Build.txt'
    res = requests.get(build_url)
    res.raise_for_status()
    # example of 00-Build.txt content: (each line is formatted as 'key: value`)
    #
    #    url-id: 2022-08-29T08:05:34Z
    #    docker-image-name: scylla-nightly:5.2.0-dev-0.20220829.67c91e8bcd61
    return yaml.safe_load(res.content)


def normalize_scylla_version(version):
    """
    take 2020.2.rc3 or 2020.2.0.rc4 or 2020.2.0~rc5
    and normalize them in to a semver base version 2020.2.0~rc5
    """

    # since 5.0/2022.1 version change from x.x.rc1 to x.x.0~rc1 to be semver compliant
    major = extract_major_version(version.split('/')[1])
    if parse_version(major) <= parse_version('5.0') or \
            parse_version('2018') < parse_version(major) <= parse_version('2022.1'):
        version = version.replace('-rc', '.rc').replace('~rc', '.rc')
        return version

    version = version.replace('-', '~').replace('.rc', '~rc')
    version = version if re.match(r'.*\d*.\d*.0~rc', version) else version.replace('~rc', '.0~rc')

    return version


def extract_major_version(version):
    major_version = version if version.count('.') == 1 else '.'.join(n for n in version.split('.')[:2])
    major_version = major_version.split('~')[0]
    return major_version


def setup(version, verbose=True, skip_downloads=False):
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
              - release:2020.1
              - release:2020.1.5

    :param verbose: if True, print progress during download
    :param skip_downloads: if True, skips the actual download of files for testing purposes

    """
    s3_url = ''
    type_n_version = version.split(':', 1)
    scylla_product = os.environ.get('SCYLLA_PRODUCT', 'scylla')
    scylla_arch = os.environ.get('SCYLLA_ARCH', 'x86_64')

    packages = None

    # if version_dir exist skip downloading a version
    version = os.path.join(*type_n_version)
    if type_n_version[0] == 'release':
        version = normalize_scylla_version(version)
        type_n_version = version.split(os.path.sep, 1)
    version_dir = version_directory(version) if not skip_downloads else None

    if len(type_n_version) == 2 and version_dir is None:
        s3_version = type_n_version[1]

        if type_n_version[0] == 'release':
            scylla_product = 'scylla-enterprise' if parse_version(extract_major_version(s3_version)) > parse_version("2018.1") else 'scylla'
            scylla_product = os.environ.get('SCYLLA_PRODUCT', scylla_product)
            if 'enterprise' in scylla_product:
                s3_url = ENTERPRISE_RELEASE_RELOCATABLE_URLS_BASE
            else:
                s3_url = RELEASE_RELOCATABLE_URLS_BASE
            packages, type_n_version[1] = release_packages(s3_url=s3_url, version=s3_version, arch=scylla_arch, scylla_product=scylla_product)
        else:
            _, branch = type_n_version[0].split("/")
            if 'enterprise' in scylla_product:
                s3_url = get_relocatable_s3_url(branch, s3_version, ENTERPRISE_RELOCATABLE_URLS_BASE)
            else:
                s3_url = get_relocatable_s3_url(branch, s3_version, RELOCATABLE_URLS_BASE)

            try:
                build_manifest = read_build_manifest(s3_url)
                url = build_manifest.get(f'unified-pack-url-{scylla_arch}')
                assert url, "didn't found the url for unified package"
                if not url.startswith('s3'):
                    url = f's3.amazonaws.com/{url}'
                url = f'http://{url}'
                packages = RelocatablePackages(scylla_unified_package=url)
            except Exception as ex:
                logging.exception("could download relocatable")

                scylla_package_path = f'{scylla_product}-package.tar.gz'
                scylla_arch_package_path = f'{scylla_product}-{scylla_arch}-package.tar.gz'
                scylla_noarch_package_path = f'{scylla_product}-package.tar.gz'
                scylla_java_reloc = f'{scylla_product}-tools-package.tar.gz'
                scylla_jmx_reloc = f'{scylla_product}-jmx-package.tar.gz'

                aws_files = aws_bucket_ls(s3_url)

                if scylla_arch_package_path not in aws_files and scylla_package_path not in aws_files:
                    scylla_java_reloc = list(filter(re.compile(f'{scylla_product}-tools-[0-9].*.noarch.tar.gz').match, aws_files))[0]
                    scylla_jmx_reloc = list(filter(re.compile(f'{scylla_product}-jmx-[0-9].*.noarch.tar.gz').match, aws_files))[0]
                    scylla_arch_package_path = list(filter(re.compile(f'{scylla_product}-[0-9].*.{scylla_arch}.tar.gz').match, aws_files))[0]

                if scylla_arch_package_path in aws_files:
                    scylla_package_path = scylla_arch_package_path
                elif scylla_noarch_package_path in aws_files:
                    scylla_package_path = scylla_noarch_package_path
                elif scylla_package_path in aws_files:
                    scylla_package_path = scylla_package_path
                else:
                    raise RuntimeError("Can't find %s or %s in the %s",
                                       scylla_arch_package_path, scylla_noarch_package_path, s3_url)

                packages = RelocatablePackages(
                    scylla_jmx_package=os.path.join(s3_url, scylla_jmx_reloc),
                    scylla_tools_package=os.path.join(s3_url, scylla_java_reloc),
                    scylla_package=os.path.join(s3_url, scylla_package_path)
                )

        version = os.path.join(*type_n_version)

    if skip_downloads:
        return directory_name(version), packages

    if version_dir is None:
        # Create version folder and add placeholder file to prevent parallel downloading from another test.
        version_dir = directory_name(version)
        download_in_progress_file = Path(version_dir) / DOWNLOAD_IN_PROGRESS_FILE

        # Give a chance not to start few downloads in the exactly same second
        time.sleep(random.randint(0, 5))

        os.makedirs(version_dir, exist_ok=True)
        with LockFile(download_in_progress_file) as f:
            if f.read_status() != 'done':
                # First ensure that we are working on a clean directory
                # This prevents lockfile deletion by download_packages, as it doesn't have to clean the directory.
                for p in Path(version_dir).iterdir():
                    if p.name != DOWNLOAD_IN_PROGRESS_FILE:
                        shutil.rmtree(p)

                try:
                    package_version, packages = download_packages(version_dir=version_dir, packages=packages, s3_url=s3_url,
                                                                  scylla_product=scylla_product, version=version, verbose=verbose)
                except requests.HTTPError as err:
                    if '404' in err.args[0]:
                        packages_x86_64 = RelocatablePackages(
                            scylla_jmx_package=os.path.join(s3_url, f'{scylla_product}-jmx-package.tar.gz'),
                            scylla_tools_package=os.path.join(s3_url, f'{scylla_product}-tools-package.tar.gz'),
                            scylla_package=os.path.join(s3_url, f'{scylla_product}-x86_64-package.tar.gz')
                        )
                        package_version, packages = download_packages(version_dir=version_dir, packages=packages_x86_64,
                                                                      s3_url=s3_url, scylla_product=scylla_product,
                                                                      version=version, verbose=verbose)
                    else:
                        raise

                args = dict(install_dir=os.path.join(version_dir, CORE_PACKAGE_DIR_NAME),
                            target_dir=version_dir,
                            package_version=package_version)

                # install using scylla install.sh
                if packages.scylla_unified_package:
                    run_scylla_unified_install_script(**args)
                else:
                    run_scylla_install_script(**args)
                print(f"Completed to install Scylla in the folder '{version_dir}'")
                f.write_status('done')

    scylla_ext_opts = os.environ.get('SCYLLA_EXT_OPTS', '')
    scylla_manager_package = os.environ.get('SCYLLA_MANAGER_PACKAGE')
    if scylla_manager_package:
        manager_install_dir = setup_scylla_manager(scylla_manager_package)
        scylla_ext_opts += f' --scylla-manager={manager_install_dir}'
        os.environ['SCYLLA_EXT_OPTS'] = scylla_ext_opts

    return version_dir, get_scylla_version(version_dir)


def download_packages(version_dir, packages, s3_url, scylla_product, version, verbose):
    if not packages and not s3_url:
        packages = RelocatablePackages(scylla_jmx_package=os.environ.get('SCYLLA_JMX_PACKAGE'),
                                       scylla_tools_package=os.environ.get("SCYLLA_TOOLS_JAVA_PACKAGE") or
                                                            os.environ.get("SCYLLA_JAVA_TOOLS_PACKAGE"),
                                       scylla_package=os.environ.get('SCYLLA_CORE_PACKAGE'),
                                       scylla_unified_package=os.environ.get('SCYLLA_UNIFIED_PACKAGE')
                                       )

        if not packages:
            raise EnvironmentError("Not found environment parameters: 'SCYLLA_JMX_PACKAGE' and "
                                   "('SCYLLA_TOOLS_JAVA_PACKAGE' or 'SCYLLA_JAVA_TOOLS_PACKAGE) and"
                                   "'SCYLLA_CORE_PACKAGE'")

    tmp_download = tempfile.mkdtemp()
    if packages.scylla_unified_package:
        package_version = download_version(version=version, verbose=verbose, url=packages.scylla_unified_package,
                                           target_dir=tmp_download, unified=True)
        target_dir = Path(version_dir) / CORE_PACKAGE_DIR_NAME
        target_dir.parent.mkdir(parents=False, exist_ok=True)
        shutil.move(tmp_download, target_dir)
    else:
        package_version = download_version(version=version, verbose=verbose, url=packages.scylla_package,
                                           target_dir=os.path.join(tmp_download, CORE_PACKAGE_DIR_NAME))

        download_version(version=version, verbose=verbose, url=packages.scylla_tools_package,
                         target_dir=os.path.join(tmp_download, 'scylla-tools-java'))

        download_version(version=version, verbose=verbose, url=packages.scylla_jmx_package,
                         target_dir=os.path.join(tmp_download, 'scylla-jmx'))

        shutil.move(tmp_download, version_dir)

    return package_version, packages


def setup_scylla_manager(scylla_manager_package=None, verbose=False):

    """
    download and cache scylla-manager RPMs,
    :return:
    """

    if scylla_manager_package and '--scylla-manager':
        dir_hash = get_url_hash(scylla_manager_package)

        # select a dir to change this version of scylla-manager based on the md5 of the path or the etag of the s3 object
        manager_install_dir = directory_name(os.path.join('manager', dir_hash))
        if not os.path.exists(manager_install_dir):
            os.makedirs(manager_install_dir)
            _, destination_file = tempfile.mkstemp(suffix=".tar.gz", prefix="ccm-manager-")

            if os.path.exists(scylla_manager_package) and scylla_manager_package.endswith('.tar.gz'):
                destination_file = scylla_manager_package
            elif is_valid(scylla_manager_package):
                res = download_version_from_s3(url=scylla_manager_package, target_path=destination_file, verbose=verbose)
                if not res:
                    download_file(url=scylla_manager_package, target_path=destination_file, verbose=verbose)

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


def download_version(version, url=None, verbose=False, target_dir=None, unified=False):
    """
    Download, scylla relocatable package tarballs.
    """
    try:
        if os.path.exists(url) and url.endswith('.tar.gz'):
            target = url
        elif is_valid(url):
            _, target = tempfile.mkstemp(suffix=".tar.gz", prefix="ccm-")
            res = download_version_from_s3(url=url, target_path=target, verbose=verbose)
            if not res:
                download_file(url=url, target_path=target, verbose=verbose)
        else:
            raise ArgumentError(
                f"unsupported url or file doesn't exist\n\turl={url}")

        if verbose:
            print(f"Extracting {target} ({url}, {target_dir}) as version {version} ...")
        tar = tarfile.open(target)
        tar.extractall(path=target_dir)
        tar.close()

        # if relocatable package format >= 2, need to extract files under subdir
        package_version_files = glob.glob(f"{Path(target_dir)}/.relocatable_package_version") + \
                                glob.glob(f"{Path(target_dir)}/**/.relocatable_package_version")
        if package_version_files and os.path.exists(package_version_files[0]):
            with open(package_version_files[0]) as f:
                package_version = packaging.version.parse(f.read().strip())
            if package_version > packaging.version.parse('3'):
                print(f'Unknown relocatable package format version: {package_version}')
                sys.exit(1)
            print(f'Relocatable package format version {package_version} detected.')
            if not unified:
                pkg_dir = glob.glob(f'{target_dir}/*/')[0]
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
            f.write(f"version={version}\n")
            f.write(f"url={url}\n")

        return package_version
    except urllib.error.URLError as e:
        msg = f"Invalid version {version}" if url is None else f"Invalid url {url}"
        msg = msg + f" (underlying error is: {str(e)})"
        raise ArgumentError(msg)
    except tarfile.ReadError as e:
        raise ArgumentError(
            f"Unable to uncompress downloaded file: {str(e)}")


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
        os.makedirs(repo, exist_ok=True)
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

    if package_version >= packaging.version.parse('2.2'):
        install_opt += ' --without-systemd'

    run('''{0}/install.sh --prefix {1} --nonroot{2}'''.format(
        install_dir, scylla_target_dir, install_opt), cwd=install_dir)
    run('''mkdir -p {0}/conf; cp ./conf/scylla.yaml {0}/conf'''.format(
        scylla_target_dir), cwd=install_dir)
    run(f'''ln -s {scylla_target_dir}/bin .''', cwd=target_dir)
    run(f'''ln -s {scylla_target_dir}/conf .''', cwd=target_dir)


def run_scylla_unified_install_script(install_dir, target_dir, package_version):
    if package_version >= packaging.version.parse('3'):
        run('''cd scylla-*; mv * ../''', cwd=install_dir)

    # to skip systemd check at https://github.com/scylladb/scylladb/blob/master/unified/install.sh#L102
    install_opt = ' --supervisor' if '--supervisor' in (Path(install_dir) / 'install.sh').read_text() else ' '

    if package_version >= packaging.version.parse('2.2'):
        install_opt += ' --without-systemd'
    else:
        # Patch the install.sh to not use systemctl, in newer versions --without-systemd is covering it
        run(r'''sed -i 's/systemctl --user.*/echo "commented out systemctl command"/' ./install.sh ./**/install.sh''',
            cwd=install_dir)
        run(r'''sed -i 's|/run/systemd/system|/run|' ./install.sh ./**/install.sh''',  cwd=install_dir)

    run('''{0}/install.sh --prefix {1} --nonroot{2}'''.format(
        install_dir, target_dir, install_opt), cwd=install_dir)
    run(f'''ln -s {install_dir}/scylla/conf conf''', cwd=target_dir)


Architecture = Literal['x86_64', 'aarch64']
BASE_DOWNLOADS_URL = 'https://s3.amazonaws.com/downloads.scylladb.com'


def get_manager_latest_reloc_url(branch: str = "master", architecture: Architecture = None) -> str:
    """
    get the latest manager relocatable version of a specific branch
    """
    architecture = architecture or os.environ.get('SCYLLA_ARCH', 'x86_64')

    url = f"{BASE_DOWNLOADS_URL}/manager/relocatable/unstable/{branch}/"
    # filter only specific architecture
    all_packages = reversed(aws_bucket_ls(url))
    latest_package = next(filter(lambda tar: architecture in tar, all_packages))

    # return latest
    return f'{BASE_DOWNLOADS_URL}/{latest_package}'


def get_manager_release_url(version: str = '', architecture: Architecture = None) -> str:
    """
    get latest official relocatable of manager releases of specific versions i.e. '3.1' or '3.1.1'
    only works from release 3.1 and up
    when version is empty string, won't return latest release (by date, so can be from older branch)
    """
    architecture = architecture or os.environ.get('SCYLLA_ARCH', 'x86_64')

    url = f"{BASE_DOWNLOADS_URL}/downloads/scylla-manager/relocatable"

    version_regex = re.compile('scylla-manager_(.*)-0')
    # filter only specific architecture and version
    all_packages = reversed(aws_bucket_ls(url))
    latest_package = next(filter(lambda tar: architecture in tar and version in version_regex.search(tar)[0], all_packages))

    return f'{BASE_DOWNLOADS_URL}/downloads/scylla-manager/relocatable/{latest_package}'
