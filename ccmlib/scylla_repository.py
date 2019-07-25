from __future__ import with_statement

import os
import tarfile
import tempfile
import shutil
import glob

from six import print_
from six.moves import urllib

from ccmlib.common import (ArgumentError, CCMError, get_default_path, rmdirs, validate_install_dir)
from ccmlib.repository import __download

GIT_REPO = "http://github.com/scylladb/scylla.git"

RELOCATABLE_URLS_BASE = 'https://s3.amazonaws.com/downloads.scylladb.com/relocatable/{0}/{1}'


def setup(version, verbose=True):
    s3_url = ''
    type_n_version = version.split(':')
    if len(type_n_version) == 2:
        s3_version = type_n_version[1]
        s3_url = RELOCATABLE_URLS_BASE.format(type_n_version[0], s3_version)
        version = os.path.join(*type_n_version)

    cdir = version_directory(version)

    if cdir is None:
        tmp_download = tempfile.mkdtemp()

        url = os.environ.get('SCYLLA_CORE_PACKAGE', os.path.join(s3_url, 'scylla-package.tar.gz'))
        download_version(version, verbose=verbose, url=url, target_dir=tmp_download)

        url = os.environ.get('SCYLLA_JAVA_TOOLS_PACKAGE', os.path.join(s3_url, 'scylla-tools-package.tar.gz'))
        download_version(version, verbose=verbose, url=url, target_dir=os.path.join(tmp_download, 'scylla-java-tools'))

        url = os.environ.get('SCYLLA_JMX_PACKAGE', os.path.join(s3_url, 'scylla-jmx-package.tar.gz'))
        download_version(version, verbose=verbose, url=url, target_dir=os.path.join(tmp_download, 'jmx'))

        cdir = directory_name(version)

        shutil.move(tmp_download, cdir)
        # hack to make the relocatable tools work
        # https://github.com/scylladb/scylla-tools-java/issues/104
        scylla_java_tools_dir = os.path.join(cdir, 'scylla-java-tools')
        for jar_file in glob.glob(scylla_java_tools_dir + "/*.jar"):
            shutil.copy(jar_file, os.path.join(scylla_java_tools_dir, "lib"))

    return cdir, version


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
            raise ArgumentError("unsupported url or file doesn't exist\n\turl={}".format(url))

        if verbose:
            print_("Extracting %s as version %s ..." % (target, version))
        tar = tarfile.open(target)
        tar.extractall(path=target_dir)
        tar.close()

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
        raise ArgumentError("Unable to uncompress downloaded file: %s" % str(e))
    except CCMError as e:
        if target_dir:
            # wipe out the directory if anything goes wrong.
            try:
                rmdirs(target_dir)
                print_("Deleted %s due to error" % target_dir)
            except:
                raise CCMError("Downloading/extracting scylla version %s failed. Attempted to delete %s but failed. This will need to be manually deleted" % (version, target_dir))
        raise e


def directory_name(version):
    return os.path.join(__get_dir(), version)


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
