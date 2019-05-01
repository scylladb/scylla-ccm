from __future__ import with_statement

import os
import tarfile
import tempfile
import time
import shutil
import glob

from six import print_
from six.moves import urllib

from ccmlib.common import (ArgumentError, CCMError, get_default_path, rmdirs, validate_install_dir)

GIT_REPO = "http://github.com/scylladb/scylla.git"

RELOCATABLE_URLS = {
    'unstable/master': 'https://s3.amazonaws.com/downloads.scylladb.com/relocatable/unstable/master/'
}


def setup(version, verbose=True):
    s3_url = 'https://s3.amazonaws.com/downloads.scylladb.com/relocatable/unstable/master/latest'
    type_n_version = version.split(':')
    if len(type_n_version) == 2:
        s3_version = type_n_version[1]
        s3_url = "{0}{1}".format(RELOCATABLE_URLS[type_n_version[0]], s3_version)
        version = os.path.join(*type_n_version)

    cdir = version_directory(version)
    if cdir is None:
        url = os.environ.get('SCYLLA_PACKAGE', os.path.join(s3_url, 'scylla-package.tar.gz'))
        download_version(version, verbose=verbose, url=url)

        url = os.environ.get('SCYLLA_JAVA_TOOLS_PACKAGE', os.path.join(s3_url, 'scylla-tools-package.tar.gz'))
        download_version(os.path.join(version, 'scylla-java-tools'), verbose=verbose, url=url)

        # hack to make the relocatable tools work
        cdir = version_directory(version)
        scylla_java_tools_dir = os.path.join(cdir, 'scylla-java-tools')
        for jar_file in glob.glob(scylla_java_tools_dir + "/*.jar"):
            shutil.copy(jar_file, os.path.join(scylla_java_tools_dir, "lib"))

        url = os.environ.get('SCYLLA_JMX_PACKAGE', os.path.join(s3_url, 'scylla-jmx-package.tar.gz'))
        download_version(os.path.join(version, 'jmx'), verbose=verbose, url=url)

        cdir = version_directory(version)
    return cdir, version


min_attributes = ('scheme', 'netloc')


def is_valid(url, qualifying=None):
    qualifying = min_attributes if qualifying is None else qualifying
    token = urllib.parse.urlparse(url)
    return all([getattr(token, qualifying_attr)
                for qualifying_attr in qualifying])


def download_version(version, url=None, verbose=False):
    """Download, extract, and build Cassandra tarball.

    if binary == True, download precompiled tarball, otherwise build from source tarball.
    """
    target_dir = None
    try:
        if os.path.exists(url) and url.endswith('.tar.gz'):
            target = url
        elif is_valid(url):
            _, target = tempfile.mkstemp(suffix=".tar.gz", prefix="ccm-")
            __download(url, target, show_progress=verbose)
        else:
            raise ArgumentError("unsupported url={}".format(url))

        if verbose:
            print_("Extracting %s as version %s ..." % (target, version))
        tar = tarfile.open(target)
        tar.extractall(path=os.path.join(__get_dir(), version))
        tar.close()

    except urllib.error.URLError as e:
        msg = "Invalid version %s" % version if url is None else "Invalid url %s" % url
        msg = msg + " (underlying error is: %s)" % str(e)
        raise ArgumentError(msg)
    except tarfile.ReadError as e:
        raise ArgumentError("Unable to uncompress downloaded file: %s" % str(e))
    except CCMError as e:
        if target_dir:
            # wipe out the directory if anything goes wrong. Otherwise we will assume it has been compiled the next time it runs.
            try:
                rmdirs(target_dir)
                print_("Deleted %s due to error" % target_dir)
            except:
                raise CCMError("Building C* version %s failed. Attempted to delete %s but failed. This will need to be manually deleted" % (version, target_dir))
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


def clean_all():
    rmdirs(__get_dir())


def __download(url, target, username=None, password=None, show_progress=False):
    if username is not None:
        password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        password_mgr.add_password(None, url, username, password)
        handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
        opener = urllib.request.build_opener(handler)
        urllib.request.install_opener(opener)

    u = urllib.request.urlopen(url)
    f = open(target, 'wb')
    meta = u.info()
    file_size = int(meta.get("Content-Length"))
    if show_progress:
        print_("Downloading %s to %s (%.3fMB)" % (url, target, float(file_size) / (1024 * 1024)))

    file_size_dl = 0
    block_sz = 8192
    attempts = 0
    while file_size_dl < file_size:
        buffer = u.read(block_sz)
        if not buffer:
            attempts = attempts + 1
            if attempts >= 5:
                raise CCMError("Error downloading file (nothing read after %i attempts, downloded only %i of %i bytes)" % (attempts, file_size_dl, file_size))
            time.sleep(0.5 * attempts)
            continue
        else:
            attempts = 0

        file_size_dl += len(buffer)
        f.write(buffer)
        if show_progress:
            status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
            status = chr(8) * (len(status) + 1) + status
            print_(status, end='')

    if show_progress:
        print_("")
    f.close()
    u.close()


def __get_dir():
    repo = os.path.join(get_default_path(), 'scylla-repository')
    if not os.path.exists(repo):
        os.mkdir(repo)
    return repo


def lastlogfilename():
    return os.path.join(__get_dir(), "last.log")
