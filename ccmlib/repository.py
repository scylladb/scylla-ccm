# downloaded sources handling


import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib
from distutils.version import LooseVersion


from ccmlib.common import (ArgumentError, CCMError, get_default_path,
                           platform_binary, rmdirs, validate_install_dir,
                           assert_jdk_valid_for_cassandra_version, get_version_from_build)

DSE_ARCHIVE = "http://downloads.datastax.com/enterprise/dse-%s-bin.tar.gz"
OPSC_ARCHIVE = "http://downloads.datastax.com/community/opscenter-%s.tar.gz"
ARCHIVE = "http://archive.apache.org/dist/cassandra"
GIT_REPO = "http://git-wip-us.apache.org/repos/asf/cassandra.git"
GITHUB_TAGS = "https://api.github.com/repos/apache/cassandra/git/refs/tags"


def setup(version, verbose=False):
    binary = True
    fallback = True

    if version.startswith('git:'):
        clone_development(GIT_REPO, version, verbose=verbose)
        return (version_directory(version), None)

    elif version.startswith('binary:'):
        version = version.replace('binary:', '')
        fallback = False

    elif version.startswith('github:'):
        user_name, _ = github_username_and_branch_name(version)
        clone_development(github_repo_for_user(user_name), version, verbose=verbose)
        return (directory_name(version), None)

    elif version.startswith('source:'):
        version = version.replace('source:', '')
        binary = False
        fallback = False

    if version in ('stable', 'oldstable', 'testing'):
        version = get_tagged_version_numbers(version)[0]

    cdir = version_directory(version)
    if cdir is None:
        try:
            download_version(version, verbose=verbose, binary=binary)
            cdir = version_directory(version)
        except Exception as e:
            # If we failed to download from ARCHIVE,
            # then we build from source from the git repo,
            # as it is more reliable.
            # We don't do this if binary: or source: were
            # explicitly specified.
            if fallback:
                print(f"WARN:Downloading {version} failed, due to {e}. Trying to build from git instead.")
                version = f'git:cassandra-{version}'
                clone_development(GIT_REPO, version, verbose=verbose)
                return (version_directory(version), None)
            else:
                raise e
    return (cdir, version)


def setup_dse(version, username, password, verbose=False):
    cdir = version_directory(version)
    if cdir is None:
        download_dse_version(version, username, password, verbose=verbose)
        cdir = version_directory(version)
    return (cdir, version)


def setup_opscenter(opscenter, verbose=False):
    ops_version = 'opsc' + opscenter
    odir = version_directory(ops_version)
    if odir is None:
        download_opscenter_version(opscenter, ops_version, verbose=verbose)
        odir = version_directory(ops_version)
    return odir


def validate(path):
    if path.startswith(__get_dir()):
        _, version = os.path.split(os.path.normpath(path))
        setup(version)


def clone_development(git_repo, version, verbose=False):
    print(git_repo, version)
    target_dir = directory_name(version)
    assert target_dir
    if 'github' in version:
        git_repo_name, git_branch = github_username_and_branch_name(version)
    else:
        git_repo_name = 'apache'
        git_branch = version.split(':', 1)[1]
    local_git_cache = os.path.join(__get_dir(), '_git_cache_' + git_repo_name)
    logfile = lastlogfilename()
    with open(logfile, 'w') as lf:
        try:
            # Checkout/fetch a local repository cache to reduce the number of
            # remote fetches we need to perform:
            if not os.path.exists(local_git_cache):
                if verbose:
                    print("Cloning Cassandra...")
                out = subprocess.call(
                    ['git', 'clone', '--mirror', git_repo, local_git_cache],
                    cwd=__get_dir(), stdout=lf, stderr=lf)
                assert out == 0, "Could not do a git clone"
            else:
                if verbose:
                    print("Fetching Cassandra updates...")
                out = subprocess.call(
                    ['git', 'fetch', '-fup', 'origin', '+refs/*:refs/*'],
                    cwd=local_git_cache, stdout=lf, stderr=lf)

            # Checkout the version we want from the local cache:
            if not os.path.exists(target_dir):
                # development branch doesn't exist. Check it out.
                if verbose:
                    print("Cloning Cassandra (from local cache)")

                # git on cygwin appears to be adding `cwd` to the commands which is breaking clone
                if sys.platform == "cygwin":
                    local_split = local_git_cache.split(os.sep)
                    target_split = target_dir.split(os.sep)
                    subprocess.call(['git', 'clone', local_split[-1], target_split[-1]], cwd=__get_dir(), stdout=lf, stderr=lf)
                else:
                    subprocess.call(['git', 'clone', local_git_cache, target_dir], cwd=__get_dir(), stdout=lf, stderr=lf)

                # determine if the request is for a branch
                is_branch = False
                try:
                    branch_listing = subprocess.check_output(['git', 'branch', '--all'], cwd=target_dir).decode('utf-8')
                    branches = [b.strip() for b in branch_listing.replace('remotes/origin/', '').split()]
                    is_branch = git_branch in branches
                except subprocess.CalledProcessError as cpe:
                    print(f"Error Running Branch Filter: {cpe.output}\nAssumming request is not for a branch")

                # now check out the right version
                if verbose:
                    branch_or_sha_tag = 'branch' if is_branch else 'SHA/tag'
                    print(f"Checking out requested {branch_or_sha_tag} ({git_branch})")
                if is_branch:
                    # we use checkout -B with --track so we can specify that we want to track a specific branch
                    # otherwise, you get errors on branch names that are also valid SHAs or SHA shortcuts, like 10360
                    # we use -B instead of -b so we reset branches that already exist and create a new one otherwise
                    out = subprocess.call(['git', 'checkout', '-B', git_branch,
                                           '--track', f'origin/{git_branch}'],
                                          cwd=target_dir, stdout=lf, stderr=lf)
                else:
                    out = subprocess.call(['git', 'checkout', git_branch], cwd=target_dir, stdout=lf, stderr=lf)
                if int(out) != 0:
                    raise CCMError('Could not check out git branch {branch}. '
                                   'Is this a valid branch name? (see {lastlog} or run '
                                   '"ccm showlastlog" for details)'.format(
                                       branch=git_branch, lastlog=logfile
                                   ))
                # now compile
                compile_version(git_branch, target_dir, verbose)
            else:  # branch is already checked out. See if it is behind and recompile if needed.
                out = subprocess.call(['git', 'fetch', 'origin'], cwd=target_dir, stdout=lf, stderr=lf)
                assert out == 0, "Could not do a git fetch"
                status = subprocess.Popen(['git', 'status', '-sb'], cwd=target_dir, stdout=subprocess.PIPE, stderr=lf).communicate()[0]
                if str(status).find('[behind') > -1:
                    if verbose:
                        print("Branch is behind, recompiling")
                    out = subprocess.call(['git', 'pull'], cwd=target_dir, stdout=lf, stderr=lf)
                    assert out == 0, "Could not do a git pull"
                    out = subprocess.call([platform_binary('ant'), 'realclean'], cwd=target_dir, stdout=lf, stderr=lf)
                    assert out == 0, "Could not run 'ant realclean'"

                    # now compile
                    compile_version(git_branch, target_dir, verbose)
        except:
            # wipe out the directory if anything goes wrong. Otherwise we will assume it has been compiled the next time it runs.
            try:
                rmdirs(target_dir)
                print(f"Deleted {target_dir} due to error")
            except:
                raise CCMError(f"Building C* version {version} failed. Attempted to delete {target_dir} but failed. This will need to be manually deleted")
            raise


def download_dse_version(version, username, password, verbose=False):
    url = DSE_ARCHIVE % version
    _, target = tempfile.mkstemp(suffix=".tar.gz", prefix="ccm-")
    try:
        __download(url, target, username=username, password=password, show_progress=verbose)
        if verbose:
            print(f"Extracting {target} as version {version} ...")
        tar = tarfile.open(target)
        dir = tar.next().name.split("/")[0]
        tar.extractall(path=__get_dir())
        tar.close()
        target_dir = os.path.join(__get_dir(), version)
        if os.path.exists(target_dir):
            rmdirs(target_dir)
        shutil.move(os.path.join(__get_dir(), dir), target_dir)
    except urllib.error.URLError as e:
        msg = f"Invalid version {version}" if url is None else f"Invalid url {url}"
        msg = msg + f" (underlying error is: {str(e)})"
        raise ArgumentError(msg)
    except tarfile.ReadError as e:
        raise ArgumentError(f"Unable to uncompress downloaded file: {str(e)}")


def download_opscenter_version(version, target_version, verbose=False):
    url = OPSC_ARCHIVE % version
    _, target = tempfile.mkstemp(suffix=".tar.gz", prefix="ccm-")
    try:
        __download(url, target, show_progress=verbose)
        if verbose:
            print(f"Extracting {target} as version {target_version} ...")
        tar = tarfile.open(target)
        dir = tar.next().name.split("/")[0]
        tar.extractall(path=__get_dir())
        tar.close()
        target_dir = os.path.join(__get_dir(), target_version)
        if os.path.exists(target_dir):
            rmdirs(target_dir)
        shutil.move(os.path.join(__get_dir(), dir), target_dir)
    except urllib.error.URLError as e:
        msg = f"Invalid version {version}" if url is None else f"Invalid url {url}"
        msg = msg + f" (underlying error is: {str(e)})"
        raise ArgumentError(msg)
    except tarfile.ReadError as e:
        raise ArgumentError(f"Unable to uncompress downloaded file: {str(e)}")


def download_version(version, url=None, verbose=False, binary=False):
    """Download, extract, and build Cassandra tarball.

    if binary == True, download precompiled tarball, otherwise build from source tarball.
    """
    assert_jdk_valid_for_cassandra_version(version)

    if binary:
        u = f"{ARCHIVE}/{version.split('-')[0]}/apache-cassandra-{version}-bin.tar.gz" if url is None else url
    else:
        u = f"{ARCHIVE}/{version.split('-')[0]}/apache-cassandra-{version}-src.tar.gz" if url is None else url
    _, target = tempfile.mkstemp(suffix=".tar.gz", prefix="ccm-")
    try:
        __download(u, target, show_progress=verbose)
        if verbose:
            print(f"Extracting {target} as version {version} ...")
        tar = tarfile.open(target)
        dir = tar.next().name.split("/")[0]
        tar.extractall(path=__get_dir())
        tar.close()
        target_dir = os.path.join(__get_dir(), version)
        if os.path.exists(target_dir):
            rmdirs(target_dir)
        shutil.move(os.path.join(__get_dir(), dir), target_dir)

        if binary:
            # Binary installs don't have a build.xml that is needed
            # for pulling the version from. Write the version number
            # into a file to read later in common.get_version_from_build()
            with open(os.path.join(target_dir, '0.version.txt'), 'w') as f:
                f.write(version)
        else:
            compile_version(version, target_dir, verbose=verbose)

    except urllib.error.URLError as e:
        msg = f"Invalid version {version}" if url is None else f"Invalid url {url}"
        msg = msg + f" (underlying error is: {str(e)})"
        raise ArgumentError(msg)
    except tarfile.ReadError as e:
        raise ArgumentError(f"Unable to uncompress downloaded file: {str(e)}")
    except CCMError as e:
        # wipe out the directory if anything goes wrong. Otherwise we will assume it has been compiled the next time it runs.
        try:
            rmdirs(target_dir)
            print(f"Deleted {target_dir} due to error")
        except:
            raise CCMError(f"Building C* version {version} failed. Attempted to delete {target_dir} but failed. This will need to be manually deleted")
        raise e


def compile_version(version, target_dir, verbose=False):
    assert_jdk_valid_for_cassandra_version(get_version_from_build(target_dir))

    # compiling cassandra and the stress tool
    logfile = lastlogfilename()
    if verbose:
        print(f"Compiling Cassandra {version} ...")
    with open(logfile, 'w') as lf:
        lf.write("--- Cassandra Build -------------------\n")
        try:
            # Patch for pending Cassandra issue: https://issues.apache.org/jira/browse/CASSANDRA-5543
            # Similar patch seen with buildbot
            attempt = 0
            ret_val = 1
            while attempt < 3 and ret_val != 0:
                if attempt > 0:
                    lf.write(f"\n\n`ant jar` failed. Retry #{attempt}...\n\n")
                ret_val = subprocess.call([platform_binary('ant'), 'jar'], cwd=target_dir, stdout=lf, stderr=lf)
                attempt += 1
            if ret_val != 0:
                raise CCMError('Error compiling Cassandra. See {logfile} or run '
                               '"ccm showlastlog" for details'.format(logfile=logfile))
        except OSError as e:
            raise CCMError(f"Error compiling Cassandra. Is ant installed? See {logfile} for details")

        lf.write("\n\n--- cassandra/stress build ------------\n")
        stress_dir = os.path.join(target_dir, "tools", "stress") if (
            version >= "0.8.0") else \
            os.path.join(target_dir, "contrib", "stress")

        build_xml = os.path.join(stress_dir, 'build.xml')
        if os.path.exists(build_xml):  # building stress separately is only necessary pre-1.1
            try:
                # set permissions correctly, seems to not always be the case
                stress_bin_dir = os.path.join(stress_dir, 'bin')
                for f in os.listdir(stress_bin_dir):
                    full_path = os.path.join(stress_bin_dir, f)
                    os.chmod(full_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)

                if subprocess.call([platform_binary('ant'), 'build'], cwd=stress_dir, stdout=lf, stderr=lf) != 0:
                    if subprocess.call([platform_binary('ant'), 'stress-build'], cwd=target_dir, stdout=lf, stderr=lf) != 0:
                        raise CCMError("Error compiling Cassandra stress tool.  "
                                       "See %s for details (you will still be able to use ccm "
                                       "but not the stress related commands)" % logfile)
            except IOError as e:
                raise CCMError("Error compiling Cassandra stress tool: %s (you will "
                               "still be able to use ccm but not the stress related commands)" % str(e))


def directory_name(version):
    version = version.replace(':', 'COLON')  # handle git branches like 'git:trunk'.
    version = version.replace('/', 'SLASH')  # handle git branches like 'github:mambocab/trunk'.
    return os.path.join(__get_dir(), version)


def github_username_and_branch_name(version):
    assert version.startswith('github')
    return version.split(':', 1)[1].split('/', 1)


def github_repo_for_user(username):
    return f'git@github.com:{username}/cassandra.git'


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


def get_tagged_version_numbers(series='stable'):
    """Retrieve git tags and find version numbers for a release series

    series - 'stable', 'oldstable', or 'testing'"""
    releases = []
    if series == 'testing':
        # Testing releases always have a hyphen after the version number:
        tag_regex = re.compile(r'^refs/tags/cassandra-([0-9]+\.[0-9]+\.[0-9]+-.*$)')
    else:
        # Stable and oldstable releases are just a number:
        tag_regex = re.compile(r'^refs/tags/cassandra-([0-9]+\.[0-9]+\.[0-9]+$)')

    tag_url = urllib.request.urlopen(GITHUB_TAGS)
    for ref in (i.get('ref', '') for i in json.loads(tag_url.read())):
        m = tag_regex.match(ref)
        if m:
            releases.append(LooseVersion(m.groups()[0]))

    # Sort by semver:
    releases.sort(reverse=True)

    stable_major_version = LooseVersion(str(releases[0].version[0]) + "." + str(releases[0].version[1]))
    stable_releases = [r for r in releases if r >= stable_major_version]
    oldstable_releases = [r for r in releases if r not in stable_releases]
    oldstable_major_version = LooseVersion(str(oldstable_releases[0].version[0]) + "." + str(oldstable_releases[0].version[1]))
    oldstable_releases = [r for r in oldstable_releases if r >= oldstable_major_version]

    if series == 'testing':
        return [r.vstring for r in releases]
    elif series == 'stable':
        return [r.vstring for r in stable_releases]
    elif series == 'oldstable':
        return [r.vstring for r in oldstable_releases]
    else:
        raise AssertionError(f"unknown release series: {series}")


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
        print(f"Downloading {url} to {target} ({float(file_size) / (1024 * 1024):.3f}MB)")

    file_size_dl = 0
    block_sz = 8192
    status = None
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
            print(status, end='')

    if show_progress:
        print("")
    f.close()
    u.close()


def __get_dir():
    repo = os.path.join(get_default_path(), 'repository')
    if not os.path.exists(repo):
        os.makedirs(repo, exist_ok=True)
    return repo


def lastlogfilename():
    return os.path.join(__get_dir(), "last.log")
