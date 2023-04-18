#
# Cassandra Cluster Management lib
#

import fnmatch
import os
import platform
import re
import shutil
import socket
import stat
import subprocess
import sys
import time
import tempfile
import logging
import pathlib
from itertools import zip_longest
from typing import Callable, Optional

import yaml
from boto3.session import Session
from botocore import UNSIGNED
from botocore.client import Config

BIN_DIR = "bin"
CASSANDRA_CONF_DIR = "conf"
DSE_CASSANDRA_CONF_DIR = "resources/cassandra/conf"
OPSCENTER_CONF_DIR = "conf"
SCYLLA_CONF_DIR = "conf"
SCYLLAMANAGER_DIR = "scylla-manager"


CASSANDRA_CONF = "cassandra.yaml"
SCYLLA_CONF = "scylla.yaml"
SCYLLAMANAGER_CONF = "scylla-manager.yaml"
LOG4J_CONF = "log4j-server.properties"
LOG4J_TOOL_CONF = "log4j-tools.properties"
LOGBACK_CONF = "logback.xml"
LOGBACK_TOOLS_CONF = "logback-tools.xml"
CASSANDRA_ENV = "cassandra-env.sh"
CASSANDRA_WIN_ENV = "cassandra-env.ps1"
CASSANDRA_SH = "cassandra.in.sh"

CONFIG_FILE = "config"
CCM_CONFIG_DIR = "CCM_CONFIG_DIR"

DOWNLOAD_IN_PROGRESS_FILE = "download_in_progress"

logger = logging.getLogger('ccm')

class CCMError(Exception):
    pass


class LoadError(CCMError):
    pass


class ArgumentError(CCMError):
    pass


class UnavailableSocketError(CCMError):
    pass


def get_default_path():
    if CCM_CONFIG_DIR in os.environ and os.environ[CCM_CONFIG_DIR]:
        default_path = os.environ[CCM_CONFIG_DIR]
    else:
        default_path = os.path.join(get_user_home(), '.ccm')

    if not os.path.exists(default_path):
        os.mkdir(default_path)
    return default_path


def get_default_path_display_name():
    default_path = get_default_path().lower()
    user_home = get_user_home().lower()

    if default_path.startswith(user_home):
        default_path = os.path.join('~', default_path[len(user_home) + 1:])

    return default_path


def get_user_home():
    if is_win():
        if sys.platform == "cygwin":
            # Need the fully qualified directory
            output = subprocess.Popen(["cygpath", "-m", os.path.expanduser('~')], stdout=subprocess.PIPE, stderr=subprocess.STDOUT).communicate()[0].rstrip()
            return output
        else:
            return os.environ['USERPROFILE']
    else:
        return os.path.expanduser('~')


def get_config():
    config_path = os.path.join(get_default_path(), CONFIG_FILE)
    if not os.path.exists(config_path):
        return {}

    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def now_ms():
    return int(round(time.time() * 1000))


def parse_interface(itf, default_port):
    i = itf.split(':')
    if len(i) == 1:
        return (i[0].strip(), default_port)
    elif len(i) == 2:
        return (i[0].strip(), int(i[1].strip()))
    else:
        raise ValueError("Invalid interface definition: " + itf)


def current_cluster_name(path):
    try:
        with open(os.path.join(path, 'CURRENT'), 'r') as f:
            return f.readline().strip()
    except IOError:
        return None


def switch_cluster(path, new_name):
    with open(os.path.join(path, 'CURRENT'), 'w') as f:
        f.write(new_name + '\n')


def replace_in_file(file, regexp, replace):
    replaces_in_file(file, [(regexp, replace)])


def replaces_in_files(src, dst, replacement_list):
    rs = [(re.compile(regexp), repl) for (regexp, repl) in replacement_list]
    with open(src, 'r') as f:
        with open(dst, 'w') as f_tmp:
            for line in f:
                for r, replace in rs:
                    match = r.search(line)
                    if match:
                        line = replace + "\n"
                f_tmp.write(line)


def replaces_in_file(file, replacement_list):
    file_tmp = file + ".tmp"
    replaces_in_files(file, file_tmp, replacement_list)
    shutil.move(file_tmp, file)


def replace_or_add_into_file_tail(file, regexp, replace):
    replaces_or_add_into_file_tail(file, [(regexp, replace)])


def replaces_or_add_into_file_tail(file, replacement_list):
    rs = [(re.compile(regexp), repl) for (regexp, repl) in replacement_list]
    is_line_found = False
    file_tmp = file + ".tmp"
    with open(file, 'r') as f:
        with open(file_tmp, 'w') as f_tmp:
            for line in f:
                for r, replace in rs:
                    match = r.search(line)
                    if match:
                        line = replace + "\n"
                        is_line_found = True
                if "</configuration>" not in line:
                    f_tmp.write(line)
            # In case, entry is not found, and need to be added
            if not is_line_found:
                f_tmp.write('\n' + replace + "\n")
            # We are moving the closing tag to the end of the file.
            # Previously, we were having an issue where new lines we wrote
            # were appearing after the closing tag, and thus being ignored.
            f_tmp.write("</configuration>\n")

    shutil.move(file_tmp, file)


def rmdirs(path):
    if is_win():
        # Handle Windows 255 char limit
        shutil.rmtree("\\\\?\\" + path, ignore_errors=True)
    else:
        shutil.rmtree(path, ignore_errors=True)


def make_cassandra_env(install_dir, node_path, update_conf=True, hardcode_java_version: Optional[str] = None):
    if is_win() and get_version_from_build(node_path=node_path) >= '2.1':
        sh_file = os.path.join(CASSANDRA_CONF_DIR, CASSANDRA_WIN_ENV)
    else:
        sh_file = os.path.join(BIN_DIR, CASSANDRA_SH)
    orig = os.path.join(install_dir, sh_file)
    dst = os.path.join(node_path, sh_file)

    if not update_conf and not os.path.exists(dst):
        time.sleep(1)
        if not os.path.exists(dst):
            print(f"Warning: make_cassandra_env: install_dir={install_dir} node_path={node_path} missing {dst}. Recreating...")

    if not os.path.exists(dst):
        replacements = ""
        if is_win() and get_version_from_build(node_path=node_path) >= '2.1':
            replacements = [
                ('env:CASSANDRA_HOME =', f'        $env:CASSANDRA_HOME="{install_dir}"'),
                ('env:CASSANDRA_CONF =', '    $env:CCM_DIR="' + node_path + '\\conf"\n    $env:CASSANDRA_CONF="$env:CCM_DIR"'),
                ('cp = ".*?env:CASSANDRA_HOME.conf', '    $cp = """$env:CASSANDRA_CONF"""')
            ]
        else:
            replacements = [
                ('CASSANDRA_HOME=', f'\tCASSANDRA_HOME={install_dir}'),
                ('CASSANDRA_CONF=', f"\tCASSANDRA_CONF={os.path.join(node_path, 'conf')}")
            ]
        (fd, tmp) = tempfile.mkstemp(dir=node_path)
        replaces_in_files(orig, tmp, replacements)

        # If a cluster-wide cassandra.in.sh file exists in the parent
        # directory, append it to the node specific one:
        cluster_sh_file = os.path.join(node_path, os.path.pardir, 'cassandra.in.sh')
        if os.path.exists(cluster_sh_file):
            append = open(cluster_sh_file).read()
            with open(tmp, 'a') as f:
                f.write('\n\n### Start Cluster wide config ###\n')
                f.write(append)
                f.write('\n### End Cluster wide config ###\n\n')

        os.rename(tmp, dst)

    env = os.environ.copy()
    env['CASSANDRA_INCLUDE'] = os.path.join(dst)
    env['MAX_HEAP_SIZE'] = os.environ.get('CCM_MAX_HEAP_SIZE', '500M')
    env['HEAP_NEWSIZE'] = os.environ.get('CCM_HEAP_NEWSIZE', '50M')
    # FIXME workaround for now - should be removed and maybe included in tool execution scripts
    env['cassandra.config'] = "file://" + os.path.join(node_path, 'conf', 'cassandra.yaml')
    env['CASSANDRA_HOME'] = install_dir
    env['CASSANDRA_CONF'] = os.path.join(node_path, 'conf')

    if hardcode_java_version:
        known_jvm_names = {
            '8': ('8-openjdk', '8-jdk', '1.8.0-openjdk'),
            '11': ('11-openjdk', '11-jdk', '1.11.0-openjdk'),
        }
        assert hardcode_java_version in known_jvm_names.keys(), \
            f"hardcode_java_version={hardcode_java_version} not supported in:\n{known_jvm_names}"

        for java_path in pathlib.Path('/usr/lib/jvm/').iterdir():
            if java_path.is_dir and any(name in java_path.name for name in known_jvm_names[hardcode_java_version]):
                env['JAVA_HOME'] = str(java_path)
                break
        else:
            raise ArgumentError("java-8 wasn't found in /usr/lib/jvm/")

    return env


def make_dse_env(install_dir, node_path):
    env = os.environ.copy()
    env['MAX_HEAP_SIZE'] = os.environ.get('CCM_MAX_HEAP_SIZE', '500M')
    env['HEAP_NEWSIZE'] = os.environ.get('CCM_HEAP_NEWSIZE', '50M')
    env['DSE_HOME'] = os.path.join(install_dir)
    env['DSE_CONF'] = os.path.join(node_path, 'resources', 'dse', 'conf')
    env['CASSANDRA_HOME'] = os.path.join(install_dir, 'resources', 'cassandra')
    env['CASSANDRA_CONF'] = os.path.join(node_path, 'resources', 'cassandra', 'conf')
    env['HADOOP_CONF_DIR'] = os.path.join(node_path, 'resources', 'hadoop', 'conf')
    env['HIVE_CONF_DIR'] = os.path.join(node_path, 'resources', 'hive', 'conf')
    env['SQOOP_CONF_DIR'] = os.path.join(node_path, 'resources', 'sqoop', 'conf')
    env['TOMCAT_HOME'] = os.path.join(node_path, 'resources', 'tomcat')
    env['TOMCAT_CONF_DIR'] = os.path.join(node_path, 'resources', 'tomcat', 'conf')
    env['PIG_CONF_DIR'] = os.path.join(node_path, 'resources', 'pig', 'conf')
    env['MAHOUT_CONF_DIR'] = os.path.join(node_path, 'resources', 'mahout', 'conf')
    env['SPARK_CONF_DIR'] = os.path.join(node_path, 'resources', 'spark', 'conf')
    env['SHARK_CONF_DIR'] = os.path.join(node_path, 'resources', 'shark', 'conf')
    return env


def check_win_requirements():
    if is_win():
        # Make sure ant.bat is in the path and executable before continuing
        try:
            subprocess.Popen('ant.bat', stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        except Exception:
            sys.exit("ERROR!  Could not find or execute ant.bat.  Please fix this before attempting to run ccm on Windows.")

        # Confirm matching architectures
        # 32-bit python distributions will launch 32-bit cmd environments, losing PowerShell execution privileges on a 64-bit system
        if sys.maxsize <= 2 ** 32 and platform.machine().endswith('64'):
            sys.exit("ERROR!  64-bit os and 32-bit python distribution found.  ccm requires matching architectures.")


def is_win():
    return sys.platform in ("cygwin", "win32")


def is_ps_unrestricted():
    if not is_win():
        raise CCMError("Can only check PS Execution Policy on Windows")
    else:
        try:
            p = subprocess.Popen(['powershell', 'Get-ExecutionPolicy'], stdout=subprocess.PIPE)
        # pylint: disable=E0602
        except WindowsError:
            print("ERROR: Could not find powershell. Is it in your path?")
        if "Unrestricted" in p.communicate()[0]:
            return True
        else:
            return False


def join_bin(root, dir, executable):
    return os.path.join(root, dir, platform_binary(executable))


def platform_binary(input):
    return input + ".bat" if is_win() else input


def platform_pager():
    return "more" if sys.platform == "win32" else "less"


def add_exec_permission(path, executable):
    # 1) os.chmod on Windows can't add executable permissions
    # 2) chmod from other folders doesn't work in cygwin, so we have to navigate the shell
    # to the folder with the executable with it and then chmod it from there
    if sys.platform == "cygwin":
        cmd = "cd " + path + "; chmod u+x " + executable
        os.system(cmd)


def parse_path(executable):
    sep = os.sep
    if sys.platform == "win32":
        sep = "\\\\"
    tokens = re.split(sep, executable)
    del tokens[-1]
    return os.sep.join(tokens)


def parse_bin(executable):
    tokens = re.split(os.sep, executable)
    return tokens[-1]


def get_tools_java_dir(install_dir, relative_repos_root='..'):
    if 'scylla-repository' in install_dir:
        candidates = [
            os.path.join(install_dir, 'scylla-tools-java'),
            os.path.join(install_dir, 'scylla-core-package', 'scylla-tools'),
        ]
    else:
        dir = os.environ.get('TOOLS_JAVA_DIR')
        if dir:
            return dir
        candidates = [
            os.path.join(install_dir, 'resources', 'cassandra'),
            os.path.join(install_dir, 'tools', 'java'),
            os.path.join(install_dir, relative_repos_root, 'scylla-tools-java')
        ]
    for candidate in candidates:
        if os.path.isdir(candidate):
            return candidate


def get_jmx_dir(install_dir, relative_repos_root='..'):
    dir = os.environ.get('SCYLLA_JMX_DIR')
    if dir:
        return dir
    candidates = [
        os.path.join(install_dir, 'tools', 'jmx'),
        os.path.join(install_dir, relative_repos_root, 'scylla-jmx'),
        os.path.join(install_dir, relative_repos_root, 'scylla-core-package', 'scylla-jmx'),
        os.path.join(install_dir, 'scylla-core-package', 'scylla-jmx'),
        os.path.join(install_dir, 'scylla-jmx'),
    ]
    for candidate in candidates:
        if os.path.isdir(candidate):
            return candidate


def get_stress_bin(install_dir):
    candidates = [
        os.path.join(get_tools_java_dir(install_dir), 'tools', 'bin', 'cassandra-stress'),
        os.path.join(install_dir, 'contrib', 'stress', 'bin', 'stress'),
        os.path.join(install_dir, 'tools', 'stress', 'bin', 'stress'),
        os.path.join(install_dir, 'tools', 'bin', 'stress'),
        os.path.join(install_dir, 'tools', 'bin', 'cassandra-stress'),
    ]
    candidates = [platform_binary(s) for s in candidates]

    for candidate in candidates:
        if os.path.exists(candidate):
            stress = candidate
            break
    else:
        raise Exception("Cannot find stress binary (maybe it isn't compiled)")

    # make sure it's executable -> win32 doesn't care
    if sys.platform == "cygwin":
        # Yes, we're unwinding the path join from above.
        path = parse_path(stress)
        short_bin = parse_bin(stress)
        add_exec_permission(path, short_bin)
    elif not os.access(stress, os.X_OK):
        try:
            # try to add user execute permissions
            # os.chmod doesn't work on Windows and isn't necessary unless in cygwin...
            if sys.platform == "cygwin":
                add_exec_permission(path, stress)
            else:
                os.chmod(stress, os.stat(stress).st_mode | stat.S_IXUSR)
        except:
            raise Exception(f"stress binary is not executable: {stress}")

    return stress


def isDse(install_dir):
    if install_dir is None:
        raise ArgumentError('Undefined installation directory')

    bin_dir = os.path.join(install_dir, BIN_DIR)

    if not os.path.exists(bin_dir):
        raise ArgumentError(f'Installation directory does not contain a bin directory: {install_dir}')

    dse_script = os.path.join(bin_dir, 'dse')
    return os.path.exists(dse_script)


def isScylla(install_dir):
    if install_dir is None:
        scylla_version = os.environ.get('SCYLLA_VERSION', None)
        if scylla_version:
            from ccmlib.scylla_repository import setup
            cdir, _ = setup(scylla_version)
            return cdir is not None

        scylla_docker_image = os.environ.get('SCYLLA_DOCKER_IMAGE', None)
        if scylla_docker_image:
            return True

        raise ArgumentError('Undefined installation directory')

    return (os.path.exists(os.path.join(install_dir, 'scylla')) or
            os.path.exists(os.path.join(install_dir, 'build', 'debug', 'scylla')) or
            os.path.exists(os.path.join(install_dir, 'build', 'dev', 'scylla')) or
            os.path.exists(os.path.join(install_dir, 'build', 'release', 'scylla')) or
            os.path.exists(os.path.join(install_dir, 'bin', 'scylla')))


def isOpscenter(install_dir):
    if install_dir is None:
        raise ArgumentError('Undefined installation directory')

    bin_dir = os.path.join(install_dir, BIN_DIR)

    if not os.path.exists(bin_dir):
        raise ArgumentError('Installation directory does not contain a bin directory')

    opscenter_script = os.path.join(bin_dir, 'opscenter')
    return os.path.exists(opscenter_script)


def scylla_extract_mode(path):
    # path/url examples:
    #   build/dev
    #   ../build/release
    #   /home/foo/scylla/build/debug
    #   build/dev/
    #   ../build/release/scylla
    #   url=../scylla/build/debug/scylla-package.tar.gz
    m = re.search(r'(^|/)build/(\w+)(/|$)', path)
    if m:
        return m.group(2)

    # path/url examples:
    #   /jenkins/data/relocatable/unstable/master/202001192256/scylla-package.tar.gz
    #   url=https://downloads.scylla.com/relocatable/unstable/master/202001192256/scylla-debug-package.tar.gz
    m = re.search(r'(^|/)(scylla|scylla-enterprise)(-(?P<mode>\w+))?(-(\w+))?-package([-./][\w./]+|$)', path)
    if m:
        mode = m.groupdict().get('mode')
        return mode if mode in ('debug', 'dev') else 'release'

    return None


def scylla_extract_install_dir_and_mode(install_dir):
    scylla_mode = scylla_extract_mode(install_dir)
    if scylla_mode:
        install_dir = str(os.path.join(install_dir, os.pardir, os.pardir))
    else:
        scylla_mode = 'release'
        if os.path.exists(os.path.join(install_dir, 'scylla-core-package')):
            try:
                f = open(os.path.join(install_dir, 'scylla-core-package', 'source.txt'), 'r')
                for l in f.readlines():
                    if l.startswith('url='):
                        scylla_mode = scylla_extract_mode(l) or scylla_mode
                f.close()
            except:
                pass
    return install_dir, scylla_mode


def wait_for(func: Callable, timeout: int, first: float = 0.0, step: float = 1.0, text: str = ""):
    """
    Wait until func() evaluates to True.

    If func() evaluates to True before timeout expires, return True.
    Otherwise, return False.

    param func: Function that will be evaluated.
    param timeout: Timeout in seconds.
    param first: Time to sleep in seconds before first attempt.
    param step: Time to sleep in seconds between attempts in seconds.
    param text: Text to print while waiting, for debug purposes.
    """
    start_time = time.time()
    end_time = time.time() + timeout

    time.sleep(first)

    while time.time() < end_time:
        if text:
            print(f"{text} ({time.time() - start_time:f} secs)")
        if func():
            return True
        time.sleep(step)
    return False


def wait_for_parallel_download_finish(placeholder_file):
    if not wait_for(func=lambda: not os.path.exists(placeholder_file), timeout=3600):
        raise TimeoutError(f"Relocatables download still runs in parallel from another test after 60 min. "
                           f"Placeholder file exists: {placeholder_file}")


def validate_install_dir(install_dir):
    if install_dir is None:
        raise ArgumentError('Undefined installation directory')

    # If relocatables download is running in parallel from another test, the install_dir exists with placehoslder file
    # in the folder. Once it will be downloaded and installed, this file will be removed.
    wait_for_parallel_download_finish(placeholder_file=os.path.join(install_dir, DOWNLOAD_IN_PROGRESS_FILE))

    # Windows requires absolute pathing on installation dir - abort if specified cygwin style
    if is_win():
        if ':' not in install_dir:
            raise ArgumentError(f'{install_dir} does not appear to be a cassandra or dse installation directory.  Please use absolute pathing (e.g. C:/cassandra.')

    bin_dir = os.path.join(install_dir, BIN_DIR)
    if isScylla(install_dir):
        install_dir, mode = scylla_extract_install_dir_and_mode(install_dir)
        bin_dir = install_dir
        conf_dir = os.path.join(install_dir, SCYLLA_CONF_DIR)
    elif isDse(install_dir):
        conf_dir = os.path.join(install_dir, DSE_CASSANDRA_CONF_DIR)
    elif isOpscenter(install_dir):
        conf_dir = os.path.join(install_dir, OPSCENTER_CONF_DIR)
    else:
        conf_dir = os.path.join(install_dir, CASSANDRA_CONF_DIR)
    cnd = os.path.exists(bin_dir)
    cnd = cnd and os.path.exists(conf_dir)
    if isScylla(install_dir):
        cnd = os.path.exists(os.path.join(conf_dir, SCYLLA_CONF))
    elif not isOpscenter(install_dir):
        cnd = cnd and os.path.exists(os.path.join(conf_dir, CASSANDRA_CONF))
    if not cnd:
        raise ArgumentError(f'{install_dir} does not appear to be a cassandra or dse installation directory')


def check_socket_available(itf):
    info = socket.getaddrinfo(itf[0], itf[1], socket.AF_UNSPEC, socket.SOCK_STREAM)
    if not info:
        raise UnavailableSocketError("Failed to get address info for [%s]:%s" % itf)

    (family, socktype, proto, canonname, sockaddr) = info[0]
    s = socket.socket(family, socktype)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        s.bind(sockaddr)
        s.close()
    except socket.error as msg:
        s.close()
        addr, port = itf
        raise UnavailableSocketError(f"Inet address {addr}:{port} is not available: {msg}")


def check_socket_listening(itf, timeout=60):
    def _check_socket_listening() -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as _socket:
            try:
                _socket.connect(itf)
                return True
            except socket.error:
                return False

    return wait_for(func=_check_socket_listening, timeout=timeout, step=0.2)


def interface_is_ipv6(itf):
    info = socket.getaddrinfo(itf[0], itf[1], socket.AF_UNSPEC, socket.SOCK_STREAM)
    if not info:
        raise UnavailableSocketError("Failed to get address info for [%s]:%s" % itf)

    return socket.AF_INET6 == info[0][0]

# note: does not handle collapsing hextets with leading zeros


def normalize_interface(itf):
    if not itf:
        return itf
    ip = itf[0]
    parts = ip.partition('::')
    if '::' in parts:
        missing_hextets = 9 - ip.count(':')
        zeros = '0'.join([':'] * missing_hextets)
        ip = ''.join(['0' if p == '' else zeros if p == '::' else p for p in ip.partition('::')])
    return (ip, itf[1])


def parse_settings(args):
    settings = {}
    for s in args:
        if is_win():
            # Allow for absolute path on Windows for value in key/value pair
            splitted = s.split(':', 1)
        else:
            splitted = s.split(':')
        if len(splitted) != 2:
            raise ArgumentError("A new setting should be of the form 'key: value', got " + s)
        key = splitted[0].strip()
        val = splitted[1].strip()
        # ok, that's not super beautiful
        if val.lower() == "true":
            val = True
        elif val.lower() == "false":
            val = False
        else:
            try:
                val = int(val)
            except ValueError:
                pass
        splitted = key.split('.')
        if len(splitted) == 2:
            try:
                settings[splitted[0]][splitted[1]] = val
            except KeyError:
                settings[splitted[0]] = {}
                settings[splitted[0]][splitted[1]] = val
        else:
            settings[key] = val
    return settings

#
# Copy file from source to destination with reasonable error handling
#


def copy_file(src_file, dst_file):
    try:
        shutil.copy2(src_file, dst_file)
    except (IOError, shutil.Error) as e:
        print(str(e), file=sys.stderr)
        exit(1)


def copy_directory(src_dir, dst_dir):
    for name in os.listdir(src_dir):
        filename = os.path.join(src_dir, name)
        if os.path.isfile(filename):
            shutil.copy(filename, dst_dir)


def get_version_from_build(install_dir=None, node_path=None):
    if install_dir is None and node_path is not None:
        install_dir = get_install_dir_from_cluster_conf(node_path)
    if install_dir is not None:
        if isScylla(install_dir):
            return _get_scylla_version(install_dir)
        # Binary cassandra installs will have a 0.version.txt file
        version_file = os.path.join(install_dir, '0.version.txt')
        if os.path.exists(version_file):
            with open(version_file) as f:
                return f.read().strip()
        # For DSE look for a dse*.jar and extract the version number
        dse_version = get_dse_version(install_dir)
        if (dse_version is not None):
            return dse_version
        # Source cassandra installs we can read from build.xml
        build = os.path.join(install_dir, 'build.xml')
        with open(build) as f:
            for line in f:
                match = re.search('name="base\.version" value="([0-9.]+)[^"]*"', line)
                if match:
                    return match.group(1)
    raise CCMError("Cannot find version")


def _get_scylla_version(install_dir):
    scylla_version_files = [
        os.path.join(install_dir, 'build', 'SCYLLA-VERSION-FILE'),
        os.path.join(install_dir, '..', '..', 'build', 'SCYLLA-VERSION-FILE'),
        os.path.join(install_dir, 'scylla-core-package', 'SCYLLA-VERSION-FILE'),
        os.path.join(install_dir, 'scylla-core-package', 'scylla', 'SCYLLA-VERSION-FILE'),
    ]
    for version_file in scylla_version_files:
        if os.path.exists(version_file):
            with open(version_file) as file:
                v = file.read().strip()
            # return only version strings (loosly) conforming to PEP-440
            # See https://www.python.org/dev/peps/pep-0440/
            # 'i.j(.|-)dev[N]' < 'i.j.rc[N]' < 'i.j.k' < i.j(.|-)post[N]
            if re.fullmatch('(\d+!)?\d+([.-]\d+)*([a-z]+\d*)?([.-](post|dev|rc)\d*)*', v):
                return v
    return '3.0'


def _get_scylla_release(install_dir):
    scylla_release_files = [
        os.path.join(install_dir, 'build', 'SCYLLA-RELEASE-FILE'),
        os.path.join(install_dir, 'scylla-core-package', 'SCYLLA-RELEASE-FILE'),
        os.path.join(install_dir, 'scylla-core-package', 'scylla', 'SCYLLA-RELEASE-FILE'),
    ]
    for release_file in scylla_release_files:
        if os.path.exists(release_file):
            with open(release_file) as file:
                return file.read().strip()
    raise ValueError("SCYLLA-RELEASE-FILE wasn't found")


def get_scylla_full_version(install_dir):
    return f'{_get_scylla_version(install_dir)}-{_get_scylla_release(install_dir)}'


def get_scylla_version(install_dir):
    if isScylla(install_dir):
        return _get_scylla_version(install_dir)
    else:
        return None


def get_dse_version(install_dir):
    for root, dirs, files in os.walk(install_dir):
        for file in files:
            match = re.search('^dse(?:-core)?-([0-9.]+)(?:-SNAPSHOT)?\.jar', file)
            if match:
                return match.group(1)
    return None


def get_dse_cassandra_version(install_dir):
    clib = os.path.join(install_dir, 'resources', 'cassandra', 'lib')
    for file in os.listdir(clib):
        if fnmatch.fnmatch(file, 'cassandra-all*.jar'):
            match = re.search('cassandra-all-([0-9.]+)(?:-.*)?\.jar', file)
            if match:
                return match.group(1)
    raise ArgumentError("Unable to determine Cassandra version in: " + install_dir)


def get_install_dir_from_cluster_conf(node_path):
    file = os.path.join(os.path.dirname(node_path), "cluster.conf")
    with open(file) as f:
        for line in f:
            match = re.search('install_dir: (.*?)$', line)
            if match:
                return match.group(1)
    return None


def is_dse_cluster(path):
    try:
        with open(os.path.join(path, 'CURRENT'), 'r') as f:
            name = f.readline().strip()
            cluster_path = os.path.join(path, name)
            filename = os.path.join(cluster_path, 'cluster.conf')
            with open(filename, 'r') as f:
                data = yaml.safe_load(f)
            if 'dse_dir' in data:
                return True
    except IOError:
        return False


def invalidate_cache():
    rmdirs(os.path.join(get_default_path(), 'repository'))


def get_jdk_version():
    version = subprocess.check_output(['java', '-version'], stderr=subprocess.STDOUT)
    ver_pattern = '\"(\d+\.\d+).*\"'
    return re.search(ver_pattern, str(version)).groups()[0]


def assert_jdk_valid_for_cassandra_version(cassandra_version):
    if cassandra_version >= '3.0' and get_jdk_version() < '1.8':
        print(f'ERROR: Cassandra 3.0+ requires Java >= 1.8, found Java {get_jdk_version()}')
        exit(1)


def aws_bucket_ls(s3_url):
    bucket_object = s3_url.replace('https://s3.amazonaws.com/', '').split('/')
    prefix = '/'.join(bucket_object[1:])
    s3_conn = Session().client(service_name='s3', config=Config(signature_version=UNSIGNED))
    paginator = s3_conn.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_object[0], Prefix=prefix)

    files_in_bucket = []
    for page in pages:
        if 'Contents' not in page:
            break

        for obj in page['Contents']:
            files_in_bucket.append(obj['Key'].replace(prefix + "/", ''))
    return files_in_bucket


def grouper(n, iterable, padvalue=None):
    """
    grouper(3, 'abcdefg', 'x') --> ('a','b','c'), ('d','e','f'), ('g','x','x')

    idea from: https://stackoverflow.com/a/312644/459189
    """
    return zip_longest(*[iter(iterable)] * n, fillvalue=padvalue)


def print_if_standalone(*args, debug_callback=None, end='\n', **kwargs):
    standalone = os.environ.get('SCYLLA_CCM_STANDALONE', None)
    if standalone:
        print(*args, *kwargs, end=end)
    else:
        debug_callback(*args, **kwargs)
