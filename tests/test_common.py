import tempfile
import os

import pytest
import ruamel

from ccmlib.common import scylla_extract_mode, LockFile, parse_settings


def test_scylla_extract_mode():
    assert scylla_extract_mode("build/dev") == 'dev'
    assert scylla_extract_mode("../build/release") == 'release'
    assert scylla_extract_mode("../build/release/scylla") == 'release'
    assert scylla_extract_mode("/home/foo/scylla/build/debug") == 'debug'
    assert scylla_extract_mode("url=../scylla/build/debug/scylla-package.tar.gz") == 'debug'

    assert scylla_extract_mode("url=./scylla-debug-x86_64-package.tar.gz") == 'debug'
    assert scylla_extract_mode("url=./scylla-x86_64-package.tar.gz") == 'release'
    assert scylla_extract_mode("url=./scylla-debug-aarch64-package.tar.gz") == 'debug'
    assert scylla_extract_mode("url=./scylla-package.tar.gz") == 'release'
    assert scylla_extract_mode("url=./scylla-debug-package.tar.gz") == 'debug'

    assert scylla_extract_mode("url=./scylla-debug-x86_64-package-4.5.2.0.20211114.26aca7b9f.tar.gz") == 'debug'
    assert scylla_extract_mode("url=./scylla-debug-package-4.5.2.0.20211114.26aca7b9f.tar.gz") == 'debug'
    assert scylla_extract_mode("url=./scylla-package-4.5.2.0.20211114.26aca7b9f.tar.gz") == 'release'

    assert scylla_extract_mode('url=https://s3.amazonaws.com/downloads.scylladb.com/downloads/scylla-enterprise/'
                               'relocatable/scylladb-2022.1/scylla-enterprise-x86_64-package-2022.1.rc6.0.20220523.'
                               '30ce52b2e.tar.gz') == 'release'
    assert scylla_extract_mode('url=https://s3.amazonaws.com/downloads.scylladb.com/downloads/scylla-enterprise/'
                               'relocatable/scylladb-2022.1/scylla-enterprise-debug-aarch64-package-2022.1.rc0.0.20220331.f3ee71fba.tar.gz') == 'debug'

    assert scylla_extract_mode("url=https://downloads.scylla.com/relocatable/unstable/master/202001192256/scylla-debug-package.tar.gz") == 'debug'
    assert scylla_extract_mode("url=https://downloads.scylladb.com/unstable/scylla/master/relocatable/latest/scylla-debug-unified-5.4.0~dev-0.20230801.37b548f46365.x86_64.tar.gz") == 'debug'
    assert scylla_extract_mode("url=https://s3.amazonaws.com/downloads.scylladb.com/unstable/scylla-enterprise/enterprise/relocatable/latest/scylla-enterprise-debug-unstripped-2023.3.0~dev-0.20230806.6dc3aeaf312c.aarch64.tar.gz") == 'debug'
    assert scylla_extract_mode("/jenkins/workspace/scylla-master/dtest-debug/scylla/build/debug/dist/tar/scylla-debug-unified-5.4.0~dev-0.20231013.055f0617064d.x86_64.tar.gz") == 'debug'

# Those tests assume that LockFile uses fcntl.flock
# If it switches to anything else, the tests need to be adjusted.

def test_lockfile_basic():
    f, path = tempfile.mkstemp(prefix='ccm-test-lockfile')
    lf = LockFile(path)
    assert lf.acquire(blocking=True) == (True, None)

    assert lf.read_contents() == (os.getpid(), '')
    lf.write_status('abc')
    assert lf.read_contents() == (os.getpid(), 'abc')

    lf.release()


def test_lockfile_locks():
    f, path = tempfile.mkstemp(prefix='ccm-test-lockfile')
    lf1 = LockFile(path)
    lf2 = LockFile(path)
    with lf1:
        assert lf2.acquire(blocking=False) == (False, os.getpid())
    assert lf2.acquire(blocking=False) == (True, None)
    assert lf1.acquire(blocking=False) == (False, os.getpid())
    lf2.release()


def test_lockfile_retain_status_by_default():
    f, path = tempfile.mkstemp(prefix='ccm-test-lockfile')
    lf = LockFile(path)

    assert lf.acquire(blocking=False)[0] is True
    lf.write_status('some_status_1')
    assert lf.read_status() == 'some_status_1'
    lf.release()

    # Status should be retained from previous lock.
    assert lf.acquire(blocking=False)[0] is True
    assert lf.read_status() == 'some_status_1'
    lf.release()

def test_parse_settings():
    res = parse_settings(["number:12", "string:somthing", "bool:false"])
    assert res == {'bool': False, 'number': 12, 'string': 'somthing'}

    res = parse_settings(["nested.number:12", "nested.string:somthing", "nested.bool:false"])
    assert res == {'nested': {'bool': False, 'number': 12, 'string': 'somthing'}}

    # a fix that need to be backported from upstream
    # https://github.com/riptano/ccm/commit/37afe6ab86fe03d5be7d0cf7a328dccac035a9a0
    # res = parse_settings(["double.nested.number:12", "double.nested.string:somthing", "double.nested.bool:false"])
    # assert res == {'double': {'nested': {'bool': False, 'number': 12, 'string': 'somthing'}}}

    res = parse_settings(["experimental_features:[udf,tablets]"])
    assert res == {'experimental_features': ['udf', 'tablets']}

    res = parse_settings(["experimental_features:['udf',\"tablets\"]"])
    assert res == {'experimental_features': ['udf', 'tablets']}

    # would break if incorrect yaml format is passed in the value
    with pytest.raises(ruamel.yaml.parser.ParserError):
        parse_settings(["experimental_features:['udf',\"tablets\""])
