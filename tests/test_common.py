from ccmlib.common import scylla_extract_mode


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
