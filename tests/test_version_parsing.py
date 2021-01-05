import pytest
from pathlib import Path

from ccmlib.common import get_scylla_version


@pytest.mark.parametrize("version_to_test", ['4.2', '4.2.rc1', '4.2.rc111', '4.2.dev'])
def test_valid_versions(tmpdir, version_to_test):

    # mock it's a scylla dir
    (Path(tmpdir) / 'scylla').touch()

    # mock the version file
    (Path(tmpdir) / 'build').mkdir()
    with (Path(tmpdir) / 'build' / 'SCYLLA-VERSION-FILE').open(mode='w') as f:
        f.write(version_to_test)

    version = get_scylla_version(install_dir=tmpdir)
    assert version == version_to_test


@pytest.mark.parametrize("version_to_test", ['666.development', 'strings_only'])
def test_invalid_versions(tmpdir, version_to_test):

    # mock it's a scylla dir
    (Path(tmpdir) / 'scylla').touch()

    # mock the version file
    (Path(tmpdir) / 'build').mkdir()
    with (Path(tmpdir) / 'build' / 'SCYLLA-VERSION-FILE').open(mode='w') as f:
        f.write(version_to_test)

    version = get_scylla_version(install_dir=tmpdir)
    assert version == '3.0'
