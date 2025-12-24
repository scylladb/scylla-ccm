import tempfile
import os
import pytest

from ccmlib.common import get_version_from_build, CCMError


def test_get_version_from_build_missing_scylla_binary():
    """Test that we get a helpful error when scylla binary is missing"""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a directory that looks like a Cassandra build (not Scylla)
        # Just having build/release directory is not enough - needs actual binary
        os.makedirs(os.path.join(tmpdir, 'build', 'release'))
        
        with pytest.raises(CCMError) as exc_info:
            get_version_from_build(tmpdir)
        
        error_msg = str(exc_info.value)
        # Since no scylla binary exists, it will be treated as Cassandra
        assert "Cannot find version information" in error_msg
        assert tmpdir in error_msg


def test_get_version_from_build_missing_scylla_version_file():
    """Test that we get a helpful error when SCYLLA-VERSION-FILE is missing"""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a directory with a scylla binary but no version file
        os.makedirs(os.path.join(tmpdir, 'build', 'release'))
        scylla_bin = os.path.join(tmpdir, 'build', 'release', 'scylla')
        with open(scylla_bin, 'w') as f:
            f.write('#!/bin/bash\necho "scylla"\n')
        
        with pytest.raises(CCMError) as exc_info:
            get_version_from_build(tmpdir)
        
        error_msg = str(exc_info.value)
        assert "Could not find SCYLLA-VERSION-FILE" in error_msg
        assert "SCYLLA-VERSION-FILE" in error_msg
        assert tmpdir in error_msg


def test_get_version_from_build_missing_cassandra_build_xml():
    """Test that we get a helpful error when build.xml is missing for Cassandra"""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a directory that looks like a Cassandra install but has no version info
        os.makedirs(os.path.join(tmpdir, 'bin'))
        os.makedirs(os.path.join(tmpdir, 'conf'))
        
        # Create dummy files to make it look like Cassandra
        cassandra_bin = os.path.join(tmpdir, 'bin', 'cassandra')
        with open(cassandra_bin, 'w') as f:
            f.write('#!/bin/bash\n')
        
        cassandra_yaml = os.path.join(tmpdir, 'conf', 'cassandra.yaml')
        with open(cassandra_yaml, 'w') as f:
            f.write('cluster_name: test\n')
        
        with pytest.raises(CCMError) as exc_info:
            get_version_from_build(tmpdir)
        
        error_msg = str(exc_info.value)
        assert "Cannot find version information" in error_msg
        assert "build.xml" in error_msg
        assert tmpdir in error_msg


def test_get_version_from_build_with_valid_scylla_version_file():
    """Test that we can read version from SCYLLA-VERSION-FILE"""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a directory with scylla binary and version file
        os.makedirs(os.path.join(tmpdir, 'build', 'release'))
        
        scylla_bin = os.path.join(tmpdir, 'build', 'release', 'scylla')
        with open(scylla_bin, 'w') as f:
            f.write('#!/bin/bash\n')
        
        version_file = os.path.join(tmpdir, 'build', 'SCYLLA-VERSION-FILE')
        with open(version_file, 'w') as f:
            f.write('5.2.0-dev\n')
        
        version = get_version_from_build(tmpdir)
        assert version == '5.2.0-dev'


def test_get_version_from_build_with_valid_cassandra_0_version_txt():
    """Test that we can read version from 0.version.txt"""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a directory with 0.version.txt
        version_file = os.path.join(tmpdir, '0.version.txt')
        with open(version_file, 'w') as f:
            f.write('4.0.5\n')
        
        version = get_version_from_build(tmpdir)
        assert version == '4.0.5'
