"""
Test that build modes are correctly extracted from relocatable packages.
"""
import pytest

from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.common import scylla_extract_install_dir_and_mode
from ccmlib.scylla_repository import CORE_PACKAGE_DIR_NAME, SOURCE_FILE_NAME


def test_scylla_mode_extraction_from_install_dir(tmp_path):
    """Test that scylla_mode is extracted from install_dir when both install_dir and cassandra_version are provided."""
    
    # Create a mock install directory structure similar to relocatable packages
    install_dir = tmp_path / "scylla-install"
    core_package_dir = install_dir / CORE_PACKAGE_DIR_NAME
    core_package_dir.mkdir(parents=True, exist_ok=True)
    
    # Test debug mode
    source_file = core_package_dir / SOURCE_FILE_NAME
    source_file.write_text("url=https://downloads.scylla.com/relocatable/unstable/master/202001192256/scylla-debug-package.tar.gz\n")
    
    extracted_dir, mode = scylla_extract_install_dir_and_mode(str(install_dir))
    assert mode == 'debug', f"Expected mode 'debug', got '{mode}'"
    
    # Test release mode
    source_file.write_text("url=https://downloads.scylla.com/relocatable/unstable/master/202001192256/scylla-package.tar.gz\n")
    
    extracted_dir, mode = scylla_extract_install_dir_and_mode(str(install_dir))
    assert mode == 'release', f"Expected mode 'release', got '{mode}'"
    
    # Test dev mode
    source_file.write_text("url=https://downloads.scylla.com/relocatable/unstable/master/202001192256/scylla-dev-package.tar.gz\n")
    
    extracted_dir, mode = scylla_extract_install_dir_and_mode(str(install_dir))
    assert mode == 'dev', f"Expected mode 'dev', got '{mode}'"


def test_scylla_cluster_init_extracts_mode_from_install_dir(tmp_path):
    """Test that ScyllaCluster.__init__ extracts build mode from install_dir when cassandra_version is also provided."""
    
    # Create a mock install directory with debug mode
    install_dir = tmp_path / "scylla-install-debug"
    core_package_dir = install_dir / CORE_PACKAGE_DIR_NAME
    core_package_dir.mkdir(parents=True, exist_ok=True)
    
    source_file = core_package_dir / SOURCE_FILE_NAME
    source_file.write_text("url=https://downloads.scylla.com/relocatable/unstable/master/202001192256/scylla-debug-package.tar.gz\n")
    
    # Test that mode is extracted during __init__ before calling parent
    cluster = ScyllaCluster.__new__(ScyllaCluster)
    cluster.started = False
    cluster.force_wait_for_cluster_start = False
    cluster._scylla_manager = None
    cluster.skip_manager_server = False
    cluster.scylla_version = "unstable/master:202001192256"
    cluster.scylla_reloc = True
    
    # This is the key part - extract mode from install_dir
    from ccmlib.common import scylla_extract_install_dir_and_mode
    _, cluster.scylla_mode = scylla_extract_install_dir_and_mode(str(install_dir))
    
    # Set timeouts
    cluster.default_wait_other_notice_timeout = 120 if cluster.scylla_mode != 'debug' else 600
    cluster.default_wait_for_binary_proto = 420 if cluster.scylla_mode != 'debug' else 900
    
    # Verify that the mode was extracted correctly
    assert cluster.scylla_mode == 'debug', f"Expected mode 'debug', got '{cluster.scylla_mode}'"
    
    # Verify that timeouts are set correctly for debug mode
    assert cluster.default_wait_other_notice_timeout == 600, \
        f"Expected timeout 600 for debug mode, got {cluster.default_wait_other_notice_timeout}"
    assert cluster.default_wait_for_binary_proto == 900, \
        f"Expected timeout 900 for debug mode, got {cluster.default_wait_for_binary_proto}"


@pytest.mark.parametrize('mode,expected_notice_timeout,expected_binary_timeout', [
    ('debug', 600, 900),
    ('release', 120, 420),
    ('dev', 120, 420),
])
def test_scylla_cluster_timeout_settings(tmp_path, mode, expected_notice_timeout, expected_binary_timeout):
    """Test that timeouts are correctly set based on build mode."""
    
    install_dir = tmp_path / f"scylla-install-{mode}"
    core_package_dir = install_dir / CORE_PACKAGE_DIR_NAME
    core_package_dir.mkdir(parents=True, exist_ok=True)
    
    # Create source file with appropriate mode
    source_file = core_package_dir / SOURCE_FILE_NAME
    mode_suffix = f'-{mode}' if mode != 'release' else ''
    source_file.write_text(f"url=https://downloads.scylla.com/relocatable/unstable/master/202001192256/scylla{mode_suffix}-package.tar.gz\n")
    
    # Extract mode
    _, extracted_mode = scylla_extract_install_dir_and_mode(str(install_dir))
    assert extracted_mode == mode, f"Expected mode '{mode}', got '{extracted_mode}'"
    
    # Test timeout calculation
    notice_timeout = 120 if extracted_mode != 'debug' else 600
    binary_timeout = 420 if extracted_mode != 'debug' else 900
    
    assert notice_timeout == expected_notice_timeout, \
        f"For mode '{mode}', expected notice timeout {expected_notice_timeout}, got {notice_timeout}"
    assert binary_timeout == expected_binary_timeout, \
        f"For mode '{mode}', expected binary timeout {expected_binary_timeout}, got {binary_timeout}"
