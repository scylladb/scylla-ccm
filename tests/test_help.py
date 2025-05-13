def test_help_works(ccm_reloc_cluster):
    proc = ccm_reloc_cluster.run_command([ccm_reloc_cluster.ccm_bin])
    (stdout, _) = proc.communicate()
    # No error messages
    assert 'Internal error' not in stdout
    assert 'unknown command' not in stdout
    # CCM always returns 1 when printing help
    assert proc.returncode == 1
    # Check some of the commends that should be present in the help
    # Pause and resume are chosen, because they are low on the list.
    # If some command above them failed, they would not be shown.
    assert 'pause' in stdout
    assert 'resume' in stdout
    assert 'nodetool' in stdout
    assert 'show' in stdout
