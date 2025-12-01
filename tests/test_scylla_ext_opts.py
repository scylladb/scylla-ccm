"""
Tests for SCYLLA_EXT_OPTS handling, specifically for short-form options.

This addresses the issue where -c (shorthand for --smp) and -m (shorthand for --memory)
in SCYLLA_EXT_OPTS would cause Scylla startup to fail with duplicate command line options.

The fix normalizes short form options (-c, -m) to their long form equivalents (--smp, --memory)
in the process_opts function in scylla_node.py.
"""

import pytest

from ccmlib.scylla_node import process_opts, OPTION_ALIASES


class TestProcessOptsShortForms:
    """Tests to verify that short-form options are properly handled."""

    def test_short_smp_option(self):
        """Verify that -c is normalized to --smp."""
        opts = "-c 2".split()
        result = process_opts(opts)
        assert '--smp' in result
        assert result['--smp'] == ['2']
        assert '-c' not in result

    def test_short_memory_option(self):
        """Verify that -m is normalized to --memory."""
        opts = "-m 512M".split()
        result = process_opts(opts)
        assert '--memory' in result
        assert result['--memory'] == ['512M']
        assert '-m' not in result

    def test_long_smp_option_unchanged(self):
        """Long form --smp should still work."""
        opts = "--smp 4".split()
        result = process_opts(opts)
        assert '--smp' in result
        assert result['--smp'] == ['4']

    def test_long_memory_option_unchanged(self):
        """Long form --memory should still work."""
        opts = "--memory 1G".split()
        result = process_opts(opts)
        assert '--memory' in result
        assert result['--memory'] == ['1G']

    def test_mixed_short_and_long_options(self):
        """Test mixed short and long options."""
        opts = "-c 2 --memory 512M --developer-mode true".split()
        result = process_opts(opts)
        assert '--smp' in result
        assert result['--smp'] == ['2']
        assert '--memory' in result
        assert result['--memory'] == ['512M']
        assert '--developer-mode' in result
        assert result['--developer-mode'] == ['true']

    def test_short_options_with_equals_syntax(self):
        """Test short options with equals syntax."""
        opts = "-c=2".split()
        result = process_opts(opts)
        assert '--smp' in result
        assert result['--smp'] == ['2']

    def test_memory_short_form_with_equals(self):
        """Test -m=value syntax."""
        opts = "-m=1024M".split()
        result = process_opts(opts)
        assert '--memory' in result
        assert result['--memory'] == ['1024M']

    def test_both_short_options(self):
        """Test both -c and -m together."""
        opts = "-c 2 -m 512M".split()
        result = process_opts(opts)
        assert '--smp' in result
        assert result['--smp'] == ['2']
        assert '--memory' in result
        assert result['--memory'] == ['512M']
        assert '-c' not in result
        assert '-m' not in result

    def test_other_options_not_affected(self):
        """Ensure other options are not affected by normalization."""
        opts = "--developer-mode true -c 2 --default-log-level info".split()
        result = process_opts(opts)
        assert '--developer-mode' in result
        assert '--smp' in result
        assert '--default-log-level' in result
        assert result['--developer-mode'] == ['true']
        assert result['--smp'] == ['2']
        assert result['--default-log-level'] == ['info']

    def test_scylla_manager_options_excluded(self):
        """Verify that scylla-manager options are still excluded."""
        opts = "--scylla-manager-auth-token abc123 -c 2".split()
        result = process_opts(opts)
        assert '--scylla-manager-auth-token' not in result
        assert '--smp' in result
        assert result['--smp'] == ['2']

    def test_option_aliases_contains_expected_mappings(self):
        """Verify the OPTION_ALIASES constant has the expected mappings."""
        assert OPTION_ALIASES['-c'] == '--smp'
        assert OPTION_ALIASES['-m'] == '--memory'
