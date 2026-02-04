"""
Tests for max_matches functionality in grep_log and grep_log_for_errors.
Validates the limit enforcement to prevent OOM with huge log files.
"""

import os
from ccmlib.node import _grep_log_for_errors


class TestMaxLogMatches:
    """Test suite for max_matches parameter in log processing functions."""

    def test_grep_log_for_errors_with_limit(self):
        """Test that _grep_log_for_errors respects max_matches limit."""
        # Create a log with multiple error lines
        log_content = """\
INFO  [main] 2024-01-01 10:00:00,000 Node.java:100 - Starting node
ERROR [main] 2024-01-01 10:00:01,000 Node.java:101 - Error 1
    at com.example.Class1.method1()
    at com.example.Class2.method2()
INFO  [main] 2024-01-01 10:00:02,000 Node.java:102 - Processing
ERROR [main] 2024-01-01 10:00:03,000 Node.java:103 - Error 2
    at com.example.Class3.method3()
INFO  [main] 2024-01-01 10:00:04,000 Node.java:104 - Processing
ERROR [main] 2024-01-01 10:00:05,000 Node.java:105 - Error 3
    at com.example.Class4.method4()
INFO  [main] 2024-01-01 10:00:06,000 Node.java:106 - Done
ERROR [main] 2024-01-01 10:00:07,000 Node.java:107 - Error 4
    at com.example.Class5.method5()
INFO  [main] 2024-01-01 10:00:08,000 Node.java:108 - Complete
"""
        
        # Test with limit of 2 errors
        errors = _grep_log_for_errors(log_content, distinct_errors=False, max_matches=2)
        assert len(errors) == 2, f"Expected 2 errors but got {len(errors)}"
        
        # Verify the first two errors were captured
        assert "Error 1" in errors[0][0]
        assert "Error 2" in errors[1][0]

    def test_grep_log_for_errors_unlimited(self):
        """Test that max_matches=0 means unlimited."""
        log_content = """\
ERROR [main] 2024-01-01 10:00:01,000 Node.java:101 - Error 1
INFO  [main] 2024-01-01 10:00:02,000 Node.java:102 - Processing
ERROR [main] 2024-01-01 10:00:03,000 Node.java:103 - Error 2
INFO  [main] 2024-01-01 10:00:04,000 Node.java:104 - Processing
ERROR [main] 2024-01-01 10:00:05,000 Node.java:105 - Error 3
INFO  [main] 2024-01-01 10:00:06,000 Node.java:106 - Done
"""
        
        # Test with max_matches=0 (unlimited)
        errors = _grep_log_for_errors(log_content, distinct_errors=False, max_matches=0)
        assert len(errors) == 3, f"Expected 3 errors but got {len(errors)}"

    def test_grep_log_for_errors_default_from_env(self):
        """Test that default max_matches comes from DTEST_MAX_LOG_MATCHES env var."""
        log_content = """\
ERROR [main] 2024-01-01 10:00:01,000 Node.java:101 - Error 1
INFO  [main] 2024-01-01 10:00:02,000 Node.java:102 - Processing
ERROR [main] 2024-01-01 10:00:03,000 Node.java:103 - Error 2
INFO  [main] 2024-01-01 10:00:04,000 Node.java:104 - Processing
ERROR [main] 2024-01-01 10:00:05,000 Node.java:105 - Error 3
INFO  [main] 2024-01-01 10:00:06,000 Node.java:106 - Done
"""
        
        # Test with environment variable set to 2
        original_value = os.environ.get('DTEST_MAX_LOG_MATCHES')
        try:
            os.environ['DTEST_MAX_LOG_MATCHES'] = '2'
            errors = _grep_log_for_errors(log_content, distinct_errors=False, max_matches=None)
            assert len(errors) == 2, f"Expected 2 errors (from env var) but got {len(errors)}"
        finally:
            # Restore original value
            if original_value is None:
                os.environ.pop('DTEST_MAX_LOG_MATCHES', None)
            else:
                os.environ['DTEST_MAX_LOG_MATCHES'] = original_value

    def test_grep_log_for_errors_distinct_with_limit(self):
        """Test that max_matches works with distinct_errors=True.
        
        Note: distinct_errors deduplication is based on exact string match.
        The same error message from different lines will not be deduplicated 
        unless the full line (including timestamps, line numbers) is identical.
        """
        log_content = """\
ERROR [main] 2024-01-01 10:00:01,000 Node.java:101 - Error A
INFO  [main] 2024-01-01 10:00:02,000 Node.java:102 - Processing
ERROR [main] 2024-01-01 10:00:03,000 Node.java:103 - Error B
INFO  [main] 2024-01-01 10:00:04,000 Node.java:104 - Processing
ERROR [main] 2024-01-01 10:00:01,000 Node.java:101 - Error A
INFO  [main] 2024-01-01 10:00:06,000 Node.java:106 - Processing
ERROR [main] 2024-01-01 10:00:07,000 Node.java:107 - Error C
INFO  [main] 2024-01-01 10:00:08,000 Node.java:108 - Done
"""
        
        # With distinct_errors, limit is applied first, then deduplication
        # max_matches=2 collects first 2 errors (A, B)
        errors = _grep_log_for_errors(log_content, distinct_errors=True, max_matches=2)
        assert len(errors) == 2, f"Expected 2 distinct errors but got {len(errors)}"
        
        # max_matches=3 collects first 3 errors (A, B, A again)
        # Lines 90 and 94 are intentionally identical (same timestamp) to demonstrate deduplication
        # After deduplication, we get 2 unique errors (A and B)
        errors = _grep_log_for_errors(log_content, distinct_errors=True, max_matches=3)
        assert len(errors) == 2, f"Expected 2 distinct errors after dedup but got {len(errors)}"

    def test_grep_log_for_errors_backward_compatibility(self):
        """Test that existing code without max_matches still works."""
        log_content = """\
ERROR [main] 2024-01-01 10:00:01,000 Node.java:101 - Error 1
INFO  [main] 2024-01-01 10:00:02,000 Node.java:102 - Processing
ERROR [main] 2024-01-01 10:00:03,000 Node.java:103 - Error 2
INFO  [main] 2024-01-01 10:00:04,000 Node.java:104 - Done
"""
        
        # Call without max_matches parameter (should use default from env or 1000)
        original_value = os.environ.get('DTEST_MAX_LOG_MATCHES')
        try:
            # Clear env var to test default of 1000
            os.environ.pop('DTEST_MAX_LOG_MATCHES', None)
            errors = _grep_log_for_errors(log_content, distinct_errors=False)
            # Should get all errors since we have less than 1000
            assert len(errors) == 2, f"Expected 2 errors but got {len(errors)}"
        finally:
            # Restore original value
            if original_value is not None:
                os.environ['DTEST_MAX_LOG_MATCHES'] = original_value
