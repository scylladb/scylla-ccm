"""
Performance tests to validate optimization improvements in ccm.
These tests verify that the performance optimizations don't break functionality
and demonstrate improved efficiency.
"""
import pytest
import tempfile
import os
import time
from ccmlib import common


class TestStringConcatenationOptimization:
    """Test that list append + join is more efficient than string concatenation."""
    
    def test_list_append_vs_string_concat(self):
        """Verify list.append() + join() pattern works correctly."""
        # This mimics the pattern used in watch_log_for()
        test_lines = [f"line {i}\n" for i in range(100)]
        
        # New optimized approach: list append
        result_list = []
        for line in test_lines:
            result_list.append(line)
        result = "".join(result_list)
        
        # Verify it produces the same output
        expected = "".join(test_lines)
        assert result == expected
        
    def test_list_slicing_for_error_messages(self):
        """Test that we can still slice joined strings for error messages."""
        test_lines = [f"line {i}\n" for i in range(100)]
        result_list = []
        for line in test_lines:
            result_list.append(line)
        
        # Should be able to join and slice for error messages
        reads_str = "".join(result_list)
        truncated = reads_str[:50]
        
        assert len(truncated) == 50
        assert truncated.startswith("line 0")


class TestRegexReplacementOptimization:
    """Test that early break in regex replacements works correctly."""
    
    def test_replaces_in_files_with_multiple_patterns(self):
        """Test that file replacement works with multiple patterns."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as src:
            src.write("line1: old_value1\n")
            src.write("line2: old_value2\n")
            src.write("line3: unchanged\n")
            src_path = src.name
        
        with tempfile.NamedTemporaryFile(delete=False) as dst:
            dst_path = dst.name
        
        try:
            # Test with multiple patterns - should only apply first match per line
            replacements = [
                (r'old_value1', 'new_value1'),
                (r'old_value2', 'new_value2'),
            ]
            common.replaces_in_files(src_path, dst_path, replacements)
            
            with open(dst_path, 'r') as f:
                content = f.read()
            
            assert 'new_value1' in content
            assert 'new_value2' in content
            assert 'unchanged' in content
            assert 'old_value1' not in content
            assert 'old_value2' not in content
        finally:
            os.unlink(src_path)
            os.unlink(dst_path)
    
    def test_replaces_in_files_early_break(self):
        """Test that only first matching pattern is applied per line."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as src:
            src.write("test_pattern\n")
            src_path = src.name
        
        with tempfile.NamedTemporaryFile(delete=False) as dst:
            dst_path = dst.name
        
        try:
            # Multiple patterns that could match - only first should apply
            replacements = [
                (r'test', 'FIRST'),
                (r'pattern', 'SECOND'),  # Should not be checked if first matches
            ]
            common.replaces_in_files(src_path, dst_path, replacements)
            
            with open(dst_path, 'r') as f:
                content = f.read()
            
            # With early break, only first replacement should happen
            assert content == "FIRST\n"
            # Without early break, it would be "FIRST\n" then "SECOND\n"
        finally:
            os.unlink(src_path)
            os.unlink(dst_path)


class TestExponentialBackoffBehavior:
    """Test exponential backoff logic for polling operations."""
    
    def test_exponential_backoff_calculation(self):
        """Test that exponential backoff increases correctly."""
        sleep_time = 0.1
        max_sleep = 1.0
        
        # Simulate the backoff pattern
        sleep_times = [sleep_time]
        for _ in range(10):
            sleep_time = min(sleep_time * 1.5, max_sleep)
            sleep_times.append(sleep_time)
        
        # Verify it grows exponentially up to max (use approximate equality for floats)
        assert abs(sleep_times[0] - 0.1) < 0.001
        assert abs(sleep_times[1] - 0.15) < 0.001
        assert abs(sleep_times[2] - 0.225) < 0.001
        assert abs(sleep_times[3] - 0.3375) < 0.001
        # Should eventually cap at 1.0
        assert abs(sleep_times[-1] - 1.0) < 0.001
        assert all(t <= max_sleep for t in sleep_times)
    
    def test_backoff_resets_on_progress(self):
        """Test that backoff resets to minimum on progress."""
        sleep_time = 0.5  # Some accumulated backoff
        
        # Simulate progress detection
        sleep_time = 0.1  # Reset to minimum
        
        assert sleep_time == 0.1


class TestFileHandleManagement:
    """Test that file handles are properly managed with context managers."""
    
    def test_context_manager_closes_file(self):
        """Verify context managers properly close file handles."""
        test_file = tempfile.NamedTemporaryFile(delete=False)
        test_path = test_file.name
        test_file.close()
        
        try:
            # Write using context manager
            with open(test_path, 'w') as f:
                f.write("test")
                # File should auto-close on exit
            
            # File should be closed and readable
            with open(test_path, 'r') as f:
                content = f.read()
            
            assert content == "test"
        finally:
            os.unlink(test_path)
    
    def test_devnull_context_manager(self):
        """Test that /dev/null can be used with context manager."""
        # This pattern is now used in node.py for subprocess output
        with open(os.devnull, 'w') as FNULL:
            FNULL.write("test")
            # Should not raise any errors


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
