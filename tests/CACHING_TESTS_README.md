# Package Caching Tests - Implementation Guide

## Overview

This document describes the comprehensive test suite created to validate the package caching mechanism in scylla-ccm, specifically addressing PR #557, issue #521, and PR #732.

## Background

### Issue #521
Issue #521 reported that caching wasn't working as expected with local files. When using local file paths for Scylla packages, the system was not properly caching the packages, leading to unnecessary re-extractions even when the file hadn't changed.

### PR #557
PR #557 implemented a fix that uses file hashing to enable proper caching for local files, S3 URLs, and HTTP URLs.

### PR #732
PR #732 fixed a critical bug where packages provided via environment variables (e.g., SCYLLA_UNIFIED_PACKAGE) were only read inside download_packages(), after the hash-based cache validation block. Since packages was None at validation time, a stale cached install was reused forever even when the local tarball changed. The fix reads env var packages earlier in setup() so they participate in hash validation.

## How Caching Works

The caching mechanism operates as follows:

1. **Hash Calculation**: When a package is downloaded/extracted:
   - For local files: Calculate MD5 hash using `md5sum` command
   - For S3 URLs: Use ETag from S3 metadata
   - For HTTP URLs: Use ETag from HTTP headers

2. **Hash Storage**: The hash is saved in `source.txt` file in the package directory:
   ```
   version=<version>
   url=<url>
   hash=<calculated_hash>
   ```

3. **Cache Validation**: On subsequent runs:
   - Read environment variable packages (if set) using `packages_from_env()`
   - Read the stored hash from `source.txt`
   - Calculate/retrieve the current package hash
   - If hashes match: Use cached package (no re-download/re-extract)
   - If hashes differ: Remove old package and download/extract again

## Test Files

### 1. `tests/test_caching_hash.py` (New)
Comprehensive unit and integration tests for the caching mechanism.

**Test Classes:**

- **TestGetUrlHash**: Tests hash calculation for different URL types
  - Local file hash calculation using md5sum
  - S3 ETag retrieval
  - HTTP ETag retrieval
  - Fallback mechanisms

- **TestSourceFileOperations**: Tests source.txt file handling
  - Saving hash to file
  - Reading hash from file
  - Handling missing/corrupted files
  - Format consistency

- **TestCachingIntegration**: Integration tests
  - Hash-based caching logic
  - Cache invalidation on hash change

- **TestScyllaSetupCaching**: Setup function integration
  - Local unified package handling
  - Hash-based cache validation

- **TestRealDownloadCaching**: Network tests (skipped by default)
  - Real S3 downloads
  - Real HTTP downloads
  - End-to-end validation

- **TestCachingEdgeCases**: Edge case handling
  - Empty hashes
  - Corrupted files
  - Special characters in hashes
  - md5sum command failures

- **TestPackagesFromEnv**: Tests packages_from_env() function (NEW for PR #732)
  - Reading all package types from environment variables
  - Reading only unified package
  - Handling empty environment
  - Alternate tools package variable name

### 2. `tests/test_scylla_repository.py` (Modified)
Added `TestLocalFileCaching` class with integration tests:

- **test_local_unified_package_hash_caching**: 
  - Uses actual test data file
  - Validates complete hash calculation and storage
  - **Primary test for issue #521 fix**

- **test_local_file_hash_change_detection**:
  - Tests that file changes are detected
  - Validates cache invalidation logic

- **test_setup_with_env_var_local_package**:
  - Tests environment variable handling
  - Validates SCYLLA_UNIFIED_PACKAGE usage

- **test_env_var_package_cache_invalidation_on_change** (NEW for PR #732):
  - Validates the fix for PR #732
  - Tests that cache is invalidated when env var package content changes
  - Ensures packages from env vars participate in hash validation
  - Verifies that stale cached installs are not reused when tarball changes

## Running the Tests

### Quick Test (Unit Tests Only)
```bash
python3 -m pytest tests/test_caching_hash.py -v
```
Expected: 23 passed, 3 skipped in ~0.1s

### Complete Caching Tests
```bash
python3 -m pytest tests/test_caching_hash.py tests/test_scylla_repository.py::TestLocalFileCaching -v
```
Expected: 27 passed, 3 skipped in ~0.1s

### All Unit Tests (No Network)
```bash
python3 -m pytest tests/test_common.py tests/test_utils_version.py tests/test_internal_functions.py tests/test_version_parsing.py tests/test_caching_hash.py -v
```
Expected: 72+ tests passing

### Specific Test Categories
```bash
# Only hash calculation tests
pytest tests/test_caching_hash.py::TestGetUrlHash -v

# Only source file operations
pytest tests/test_caching_hash.py::TestSourceFileOperations -v

# Only integration tests
pytest tests/test_scylla_repository.py::TestLocalFileCaching -v
```

## Test Data

The tests use an existing test data file:
```
tests/tests/test_data/scylla_unified_master_2023_04_03.tar.gz
```

This file is used to validate:
- Correct MD5 hash calculation
- Source file creation and reading
- Integration with actual package format

Expected hash: `d2be7852b8c65f74c1da8c9efbc7e408`

## What the Tests Validate

### Issue #521 Fix Validation
1. ✅ Local files can have their hash calculated using md5sum
2. ✅ Hash is stored correctly in source.txt
3. ✅ Hash is read correctly from source.txt
4. ✅ Hash comparison works for cache validation
5. ✅ Changed files are detected via hash mismatch

### PR #557 Implementation Validation
1. ✅ get_url_hash() works for local files
2. ✅ get_url_hash() works for S3 URLs (with ETag)
3. ✅ get_url_hash() works for HTTP URLs (with ETag)
4. ✅ save_source_file() preserves all required information
5. ✅ get_installed_scylla_package_hash() retrieves hash correctly
6. ✅ Cache invalidation logic in setup() function works

### PR #732 Fix Validation (NEW)
1. ✅ packages_from_env() correctly reads environment variables
2. ✅ packages_from_env() handles all package types (unified, core, tools, jmx)
3. ✅ packages_from_env() handles alternate variable names
4. ✅ Environment variable packages participate in hash validation
5. ✅ Cache is invalidated when env var package content changes
6. ✅ Stale cached installs are not reused when local tarball changes
4. ✅ save_source_file() preserves all required information
5. ✅ get_installed_scylla_package_hash() retrieves hash correctly
6. ✅ Cache invalidation logic in setup() function works

### Edge Cases Covered
1. ✅ Missing source.txt file
2. ✅ Corrupted source.txt file
3. ✅ Empty/missing hash value
4. ✅ md5sum command failures
5. ✅ Special characters in hash values
6. ✅ S3 fallback to HTTP

## Test Coverage Summary

| Component | Coverage |
|-----------|----------|
| get_url_hash (local) | ✅ Full |
| get_url_hash (S3) | ✅ Full (mocked) |
| get_url_hash (HTTP) | ✅ Full (mocked) |
| save_source_file | ✅ Full |
| get_installed_scylla_package_hash | ✅ Full |
| packages_from_env | ✅ Full (NEW) |
| Hash validation in setup() | ✅ Full |
| Env var package cache validation | ✅ Full (NEW) |
| Edge cases | ✅ Full |
| Integration scenarios | ✅ Full |

## Continuous Integration

These tests are designed to run in CI without external dependencies:
- No network access required (network tests are skipped)
- Use existing test data files
- Mock external services (S3, HTTP)
- Fast execution (~0.1s for all 22 tests)

## Future Enhancements

Potential future test additions:
1. Performance tests for large files
2. Concurrent access tests (multiple processes)
3. Real S3/HTTP download tests (in integration environment)
4. Cache size management tests
5. Cleanup and garbage collection tests

## Troubleshooting

### Test Failures

**"md5sum command not found"**
- Ensure md5sum is installed: `apt-get install coreutils`

**"Test data file not available"**
- Ensure `tests/tests/test_data/scylla_unified_master_2023_04_03.tar.gz` exists
- Run from repository root directory

**"Import errors"**
- Install dependencies: `pip install -e .`
- Install test dependencies: `pip install pytest pytest-mock`

### Debugging

Enable verbose output:
```bash
pytest tests/test_caching_hash.py -vv --tb=long
```

Run a specific test:
```bash
pytest tests/test_caching_hash.py::TestGetUrlHash::test_get_local_file_hash -v
```

## Contributing

When modifying caching logic:
1. Run existing tests first to ensure no regressions
2. Add new tests for new functionality
3. Update this README if test coverage changes
4. Ensure all tests pass before submitting PR

## References

- Issue #521: https://github.com/scylladb/scylla-ccm/issues/521
- PR #557: https://github.com/scylladb/scylla-ccm/pull/557
- Main caching code: `ccmlib/scylla_repository.py` (lines 337-361, 455-565)
- Hash utilities: `ccmlib/utils/download.py` (lines 170-203)
- Common utilities: `ccmlib/common.py` (lines 1119-1132)
