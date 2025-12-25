# Copilot Instructions for scylla-ccm

## Repository Overview

CCM (Cassandra Cluster Manager) is a Python library and CLI tool to create, launch, and remove Apache Cassandra or ScyllaDB clusters on localhost for testing purposes. This is ScyllaDB's fork with enhanced Scylla support including relocatable packages and Docker-based clusters.

**Project Type:** Python CLI tool and library  
**Language:** Python 3.9+  
**Build System:** UV with setuptools (pyproject.toml)  
**Package Manager:** UV (Astral)

## Quick Setup

```bash
# Install dependencies (creates .venv automatically)
uv sync

# Run the CLI directly
./ccm --help

# Or via uv
uv run ccm --help
```

## Build and Test Commands

### Install Dependencies
Always run before building or testing:
```bash
uv sync
```

### Run Unit Tests (No External Dependencies)
These tests run quickly without needing Scylla/Cassandra binaries:
```bash
uv run python -m pytest ./tests/test_common.py ./tests/test_internal_functions.py ./tests/test_version_parsing.py ./tests/test_utils_version.py -v
```

### Run Full Test Suite
**Note:** Full tests require downloading Scylla binaries (~5-10 minutes first time) and may require Docker:
```bash
uv run python -m pytest ./tests -x
```

### Test Markers
Tests use pytest markers defined in `pyproject.toml`:
- `@pytest.mark.docker` - Tests requiring Docker
- `@pytest.mark.reloc` - Tests using relocatable packages
- `@pytest.mark.cassandra` - Tests using Cassandra binaries
- `@pytest.mark.repo_tests` - Repository version tests

## Project Layout

```
scylla-ccm/
├── ccm                      # CLI entry point script
├── ccmlib/                  # Main library
│   ├── bin/                 # CLI main() entry point
│   ├── cmds/                # CLI command implementations
│   │   ├── cluster_cmds.py  # Cluster-level commands (create, start, stop, etc.)
│   │   └── node_cmds.py     # Node-level commands (cqlsh, nodetool, etc.)
│   ├── utils/               # Utility modules (version parsing, SSL, etc.)
│   ├── cluster.py           # Base Cluster class
│   ├── scylla_cluster.py    # ScyllaCluster implementation
│   ├── scylla_docker_cluster.py  # Docker-based Scylla cluster
│   ├── node.py              # Base Node class
│   ├── scylla_node.py       # ScyllaNode implementation
│   ├── common.py            # Shared utilities
│   └── repository.py        # Cassandra binary download
├── tests/                   # Test suite
│   ├── conftest.py          # Pytest fixtures
│   ├── test_config.py       # Test configuration
│   └── test_*.py            # Test modules
├── pyproject.toml           # Build configuration and dependencies
├── .github/workflows/       # CI workflows
└── docs/                    # Documentation
```

## Configuration Files

| File | Purpose |
|------|---------|
| `pyproject.toml` | Build config, dependencies, pytest settings |
| `.python-version` | Python version (3.12) |
| `flake.nix` | Nix flake for reproducible builds |
| `.gitignore` | Git ignore patterns |

## CI Workflows

The CI runs on push/PR to `master` and `next*` branches:

1. **integration-tests.yml** - Main test workflow
   - Python versions: 3.11, 3.12, 3.13, 3.14
   - Runs: `uv sync` → downloads Scylla binaries → `uv run python -m pytest ./tests -x`

2. **nix.yml** - Nix-based tests (Python 3.9 and 3.11)

3. **trigger_jenkins.yaml** - Triggers Jenkins for `next*` branches

## Key Environment Variables

| Variable | Purpose |
|----------|---------|
| `SCYLLA_VERSION` | Scylla version to test (e.g., `unstable/master:2025-01-19T09:39:05Z`) |
| `SCYLLA_DOCKER_IMAGE` | Docker image for tests |
| `CCM_CONFIG_DIR` | Custom CCM config directory (default: `~/.ccm`) |
| `SCYLLA_MANAGER_PACKAGE` | Path to Scylla Manager package |

## Code Style Guidelines

- No explicit linter configuration exists
- Follow existing code patterns
- Use type hints where present in existing code
- Keep imports organized (standard library, third-party, local)

### Test Style Guidelines

When writing tests, follow pytest best practices:
- Use **function-based tests** (not class-based) with fixtures
- Create **reusable fixtures** to minimize code duplication between tests
- Use **pytest.fixture** decorator for setup/teardown and shared test data
- Use **parametrize** when testing multiple similar cases
- Prefer pytest fixtures over manual setup/teardown in try/finally blocks

Example:
```python
@pytest.fixture
def mock_cluster():
    """Create a minimal cluster instance with mocked methods."""
    cluster = object.__new__(Cluster)
    cluster.name = 'test_cluster'
    # ... setup code ...
    return cluster

def test_add_node(mock_cluster):
    """Test adding a node to the cluster."""
    # Test code using the fixture
    pass
```

## Common Issues and Workarounds

1. **Test failures on first run**: Tests download Scylla binaries to `~/.ccm/scylla-repository/`. This can take several minutes.

2. **Docker tests skipped**: Docker-based cluster tests are currently skipped with `@pytest.mark.skip(reason="ccm docker support broke in master, need to fix")`.

3. **Java requirement**: Cassandra tests and older Scylla (<6.0) require Java 8.

## Adding New Dependencies

Dependencies are managed in `pyproject.toml`:
- Runtime deps: under `[project.dependencies]`
- Dev deps: under `[dependency-groups.dev]`

After modifying, run `uv sync` to update the lock file.

## Testing Changes

1. Run unit tests first (fast, no external deps):
   ```bash
   uv run python -m pytest ./tests/test_common.py ./tests/test_utils_version.py -v
   ```

2. If modifying cluster/node code, run integration tests:
   ```bash
   uv run python -m pytest ./tests -x -k "not docker"
   ```

## Important Notes

- The `./ccm` script in the repo root is the direct entry point
- Data is stored in `~/.ccm/` by default
- Tests create clusters in `tests/test_results/` (gitignored)
- Windows is not supported in this fork

Trust these instructions. Only search the codebase if information here is incomplete or incorrect.
