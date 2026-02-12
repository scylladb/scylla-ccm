# Docker Support Architecture for CCM

## Overview

This document describes the improved Docker support architecture for CCM (Cassandra Cluster Manager) to enable running Scylla clusters using Docker containers with full CCM functionality.

## Design Goals

1. **Docker-First Approach**: Docker image should be the primary specification, not version
2. **Container Runtime Agnostic**: Support both Docker and Podman
3. **Directory Transparency**: Data and configuration directories should be accessible to CCM code as if running locally
4. **Full CCM Compatibility**: All CCM commands should work seamlessly with Docker clusters
5. **Comprehensive Testing**: Full unit and integration test coverage for Docker mode

## Architecture Components

### 1. Container Client Abstraction Layer

**Purpose**: Provide a unified interface for Docker and Podman operations

**Location**: `ccmlib/container_client.py` (new file)

**Key Classes**:
- `ContainerClient` (abstract base class)
  - `DockerClient` (Docker implementation)
  - `PodmanClient` (Podman implementation)

**Responsibilities**:
- Container lifecycle management (create, start, stop, remove)
- Image management (pull, inspect)
- Network management
- Volume management
- Command execution inside containers
- Log streaming

**Interface**:
```python
class ContainerClient(ABC):
    @abstractmethod
    def run_container(self, image, name, volumes, ports, env, network, command)
    
    @abstractmethod
    def exec_command(self, container_id, command)
    
    @abstractmethod
    def stop_container(self, container_id, timeout)
    
    @abstractmethod
    def remove_container(self, container_id, force, volumes)
    
    @abstractmethod
    def inspect_container(self, container_id)
    
    @abstractmethod
    def get_container_ip(self, container_id)
    
    @abstractmethod
    def stream_logs(self, container_id, follow)
```

### 2. Enhanced ScyllaDockerCluster

**Changes**:
- Use `ContainerClient` abstraction instead of direct shell commands
- Store container runtime preference in cluster.conf
- Validate docker_image availability before cluster creation
- Better error handling and logging
- Support custom networks for cluster isolation

**New Methods**:
- `validate_docker_image()`: Check if image exists or can be pulled
- `get_container_client()`: Return configured container client
- `setup_cluster_network()`: Create isolated network for cluster

### 3. Enhanced ScyllaDockerNode

**Changes**:
- Use `ContainerClient` for all container operations
- Improve volume mounting strategy
- Better lifecycle management
- Enhanced error handling
- Support for environment variable injection

**Volume Mounting Strategy**:
```
Host Path                           Container Path
---------                           --------------
<node_path>/conf                -> /etc/scylla
<node_path>/data                -> /usr/lib/scylla/data
<node_path>/commitlogs          -> /usr/lib/scylla/commitlogs
<node_path>/hints               -> /usr/lib/scylla/hints
<node_path>/view_hints          -> /usr/lib/scylla/view_hints
<node_path>/saved_caches        -> /usr/lib/scylla/saved_caches
<node_path>/keys                -> /usr/lib/scylla/keys
<node_path>/logs                -> /var/log/scylla
```

### 4. CLI Enhancements

**New Options**:
```bash
ccm create --docker-image <image> [--container-runtime docker|podman] [--pull-image]
ccm add --docker-image <image>  # For adding nodes
```

**Environment Variables**:
- `CCM_CONTAINER_RUNTIME`: Default container runtime (docker or podman)
- `SCYLLA_DOCKER_IMAGE`: Default Docker image to use

## Container Runtime Support

### Docker Setup
Docker must be installed and the current user must have permissions to run Docker commands.

```bash
# Verify Docker setup
docker ps

# If permission denied, add user to docker group
sudo usermod -aG docker $USER
```

### Podman Setup
Podman can be used as a drop-in replacement for Docker.

```bash
# Install Podman (Ubuntu/Debian)
sudo apt-get install podman

# Verify Podman setup
podman ps

# Optional: Create Docker alias for Podman
alias docker=podman

# Or set environment variable
export CCM_CONTAINER_RUNTIME=podman
```

**Key Differences**:
- Podman runs rootless by default
- Different network handling (may need additional configuration)
- Slightly different CLI output formats

## Directory and Configuration Management

### Directory Structure
```
~/.ccm/
  cluster_name/
    cluster.conf          # Includes docker_image and container_runtime
    node1/
      node.conf           # Includes docker_id and docker_name
      conf/               # Mounted to container /etc/scylla
        scylla.yaml
        cassandra-rackdc.properties
      data/               # Mounted to container /usr/lib/scylla/data
      commitlogs/         # Mounted to container /usr/lib/scylla/commitlogs
      hints/              # Mounted to container /usr/lib/scylla/hints
      view_hints/         # Mounted to container /usr/lib/scylla/view_hints
      saved_caches/       # Mounted to container /usr/lib/scylla/saved_caches
      keys/               # Mounted to container /usr/lib/scylla/keys (SSL)
      logs/               # Mounted to container /var/log/scylla
```

### Configuration Flow
1. Extract default config from Docker image (one-time per cluster)
2. Merge CCM configuration options
3. Write to local conf directory
4. Mount conf directory into container
5. Container reads configuration from mounted volume

### Permission Handling
Docker containers may create files with different ownership. We handle this by:
1. Using a helper container to fix permissions when needed
2. Supporting rootless Podman (which matches host UID)
3. Proper cleanup using container-based chmod/rm operations

## Testing Strategy

### Unit Tests

**Test File**: `tests/test_container_client.py` (new)
- Test Docker client operations
- Test Podman client operations
- Test client factory and selection
- Mock container runtime for fast tests

**Test File**: `tests/test_docker_cluster_unit.py` (new)
- Test volume mounting logic
- Test configuration generation
- Test network setup
- Test error handling

### Integration Tests

**Test File**: `tests/test_scylla_docker_cluster.py` (existing, enhanced)
- Test full cluster lifecycle (create, start, stop, remove)
- Test node operations (add, remove, restart)
- Test CQL operations
- Test nodetool commands
- Test configuration updates
- Test log access
- Test stress workloads

**Test File**: `tests/test_docker_podman_compat.py` (new)
- Run same tests with Docker and Podman
- Verify feature parity
- Test switching between runtimes

**Test Markers**:
- `@pytest.mark.docker` - Tests requiring Docker
- `@pytest.mark.podman` - Tests requiring Podman
- `@pytest.mark.container` - Tests requiring any container runtime

### CI Integration

**GitHub Actions Workflow**:
```yaml
name: Docker Tests
on: [push, pull_request]
jobs:
  test-docker:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Run Docker tests
        run: pytest -v -m docker tests/
        
  test-podman:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Install Podman
        run: sudo apt-get install -y podman
      - name: Run Podman tests
        run: CCM_CONTAINER_RUNTIME=podman pytest -v -m podman tests/
```

## Error Handling and Logging

### Container Client Errors
- `ContainerRuntimeNotFoundError`: Container runtime not installed
- `ContainerImageNotFoundError`: Docker image not available
- `ContainerStartError`: Failed to start container
- `ContainerExecError`: Failed to execute command in container

### Logging
- Use `ccmlib.common.LOGGER` for all Docker operations
- Log container commands before execution
- Log container output at debug level
- Log errors with full context (container ID, command, output)

## Migration from Current Implementation

### Breaking Changes
None - existing docker_image parameter remains compatible

### New Features
- Container runtime selection
- Better error messages
- Improved logging
- Network isolation
- Full test coverage

### Deprecations
None

## Performance Considerations

1. **Image Caching**: Docker images are cached locally after first pull
2. **Container Reuse**: Containers are kept running and reused across tests
3. **Network Overhead**: Docker network adds minimal overhead
4. **Volume Performance**: Mounted volumes have near-native performance

## Security Considerations

1. **Rootless Operation**: Prefer Podman for rootless containers
2. **Network Isolation**: Each cluster gets its own network
3. **Volume Permissions**: Proper permission handling to avoid security issues
4. **Image Verification**: Validate images before use

## Future Enhancements

1. **Kubernetes Support**: Extend abstraction to support K8s pods
2. **Docker Compose**: Support for docker-compose.yml definitions
3. **Multi-host Clusters**: Distribute nodes across multiple Docker hosts
4. **Resource Limits**: CPU and memory limits for containers
5. **Health Checks**: Built-in container health monitoring
