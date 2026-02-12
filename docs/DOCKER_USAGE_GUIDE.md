# Using CCM with Docker and Podman

This guide explains how to use CCM (Cassandra Cluster Manager) with Docker or Podman to run Scylla clusters in containers.

## Table of Contents

- [Why Use Docker/Podman](#why-use-dockerpodman)
- [Prerequisites](#prerequisites)
- [Docker Setup](#docker-setup)
- [Podman Setup](#podman-setup)
- [Quick Start](#quick-start)
- [Creating Clusters](#creating-clusters)
- [Managing Clusters](#managing-clusters)
- [Common Use Cases](#common-use-cases)
- [Troubleshooting](#troubleshooting)
- [Advanced Topics](#advanced-topics)

## Why Use Docker/Podman

Using containers with CCM offers several advantages:

- **No local installation**: Don't need to compile or install Scylla binaries
- **Version flexibility**: Easily switch between different Scylla versions
- **Isolation**: Each cluster runs in its own isolated network
- **Reproducibility**: Exact same environment across machines
- **Quick setup**: Start testing in seconds, not minutes

## Prerequisites

You need either Docker or Podman installed on your system. CCM will auto-detect which one is available.

### System Requirements

- **Operating System**: Linux (recommended) or macOS
- **Container Runtime**: Docker 20.10+ or Podman 3.0+
- **Memory**: At least 4GB RAM available
- **Disk Space**: 10GB+ for images and data

## Docker Setup

### Installing Docker

#### Ubuntu/Debian
```bash
# Install Docker
sudo apt-get update
sudo apt-get install docker.io

# Add your user to the docker group (avoid needing sudo)
sudo usermod -aG docker $USER

# Log out and back in, or run:
newgrp docker

# Verify installation
docker ps
```

#### Fedora/RHEL/CentOS
```bash
# Install Docker
sudo dnf install docker

# Start Docker service
sudo systemctl start docker
sudo systemctl enable docker

# Add your user to the docker group
sudo usermod -aG docker $USER

# Log out and back in, then verify
docker ps
```

#### macOS
```bash
# Install Docker Desktop from https://www.docker.com/products/docker-desktop
# Or use Homebrew:
brew install --cask docker

# Start Docker Desktop, then verify
docker ps
```

### Verifying Docker Setup

```bash
# Check Docker is running
docker version

# Test with hello-world
docker run hello-world

# Check you can run without sudo
docker ps
```

## Podman Setup

Podman is a daemonless, rootless alternative to Docker that's often preferred in production environments.

### Installing Podman

#### Ubuntu/Debian
```bash
# Install Podman
sudo apt-get update
sudo apt-get install podman

# Verify installation
podman ps
```

#### Fedora/RHEL/CentOS
```bash
# Podman is usually pre-installed on Fedora
# If not:
sudo dnf install podman

# Verify installation
podman ps
```

### Configuring Podman for CCM

Podman runs rootless by default, which is great for security. However, you may need to configure it:

```bash
# Check current configuration
podman info

# If using Podman, set environment variable (optional)
export CCM_CONTAINER_RUNTIME=podman

# Add to your ~/.bashrc or ~/.zshrc to make it permanent
echo 'export CCM_CONTAINER_RUNTIME=podman' >> ~/.bashrc
```

### Using Podman as Docker Alternative

Podman is command-line compatible with Docker. You can create an alias:

```bash
# Add to ~/.bashrc or ~/.zshrc
alias docker=podman
```

## Quick Start

### Create and start a 3-node cluster

```bash
# Using Docker (auto-detected)
ccm create my-cluster -n 3 --scylla --docker-image scylladb/scylla:latest
ccm start

# Or create and start in one command
ccm create my-cluster -n 3 --scylla --docker-image scylladb/scylla:latest -s

# Check cluster status
ccm status

# Connect with cqlsh
ccm node1 cqlsh
```

### Create a cluster with specific version

```bash
# Use a specific Scylla version
ccm create my-cluster-5.4 -n 3 --scylla --docker-image scylladb/scylla:5.4

# Use nightly builds
ccm create my-cluster-nightly -n 3 --scylla --docker-image scylladb/scylla-nightly:latest

# Use enterprise version
ccm create my-cluster-enterprise -n 3 --scylla --docker-image scylladb/scylla-enterprise:2024.1
```

## Creating Clusters

### Basic Cluster Creation

```bash
# Single-node cluster
ccm create test -n 1 --scylla --docker-image scylladb/scylla:latest

# Three-node cluster
ccm create test -n 3 --scylla --docker-image scylladb/scylla:latest

# Multi-datacenter cluster (2 DCs, 3 nodes each)
ccm create multi-dc -n 3:3 --scylla --docker-image scylladb/scylla:latest
```

### Specifying Container Runtime

```bash
# Explicitly use Docker
ccm create test -n 3 --scylla --docker-image scylladb/scylla:latest --container-runtime docker

# Explicitly use Podman
ccm create test -n 3 --scylla --docker-image scylladb/scylla:latest --container-runtime podman

# Auto-detect (default)
ccm create test -n 3 --scylla --docker-image scylladb/scylla:latest
```

### Advanced Options

```bash
# Start cluster immediately after creation
ccm create test -n 3 --scylla --docker-image scylladb/scylla:latest -s

# Use specific IP prefix
ccm create test -n 3 --scylla --docker-image scylladb/scylla:latest -i 127.0.1.

# Enable vnodes
ccm create test -n 3 --scylla --docker-image scylladb/scylla:latest --vnodes

# Configure cluster ID (for parallel clusters)
ccm create test -n 3 --scylla --docker-image scylladb/scylla:latest --id 5
```

## Managing Clusters

### Starting and Stopping

```bash
# Start entire cluster
ccm start

# Start specific node
ccm node1 start

# Stop entire cluster
ccm stop

# Stop specific node
ccm node1 stop

# Restart cluster
ccm stop && ccm start
```

### Checking Status

```bash
# Show cluster status
ccm status

# Show detailed node information
ccm node1 show

# Check nodetool status
ccm node1 nodetool status
```

### Running Commands

```bash
# Run cqlsh
ccm node1 cqlsh

# Execute CQL directly
ccm node1 cqlsh -e "DESCRIBE KEYSPACES;"

# Run nodetool commands
ccm node1 nodetool status
ccm node1 nodetool info
ccm node1 nodetool describecluster

# Run stress test
ccm node1 stress write n=100000
```

### Adding and Removing Nodes

```bash
# Add a new node
ccm add node4

# Start the new node
ccm node4 start

# Remove a node
ccm node4 stop
ccm remove node4
```

### Clearing and Removing Clusters

```bash
# Clear cluster data (keep cluster)
ccm clear

# Remove cluster entirely
ccm remove

# Switch to different cluster
ccm switch other-cluster
```

## Common Use Cases

### Testing Different Scylla Versions

```bash
# Test migration from 5.2 to 5.4
ccm create scylla-5.2 -n 3 --scylla --docker-image scylladb/scylla:5.2
ccm start
# ... run tests ...
ccm stop

ccm create scylla-5.4 -n 3 --scylla --docker-image scylladb/scylla:5.4
ccm start
# ... run same tests ...
```

### Reproducing Production Issues

```bash
# Use same version as production
ccm create repro -n 3 --scylla --docker-image scylladb/scylla:5.4.3

# Import data schema
ccm node1 cqlsh < production-schema.cql

# Run reproduction steps
# ...
```

### Development and Testing

```bash
# Quick cluster for development
ccm create dev -n 1 --scylla --docker-image scylladb/scylla:latest -s

# Run your application against it
# Your app connects to 127.0.0.1:9042

# Clean up when done
ccm remove dev
```

### Multi-Datacenter Testing

```bash
# Create 2-DC cluster
ccm create multi-dc -n 2:2 --scylla --docker-image scylladb/scylla:latest
ccm start

# Check datacenter status
ccm node1 nodetool status

# Test cross-DC replication
ccm node1 cqlsh -e "CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 2};"
```

## Troubleshooting

### Container Runtime Not Found

**Problem**: `ContainerRuntimeNotFoundError: No container runtime found`

**Solution**:
```bash
# Install Docker or Podman
sudo apt-get install docker.io
# OR
sudo apt-get install podman

# Verify installation
docker ps
# OR
podman ps
```

### Permission Denied

**Problem**: `permission denied while trying to connect to the Docker daemon socket`

**Solution**:
```bash
# Add user to docker group
sudo usermod -aG docker $USER

# Log out and back in, or run
newgrp docker

# Verify
docker ps
```

### Image Not Found

**Problem**: `Failed to pull Docker image 'scylladb/scylla:xxx'`

**Solution**:
```bash
# Pull image manually
docker pull scylladb/scylla:latest

# Or use a specific version that exists
docker pull scylladb/scylla:5.4

# List available tags at https://hub.docker.com/r/scylladb/scylla/tags
```

### Network Issues

**Problem**: Containers can't communicate or nodes can't see each other

**Solution**:
```bash
# Check if cluster network exists
docker network ls | grep ccm

# Remove and recreate cluster
ccm remove
ccm create test -n 3 --scylla --docker-image scylladb/scylla:latest

# Check container IPs
docker inspect <container-id> | grep IPAddress
```

### Port Conflicts

**Problem**: `port is already allocated`

**Solution**:
```bash
# Use different cluster ID to avoid port conflicts
ccm create test --id 5 -n 3 --scylla --docker-image scylladb/scylla:latest

# Or remove existing cluster
ccm remove conflicting-cluster
```

### Logs and Debugging

```bash
# View node logs
ccm node1 showlog

# View Docker container logs
docker logs <container-name>

# Check container status
docker ps -a | grep ccm

# Inspect container
docker inspect <container-name>
```

## Advanced Topics

### Custom Networks

Each cluster automatically gets an isolated Docker network named `ccm-<cluster-name>`. This ensures:

- Cluster isolation (multiple clusters don't interfere)
- Predictable networking
- Easy cleanup

### Volume Mounting

CCM automatically mounts directories for:

- **Configuration**: `~/.ccm/<cluster>/node*/conf` → `/etc/scylla`
- **Data**: `~/.ccm/<cluster>/node*/data` → `/usr/lib/scylla/data`
- **Commitlogs**: `~/.ccm/<cluster>/node*/commitlogs` → `/usr/lib/scylla/commitlogs`
- **Hints**: `~/.ccm/<cluster>/node*/hints` → `/usr/lib/scylla/hints`
- **Logs**: `~/.ccm/<cluster>/node*/logs` → `/var/log/scylla`

You can access these directories on your host system:

```bash
# View data files
ls ~/.ccm/my-cluster/node1/data/

# View logs
tail -f ~/.ccm/my-cluster/node1/logs/system.log

# Edit configuration (requires cluster restart)
vim ~/.ccm/my-cluster/node1/conf/scylla.yaml
ccm node1 stop && ccm node1 start
```

### File Permissions

Docker containers may create files with different ownership. CCM handles this by:

1. Using helper containers to fix permissions when needed
2. Supporting rootless Podman (which matches host UID)
3. Automatic cleanup with proper permissions

### Resource Limits

Currently, CCM doesn't set resource limits on containers. For production-like testing:

```bash
# Set limits manually on containers
docker update --memory 4g --cpus 2 <container-name>

# Or use docker run with limits (requires CCM source modification)
```

### Using Private Registries

```bash
# Login to private registry
docker login myregistry.example.com

# Use image from private registry
ccm create test -n 3 --scylla --docker-image myregistry.example.com/scylla:custom
```

### Environment Variables

Set these to customize CCM's Docker behavior:

- `CCM_CONTAINER_RUNTIME`: Force specific runtime (`docker` or `podman`)
- `SCYLLA_DOCKER_IMAGE`: Default Docker image to use

```bash
# Set default runtime
export CCM_CONTAINER_RUNTIME=podman

# Set default image
export SCYLLA_DOCKER_IMAGE=scylladb/scylla:5.4
```

### Cleaning Up

```bash
# Remove all CCM clusters
ccm list | xargs -I {} ccm switch {} && ccm remove

# Clean up Docker resources
docker system prune -a

# Remove CCM networks
docker network ls | grep ccm | awk '{print $1}' | xargs docker network rm
```

## Best Practices

1. **Use specific versions**: Instead of `:latest`, use specific tags like `:5.4.3` for reproducibility
2. **Clean up regularly**: Remove old clusters and containers to save disk space
3. **Resource awareness**: Don't run too many clusters simultaneously
4. **Use IDs for parallel testing**: Use `--id` flag to avoid conflicts
5. **Check logs**: Use `showlog` and `docker logs` for troubleshooting
6. **Network isolation**: Each cluster gets its own network automatically

## Further Reading

- [CCM Main README](../README.md)
- [Docker Architecture Documentation](DOCKER_ARCHITECTURE.md)
- [Scylla Documentation](https://docs.scylladb.com/)
- [Docker Documentation](https://docs.docker.com/)
- [Podman Documentation](https://podman.io/)
