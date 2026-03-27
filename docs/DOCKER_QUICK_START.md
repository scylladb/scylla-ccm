# CCM Docker Quick Start Guide

Get up and running with Scylla using CCM and Docker in under 5 minutes!

## Prerequisites

- Docker or Podman installed and running
- CCM installed (`pip install scylla-ccm` or clone this repo)

## 30-Second Quick Start

```bash
# Create and start a 3-node cluster
ccm create test -n 3 --scylla --docker-image scylladb/scylla:latest -s

# Verify cluster is running
ccm status

# Connect with cqlsh
ccm node1 cqlsh
```

That's it! You now have a running 3-node Scylla cluster.

## 5-Minute Tutorial

### Step 1: Create a Cluster (10 seconds)

```bash
# Create a 3-node cluster with Scylla latest
ccm create my-cluster -n 3 --scylla --docker-image scylladb/scylla:latest

# Start the cluster
ccm start
```

### Step 2: Verify Cluster is Running (5 seconds)

```bash
# Check cluster status
ccm status

# Output should show:
# Cluster: 'my-cluster'
# -----------------
# node1: UP
# node2: UP
# node3: UP
```

### Step 3: Run Some CQL Commands (30 seconds)

```bash
# Connect to node1 with cqlsh
ccm node1 cqlsh

# In cqlsh, create a keyspace and table:
CREATE KEYSPACE demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
USE demo;
CREATE TABLE users (id int PRIMARY KEY, name text);
INSERT INTO users (id, name) VALUES (1, 'Alice');
INSERT INTO users (id, name) VALUES (2, 'Bob');
SELECT * FROM users;

# Exit cqlsh
exit
```

### Step 4: Check Node Status (15 seconds)

```bash
# Run nodetool status
ccm node1 nodetool status

# You should see all 3 nodes in UN (Up/Normal) state
```

### Step 5: Cleanup (5 seconds)

```bash
# Stop the cluster
ccm stop

# Remove the cluster (optional)
ccm remove
```

## Common Use Cases

### Testing a Specific Scylla Version

```bash
# Test Scylla 5.4
ccm create scylla-5.4 -n 3 --scylla --docker-image scylladb/scylla:5.4 -s

# Test nightly builds
ccm create scylla-nightly -n 3 --scylla --docker-image scylladb/scylla-nightly:latest -s
```

### Single-Node Development Cluster

```bash
# Create a minimal cluster for development
ccm create dev -n 1 --scylla --docker-image scylladb/scylla:latest -s

# Your app can connect to 127.0.0.1:9042
```

### Multi-Datacenter Setup

```bash
# 3 nodes in DC1, 3 nodes in DC2
ccm create multi-dc -n 3:3 --scylla --docker-image scylladb/scylla:latest -s

# Verify datacenters
ccm node1 nodetool status
```

### Using Podman Instead of Docker

```bash
# Explicitly use Podman
ccm create test -n 3 --scylla \
  --docker-image scylladb/scylla:latest \
  --container-runtime podman \
  -s
```

## Useful Commands

### Cluster Management

```bash
# List all clusters
ccm list

# Switch to a cluster
ccm switch my-cluster

# Show cluster status
ccm status

# Stop cluster
ccm stop

# Start cluster
ccm start

# Clear cluster data (keeps cluster config)
ccm clear

# Remove cluster completely
ccm remove
```

### Node Operations

```bash
# Start specific node
ccm node2 start

# Stop specific node
ccm node2 stop

# Show node details
ccm node1 show

# View node logs
ccm node1 showlog
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

# Run cassandra-stress
ccm node1 stress write n=100000
```

## Troubleshooting

### Docker Not Running

**Problem**: `ContainerRuntimeNotFoundError`

**Solution**:
```bash
# Check Docker is running
docker ps

# If not, start Docker service
sudo systemctl start docker
```

### Permission Denied

**Problem**: `permission denied while trying to connect to the Docker daemon`

**Solution**:
```bash
# Add yourself to docker group
sudo usermod -aG docker $USER

# Log out and log back in
# Or run: newgrp docker
```

### Image Not Found

**Problem**: `Failed to pull Docker image`

**Solution**:
```bash
# Pull the image manually first
docker pull scylladb/scylla:latest

# Then retry cluster creation
```

### Port Already in Use

**Problem**: Port conflicts with existing cluster

**Solution**:
```bash
# Use a different cluster ID
ccm create test --id 5 -n 3 --scylla --docker-image scylladb/scylla:latest

# Or remove the conflicting cluster
ccm remove old-cluster
```

## Next Steps

- Read the full [Docker Usage Guide](DOCKER_USAGE_GUIDE.md)
- Check the [Architecture Documentation](DOCKER_ARCHITECTURE.md)
- Explore the [Main README](../README.md)
- Visit [Scylla Documentation](https://docs.scylladb.com/)

## Tips

1. **Use specific versions** - Instead of `:latest`, use `:5.4` for reproducibility
2. **Clean up regularly** - Remove old clusters to save disk space
3. **Check logs** - Use `ccm node1 showlog` for debugging
4. **Multiple clusters** - Use `--id` flag to run clusters in parallel
5. **Data location** - Cluster data is in `~/.ccm/<cluster-name>/`

## Examples for Different Scenarios

### CI/CD Testing

```bash
# Quick cluster for CI
ccm create ci-test -n 1 --scylla --docker-image scylladb/scylla:5.4 -s
# Run your tests
ccm remove ci-test
```

### Load Testing

```bash
# 3-node cluster for load testing
ccm create load-test -n 3 --scylla --docker-image scylladb/scylla:latest -s

# Run stress test
ccm node1 stress write n=1000000 -rate threads=50
```

### Version Migration Testing

```bash
# Start with old version
ccm create old-version -n 3 --scylla --docker-image scylladb/scylla:5.2 -s
# ... test application ...
ccm remove old-version

# Test with new version
ccm create new-version -n 3 --scylla --docker-image scylladb/scylla:5.4 -s
# ... test same application ...
```

### Schema Development

```bash
# Quick cluster for schema work
ccm create schema-dev -n 1 --scylla --docker-image scylladb/scylla:latest -s

# Edit your schema
ccm node1 cqlsh < my-schema.cql

# Test queries
ccm node1 cqlsh -e "SELECT * FROM my_keyspace.my_table;"
```

---

**Need help?** Check the full [Docker Usage Guide](DOCKER_USAGE_GUIDE.md) or file an issue on GitHub!
