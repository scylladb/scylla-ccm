CCM (Cassandra Cluster Manager) - Scylla Fork
====================================================

CCM is a script/library to create, launch and remove an Apache Cassandra or ScyllaDB cluster on
localhost. This is ScyllaDB's fork with enhanced support for Scylla, including:

- **Relocatable packages** - Download and use pre-built Scylla packages from S3
- **Docker support** - Run Scylla clusters using Docker images
- **Unified packages** - Simplified installation with all-in-one packages

The goal of ccm is to make it easy to create, manage and destroy a
small Scylla cluster on a local box for testing purposes.

Quick Start
-----------

### Creating a 3-node Scylla cluster (using relocatable packages):
```bash
# Create cluster with a released version
$ ccm create my_cluster --scylla -n 3 -v release:2024.2 -s

# Check cluster status
$ ccm status
Cluster: 'my_cluster'
-----------------
node1: UP
node2: UP
node3: UP
```

The nodes will be available at 127.0.0.1, 127.0.0.2 and 127.0.0.3.

### Creating a cluster with an unstable/nightly build:
```bash
# Using a specific build timestamp (from S3)
$ ccm create nightly_cluster --scylla -n 3 -v unstable/master:2024-12-20T10:30:00Z -s

# Or use the latest nightly build number
$ ccm create nightly_cluster --scylla -n 3 -v unstable/master:380 -s
```

### Creating a multi-datacenter cluster:
```bash
# 3 nodes in dc1, 4 nodes in dc2, 5 nodes in dc3
$ ccm create my_multi_dc_cluster --scylla -n 3:4:5 -v release:2024.2 -s
```

Using Relocatable Packages
---------------------------

Scylla CCM primarily uses **relocatable packages** downloaded from S3. These are pre-built packages that work across different Linux distributions.

### Version Format

CCM supports two main version formats:

1. **Release versions**: `release:X.Y.Z`
   ```bash
   ccm create my_cluster --scylla -n 3 -v release:2024.2.3
   ccm create my_cluster --scylla -n 3 -v release:6.0
   ```

2. **Unstable/nightly versions**: `unstable/<branch>:<timestamp>` or `unstable/<branch>:<build-number>`
   ```bash
   ccm create my_cluster --scylla -n 3 -v unstable/master:2024-12-20T10:30:00Z
   ccm create my_cluster --scylla -n 3 -v unstable/master:380
   ccm create my_cluster --scylla -n 3 -v unstable/branch-5.4:2024-12-01T10:00:00Z
   ```

3. **Debug builds**: Append `:debug` to any version
   ```bash
   ccm create my_cluster --scylla -n 3 -v release:2024.2:debug
   ccm create my_cluster --scylla -n 3 -v unstable/master:latest:debug
   ```

### Using Custom Relocatable Packages

You can override packages with your own locally-built versions:

```bash
# Using a unified package (all-in-one)
ccm create my_cluster -n 3 --scylla \
    --version my_custom_scylla \
    --scylla-unified-package-uri=/path/to/scylla-unified-package.tar.gz

# Mix and match: use S3 version but override just the core package
ccm create my_cluster -n 3 --scylla \
    --version unstable/master:380 \
    --scylla-core-package-uri=../scylla/build/release/scylla-package.tar.gz

# Override individual components
ccm create my_cluster -n 3 --scylla --version temp \
    --scylla-core-package-uri=../scylla/build/release/scylla-package.tar.gz \
    --scylla-tools-java-package-uri=../scylla-tools-java/temp.tar.gz \
    --scylla-jmx-package-uri=../scylla-jmx/temp.tar.gz
```

Using Docker
------------

CCM supports running Scylla clusters in Docker containers:

```bash
# Create cluster using official Scylla Docker image
ccm create my_docker_cluster --scylla --docker-image scylladb/scylla:6.0 -n 3
ccm start
ccm status
```

For more details, see [docs/docker-ccm.md](docs/docker-ccm.md).

Using Local Install Directory
------------------------------

If you have a locally compiled Scylla installation, you can use it instead of relocatable packages:

```bash
# Create cluster from local Scylla installation
ccm create my_local_cluster --scylla -n 3 --install-dir=/path/to/scylla

# Or from the Scylla source directory
cd /path/to/scylla
ccm create my_local_cluster --scylla -n 3
```

Requirements
------------

- **Python 3.9+** (tested with Python 3.14)
- **[UV](https://docs.astral.sh/uv)** - for development and building from source
- **Java 8+** - only required for:
  - Using Cassandra clusters
  - Using older Scylla versions (< 6.0)
- **Docker** (optional) - for Docker-based clusters
- **Multiple loopback interfaces** - CCM runs nodes on 127.0.0.X addresses
  - On Linux: Usually available by default
  - On Mac OS X: Create aliases manually:
    ```bash
    sudo ifconfig lo0 alias 127.0.0.2 up
    sudo ifconfig lo0 alias 127.0.0.3 up
    # ... add more as needed
    ```

Known Issues
------------

- This fork of CCM doesn't support Windows
- Docker support is currently experimental

Installation
------------

### From Source (Development)

CCM uses UV with setuptools as a build system:

```bash
# Install UV if you don't have it
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone and install
git clone https://github.com/scylladb/scylla-ccm.git
cd scylla-ccm
uv sync

# Run CCM directly
./ccm --help

# Or via UV
uv run ccm --help
```

### Using Nix

You can use CCM through Nix without installing:

```bash
# Spawn temporary shell with ccm
nix shell github:scylladb/scylla-ccm

# Install ccm to your profile
nix profile install github:scylladb/scylla-ccm

# Update or remove (find index first)
nix profile list
nix profile upgrade <index>
nix profile remove <index>
```


Nix Development Environment
---------------------------

This project features experimental Nix flake support for reproducible development environments.

### Setting up Nix

1. Install Nix: https://nixos.org/download.html
   - On Fedora, use "Single-user installation" due to SELinux compatibility
2. Enable experimental features:
   ```bash
   mkdir -p ~/.config/nix
   echo "experimental-features = nix-command flakes" >> ~/.config/nix/nix.conf
   ```
   - For multi-user installations, restart the Nix daemon after this step

### Using Nix Dev Environment

**Option 1: Using direnv (recommended)**
```bash
# Install direnv (see: https://direnv.net/docs/installation.html)
cd scylla-ccm
direnv allow .
# Environment loads automatically in project directory
```

**Option 2: Manual activation**
```bash
nix develop                     # Launches bash with dev environment
nix develop --command zsh       # Use your preferred shell
```

Common CCM Commands
-------------------

### Cluster Management
```bash
ccm create <name> [options]    # Create a new cluster
ccm list                        # List all clusters
ccm switch <name>               # Switch to a different cluster
ccm status                      # Show cluster status
ccm start                       # Start all nodes
ccm stop                        # Stop all nodes
ccm remove                      # Remove current cluster
ccm clear                       # Clear cluster data but keep config
```

### Node Management
```bash
ccm node1 start                 # Start a specific node
ccm node1 stop                  # Stop a specific node
ccm node1 cqlsh                 # Connect to node with cqlsh
ccm node1 nodetool status       # Run nodetool on a node
ccm node1 showlog               # View node logs
```

### Advanced Operations
```bash
ccm flush                       # Flush all nodes
ccm compact                     # Compact all nodes
ccm node1 ring                  # Show ring information
ccm add node4 -i 127.0.0.4      # Add a new node to cluster
```

For complete command reference, run `ccm --help` or `ccm <command> --help`.

Working with Cassandra
----------------------

While this fork is optimized for Scylla, CCM still supports Cassandra clusters.

### Using Cassandra with Version Download
```bash
ccm create cassandra_test -v 4.0.0 -n 3 -s
```

### Using Local Cassandra Installation
```bash
ccm create cassandra_test --install-dir=/path/to/cassandra -n 3 -s
```

### From Cassandra Source
```bash
# From Cassandra source directory after compilation
cd /path/to/cassandra
ccm create cassandra_test -n 3 -s
```

### Git and GitHub Sources
```bash
# From Apache Git repository
ccm create trunk -v git:trunk -n 3

# From GitHub fork
ccm create patched -v github:jbellis/trunk -n 1
```

### Source vs Binary Distribution
```bash
# Force source distribution instead of binary
ccm create test -v source:4.0.0 -n 3 -s

# Note: If 'binary:' or 'source:' aren't specified, CCM will
# fallback to building from git if Apache mirrors are unavailable
```

Remote Debugging
----------------

For Cassandra and older Scylla versions that use JMX, you can enable remote debugging:

```bash
# With populate command
ccm populate -d -n 3

# Or when adding individual nodes
ccm add node4 -r 5005 -i 127.0.0.4 -j 7400 -b
```

This sets up remote debugging on ports 2100, 2200, 2300 for nodes 1, 2, 3 respectively.
The main thread will not be suspended, so nodes start without requiring a debugger connection.

Where Things Are Stored
------------------------

By default, CCM stores all node data and configuration files under `~/.ccm/cluster_name/`.

For Scylla relocatable packages, downloaded packages are cached in `~/.ccm/scylla-repository/`.

You can override the config directory using the `--config-dir` option with each command.

Using CCMLib Programmatically
------------------------------

The CCM facilities are available programmatically through ccmlib for automated testing:

### Scylla Example
```python
from ccmlib.scylla_cluster import ScyllaCluster

# Create a 3-node Scylla cluster
cluster = ScyllaCluster('.', 'test_cluster', version='release:2024.2')
cluster.populate(3).start()
node1, node2, node3 = cluster.nodelist()

# Run operations
cluster.flush()
node2.compact()

# Connect to nodes via CQL (host and port available at node.network_interfaces['binary'])
# ...

# Cleanup
cluster.stop()
cluster.remove()
```

### Cassandra Example
```python
import ccmlib

# Create a 3-node Cassandra cluster
cluster = ccmlib.Cluster('.', 'test', cassandra_version='4.0.0')
cluster.populate(3).start()
node1, node2, node3 = cluster.nodelist()

# Run operations
cluster.flush()
node2.compact()

# Cleanup
cluster.remove()
```

--
Original Author: Sylvain Lebresne <sylvain@datastax.com>
Scylla Fork Maintained by: ScyllaDB Team
