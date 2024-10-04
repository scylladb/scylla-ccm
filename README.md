CCM (Cassandra Cluster Manager)
====================================================

A script/library to create, launch and remove an Apache Cassandra cluster on
localhost.

The goal of ccm and ccmlib is to make it easy to create, manage and destroy a
small Cassandra cluster on a local box. It is meant for testing a Cassandra cluster.


Pointer to the Scylla CCM instructions (should really be merged here)
---------------------------------------------------------------------
https://github.com/scylladb/scylla/wiki/Using-CCM


Scylla usage examples:
---------------------------------------------------------------------
### Creating a 3-node Scylla cluster:
```bash
$ ccm create my_cluster --scylla --vnodes -n 3 -v release:2022.2
$ ccm start
# Now wait...
$ ccm status
Cluster: 'my_cluster'
-----------------
node1: UP
node2: UP
node3: UP
```
The nodes will be available at 127.0.0.1, 127.0.0.2 and 127.0.0.3.

### Creating a multi-datacenter cluster that has 3 nodes in dc1, 4 nodes in dc2 and 5 nodes in dc3:
```bash
$ ccm create my_multi_dc_cluster --scylla --vnodes -n 3:4:5 -v release:2022.2
$ ccm start
# Wait a lot...
```

### Creating a cluster of nodes with a specific build-id:

Let's say you want to create a cluster of Scylla with build id `f6e718548e76ccf3564ed2387b6582ba8d37793c` (it's `Scylla 2023.1.0~rc8-20230731.b6f7c5a6910c`).

1. Go to https://backtrace.scylladb.com and find your desired Scylla version
2. Click the arrow down symbol (\\/) to show all available download links
3. Download the unified Scylla package (`unified-pack-url-x86_64`)
4. Create a 3 node cluster:
```bash
# The unified package will be extracted to ~/.ccm/scylla-repository/my_custom_scylla
# Make sure that the version name (my_custom_scylla) is different for each unified package you use, otherwise ccm will use the previously extracted version.
ccm create my_cluster -n 3 --scylla --vnodes \
    --version my_custom_scylla \
    --scylla-unified-package-uri=/home/$USER/Downloads/scylla-enterprise-unified-2023.1.0\~rc8-0.20230731.b6f7c5a6910c.x86_64.tar.gz
```

Requirements
------------

- A working python installation (tested to work with python 3.12).
- [Poetry](https://python-poetry.org/docs/)
- `poetry install` to install the required dependencies.
  - This will create virtualenv, you can use `poetry shell` to enter it, see [poetry](https://python-poetry.org/docs/basic-usage/#using-your-virtual-environment) documentation for more details
- Java if cassandra is used or older scylla < 6.0 (which version depends on the version 
  of Cassandra you plan to use. If unsure, use Java 8 as it is known to 
  work with current versions of Cassandra).
- ccm only works on localhost for now. If you want to create multiple
  node clusters, the simplest way is to use multiple loopback aliases. On
  modern linux distributions you probably don't need to do anything, but
  on Mac OS X, you will need to create the aliases with

      sudo ifconfig lo0 alias 127.0.0.2 up
      sudo ifconfig lo0 alias 127.0.0.3 up
      ...

  Note that the usage section assumes that at least 127.0.0.1, 127.0.0.2 and
  127.0.0.3 are available.

Known issues
------------

- this fork of ccm doesn't support Windows

Installation
------------

ccm uses python setuptools (with distutils fallback) so from the source directory run:

    poetry install
    poetry shell

ccm is available on the [Python Package Index][pip]:

    pip install ccm

There is also a [Homebrew package][brew] available:

    brew install ccm

You can also use ccm trough Nix.

    Spawn new temporary shell with ccm present, without installing: `nix shell github:scylladb/scylla-ccm`
    Install ccm: `nix profile install github:scylladb/scylla-ccm`
    To remove / update ccm installed this way, first locate it's index in `nix profile list`.
    To remove it, use `nix profile remove <index>`.
    To update it use `nix profile upgrade <index>` - or `nix profile upgrade '.*'` to upgrade all Nix packages.

  [pip]: https://pypi.python.org/pypi/ccm
  [brew]: https://github.com/Homebrew/homebrew/blob/master/Library/Formula/ccm.rb

Nix
-----------------------

This project features experimental Nix flake.
It allows ccm to be used as a dependency in other nix projects or to quickly launch a dev shell
with all dependencies required to run and test the project.

### How to setup Nix shell

1. Install Nix: https://nixos.org/download.html - on Fedora you should probably use "Single-user installation",
   as there are some problems with multi-user due to SELinux.
2. Activate required experimental features:
```
mkdir -p ~/.config/nix
echo "experimental-features = nix-command flakes" >> ~/.config/nix/nix.conf
```
   If you installed Nix in multi-user mode, you will need to restart Nix daemon.
3. First option: using direnv. Install direnv (see: https://direnv.net/docs/installation.html ), `cd` into project directory and execute `direnv allow .`.
   Now you will have dev env activated whenever you are in a project's directory - and automatically unloaded when you leave it.
4. Second option: use `nix develop` command directly. This command will launch a bash session with loaded dev env. If you want to use your favourite shell,
   pass `--command <shell>` flag to `nix develop` (in my case: `nix develop --command zsh`).

If you want to install ccm using Nix, or launch a temporary shell with ccm - see "Installation" section.

Usage
-----

Let's say you wanted to fire up a 3 node Cassandra cluster.

### Short version

    ccm create test -v 2.0.5 -n 3 -s

You will of course want to replace `2.0.5` by whichever version of Cassandra
you want to test.

### Longer version

ccm works from a Cassandra source tree (not the jars). There are two ways to
tell ccm how to find the sources:
  1. If you have downloaded *and* compiled Cassandra sources, you can ask ccm
     to use those by initiating a new cluster with:

        ccm create test --install-dir=<path/to/cassandra-sources>

     or, from that source tree directory, simply

          ccm create test

  2. You can ask ccm to use a released version of Cassandra. For instance to
     use Cassandra 2.0.5, run

          ccm create test -v 2.0.5

     ccm will download the binary (from http://archive.apache.org/dist/cassandra),
     and set the new cluster to use it. This means
     that this command can take a few minutes the first time you
     create a cluster for a given version. ccm saves the compiled
     source in `~/.ccm/repository/`, so creating a cluster for that
     version will be much faster the second time you run it
     (note however that if you create a lot of clusters with
     different versions, this will take up disk space).

Once the cluster is created, you can populate it with 3 nodes with:

    ccm populate -n 3

Note: If you’re running on Mac OSX, create a new interface for every node besides the first, for example if you populated your cluster with 3 nodes, create interfaces for 127.0.0.2 and 127.0.0.3 like so:

    sudo ifconfig lo0 alias 127.0.0.2
    sudo ifconfig lo0 alias 127.0.0.3

Otherwise you will get the following error message:

    (...) Inet address 127.0.0.1:9042 is not available: [Errno 48] Address already in use

After that execute:

    ccm start

That will start 3 nodes on IP 127.0.0.[1, 2, 3] on port 9042 for native transport, port
7000 for the internal cluster communication and ports 7100, 7200 and 7300 for JMX.
You can check that the cluster is correctly set up with

    ccm node1 ring

You can then bootstrap a 4th node with

    ccm add node4 -i 127.0.0.4 -j 7400 -b

(populate is just a shortcut for adding multiple nodes initially)

ccm provides a number of conveniences, like flushing all of the nodes of
the cluster:

    ccm flush

or only one node:

    ccm node2 flush

You can also easily look at the log file of a given node with:

    ccm node1 showlog

Finally, you can get rid of the whole cluster (which will stop the node and
remove all the data) with

    ccm remove

The list of other provided commands is available through

    ccm

Each command is then documented through the `-h` (or `--help`) flag. For
instance `ccm add -h` describes the options for `ccm add`.

### Source Distribution

If you'd like to use a source distribution instead of the default binary each time (for example, for Continuous Integration), you can prefix cassandra version with `source:`, for example:

```
ccm create test -v source:2.0.5 -n 3 -s
```

### Automatic Version Fallback

If 'binary:' or 'source:' are not explicitly specified in your version string, then ccm will fallback to building the requested version from git if it cannot access the apache mirrors.

### Git and GitHub

To use the latest version from the [canonical Apache Git repository](https://git-wip-us.apache.org/repos/asf?p=cassandra.git), use the version name `git:branch-name`, e.g.:

```
ccm create trunk -v git:trunk -n 5
```

and to download a branch from a GitHub fork of Cassandra, you can prefix the repository and branch with `github:`, e.g.:

```
ccm create patched -v github:jbellis/trunk -n 1
```

Remote debugging
-----------------------

If you would like to connect to your Cassandra nodes with a remote debugger you have to pass the `-d` (or `--debug`) flag to the populate command:

    ccm populate -d -n 3

That will populate 3 nodes on IP 127.0.0.[1, 2, 3] setting up the remote debugging on ports 2100, 2200 and 2300.
The main thread will not be suspended so you don't have to connect with a remote debugger to start a node.

Alternatively you can also specify a remote port with the `-r` (or `--remote-debug-port`) flag while adding a node

    ccm add node4 -r 5005 -i 127.0.0.4 -j 7400 -b

Where things are stored
-----------------------

By default, ccm stores all the node data and configuration files under `~/.ccm/cluster_name/`.
This can be overridden using the `--config-dir` option with each command.

DataStax Enterprise
-------------------

CCM 2.0 supports creating and interacting with DSE clusters. The --dse
option must be used with the `ccm create` command. See the `ccm create -h`
help for assistance.

CCM Lib
-------

The ccm facilities are available programmatically through ccmlib. This could
be used to implement automated tests again Cassandra. A simple example of
how to use ccmlib follows:

    import ccmlib

    CLUSTER_PATH="."
    cluster = ccmlib.Cluster(CLUSTER_PATH, 'test', cassandra_version='2.0.5')
    cluster.populate(3).start()
    [node1, node2, node3] = cluster.nodelist()

    # do some tests on the cluster/nodes. To connect to a node through native protocol,
    # the host and port to a node is available through
    #   node.network_interfaces['binary]

    cluster.flush()
    node2.compact()

    # do some other tests

    # after the test, you can leave the cluster running, you can stop all nodes
    # using cluster.stop() but keep the data around (in CLUSTER_PATH/test), or
    # you can remove everything with cluster.remove()


--
Sylvain Lebresne <sylvain@datastax.com>
