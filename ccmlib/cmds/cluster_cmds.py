import os
import subprocess
import sys
import tempfile


from ccmlib import common, repository
from ccmlib.cluster import Cluster
from ccmlib.cluster_factory import ClusterFactory
from ccmlib.cmds.command import Cmd, PlainHelpFormatter
from ccmlib.common import ArgumentError
from ccmlib.dse_cluster import DseCluster
from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_docker_cluster import ScyllaDockerCluster, ScyllaDockerNode
from ccmlib.scylla_node import ScyllaNode
from ccmlib.dse_node import DseNode
from ccmlib.node import Node, NodeError
from ccmlib.utils.ssl_utils import generate_ssl_stores
from ccmlib.utils.sni_proxy import get_cluster_info, start_sni_proxy, configure_sni_proxy, reload_sni_proxy, refresh_certs, create_cloud_config

os.environ['SCYLLA_CCM_STANDALONE'] = '1'


def cluster_cmds():
    return [
        "create",
        "add",
        "populate",
        "list",
        "switch",
        "status",
        "remove",
        "clear",
        "liveset",
        "start",
        "stop",
        "flush",
        "compact",
        "stress",
        "updateconf",
        "updatedseconf",
        "updatelog4j",
        "cli",
        "setdir",
        "bulkload",
        "setlog",
        "scrub",
        "verify",
        "invalidatecache",
        "checklogerror",
        "showlastlog",
        "jconsole",
        "sctool"
    ]


def parse_populate_count(v):
    if v is None:
        return None
    tmp = v.split(':')
    if len(tmp) == 1:
        return int(tmp[0])
    else:
        return [int(t) for t in tmp]

class ClusterCreateCmd(Cmd):

    def description(self):
        return "Create a new cluster"

    def get_parser(self):
        usage = "usage: ccm create [options] cluster_name"
        parser = self._get_default_parser(usage, self.description(), formatter=PlainHelpFormatter())
        parser.add_option('--no-switch', action="store_true", dest="no_switch",
                          help="Don't switch to the newly created cluster", default=False)
        parser.add_option('-p', '--partitioner', type="string", dest="partitioner",
                          help="Set the cluster partitioner class")
        parser.add_option('-v', "--version", type="string", dest="version",
                          help="Download and use provided cassandra or dse version. If version is of the form 'git:<branch name>', then the specified cassandra branch will be downloaded from the git repo and compiled. (takes precedence over --install-dir)", default=None)
        parser.add_option('--docker-image', type="string", dest="docker_image",
                          help="The dockerhub image to deploy as Scylla nodes")
        parser.add_option('-o', "--opsc", type="string", dest="opscenter",
                          help="Download and use provided opscenter version to install with DSE. Will have no effect on cassandra installs)", default=None)
        parser.add_option("--dse", action="store_true", dest="dse",
                          help="Use with -v to indicate that the version being loaded is DSE")
        parser.add_option("--dse-username", type="string", dest="dse_username",
                          help="The username to use to download DSE with", default=None)
        parser.add_option("--dse-password", type="string", dest="dse_password",
                          help="The password to use to download DSE with", default=None)
        parser.add_option("--install-dir", type="string", dest="install_dir",
                          help="Path to the cassandra or dse directory to use [default %default]", default="./")
        parser.add_option('-n', '--nodes', type="string", dest="nodes",
                          help="Populate the new cluster with that number of nodes (a single int or a colon-separate list of ints for multi-dc setups)")
        parser.add_option('-i', '--ipprefix', type="string", dest="ipprefix",
                          help="Ipprefix to use to create the ip of a node while populating")
        parser.add_option('-I', '--ip-format', type="string", dest="ipformat",
                          help="Format to use when creating the ip of a node (supports enumerating ipv6-type addresses like fe80::%d%lo0)")
        parser.add_option('-s', "--start", action="store_true", dest="start_nodes",
                          help="Start nodes added through -s", default=False)
        parser.add_option('-d', "--debug", action="store_true", dest="debug",
                          help="If -s is used, show the standard output when starting the nodes", default=False)
        parser.add_option('-b', "--binary-protocol", action="store_true", dest="binary_protocol",
                          help="Enable the binary protocol (starting from C* 1.2.5 the binary protocol is started by default and this option is a no-op)", default=False)
        parser.add_option('-D', "--debug-log", action="store_true", dest="debug_log",
                          help="With -n, sets debug logging on the new nodes", default=False)
        parser.add_option('-T', "--trace-log", action="store_true", dest="trace_log",
                          help="With -n, sets trace logging on the new nodes", default=False)
        parser.add_option("--vnodes", action="store_true", dest="vnodes",
                          help="Use vnodes (256 tokens). Must be paired with -n.", default=False)
        parser.add_option('--jvm_arg', action="append", dest="jvm_args",
                          help="Specify a JVM argument", default=[])
        parser.add_option('--profile', action="store_true", dest="profile",
                          help="Start the nodes with yourkit agent (only valid with -s)", default=False)
        parser.add_option('--profile-opts', type="string", action="store", dest="profile_options",
                          help="Yourkit options when profiling", default=None)
        parser.add_option('--ssl', type="string", dest="ssl_path",
                          help="Path to keystore.jks and cassandra.crt files (and truststore.jks [not required])", default=None)
        parser.add_option('--require_client_auth', action="store_true", dest="require_client_auth",
                          help="Enable client authentication (only vaid with --ssl)", default=False)
        parser.add_option('--node-ssl', type="string", dest="node_ssl_path",
                          help="Path to keystore.jks and truststore.jks for internode encryption", default=None)
        parser.add_option("--scylla", action="store_true", dest="scylla",
                          help="Must specify --install-dir holding Scylla")
        parser.add_option("--scylla-manager", type="string", dest="scyllamanager",
                          help="Must specify root directory for scylla management")
        parser.add_option("--scylla-manager-package", type="string", dest="scylla_manager_package",
                          help="A remote path, where the scylla-manager RPMs are available")
        parser.add_option("--snitch", type="string", dest="snitch",
                          help="Supports 'org.apache.cassandra.locator.PropertyFileSnitch','org.apache.cassandra.locator.GossipingPropertyFileSnitch' used only in multidc clusters")
        parser.add_option("--id", type="int", dest="id",
                          help="Allows running multiple clusters in paralell (up to 100) each must have a unique id value 0-99, this will set the ipprefix value if one was not set to 127.0.<id>.")

        parser.add_option('--scylla-core-package-uri', type="string", dest="scylla_core_package_uri",
                          help="The path scylla relocatable package", default=None)

        parser.add_option('--scylla-tools-java-package-uri', type="string", dest="scylla_tools_java_package_uri",
                          help="The path scylla java tools relocatable package", default=None)

        parser.add_option('--scylla-jmx-package-uri', type="string", dest="scylla_jmx_package_uri",
                          help="The path scylla jmx relocatable package", default=None)

        parser.add_option('--scylla-unified-package-uri', type="string", dest="scylla_unified_package_uri",
                          help="The path scylla relocatable unified package", default=None)

        parser.epilog = """
        
        Examples of using relocatable packages:
        
        # create cluster from version uploaded to s3 (of the daily/nightly as example)
        ccm create scylla-reloc-1 -n 1 --scylla --version unstable/master:380
        
        # create cluster with own versions of each package
        ccm create scylla-reloc-1 -n 1 --scylla --version temp \\
            --scylla-core-package-uri=../scylla/build/release/scylla-package.tar.gz \\ 
            --scylla-tools-java-package-uri=../scylla-tools-java/temp.tar.gz \\
            --scylla-jmx-package-uri=../scylla-jmx/temp.tar.gz

        # create cluster with overwriting only one package
        ccm create scylla-reloc-1 -n 1 --scylla --version unstable/master:380 \\
            --scylla-core-package-uri=../scylla/build/dev/scylla-package.tar.gz

        """


        return parser

    def validate(self, parser, options, args):
        if options.scylla and not options.install_dir:
            parser.error("must specify install_dir using scylla")
        Cmd.validate(self, parser, options, args, cluster_name=True)
        if options.ipprefix and options.ipformat:
            parser.print_help()
            parser.error(f"{parser.get_option('-i')} and {parser.get_option('-I')} may not be used together")
        self.nodes = parse_populate_count(options.nodes)
        if self.options.vnodes and self.nodes is None:
            print("Can't set --vnodes if not populating cluster in this command.")
            parser.print_help()
            sys.exit(1)
        if self.options.snitch and \
            (not isinstance(self.nodes, list) or
             not (self.options.snitch == 'org.apache.cassandra.locator.PropertyFileSnitch' or
                  self.options.snitch == 'org.apache.cassandra.locator.GossipingPropertyFileSnitch')):
            parser.print_help()
            sys.exit(1)

        if not options.version and not options.docker_image:
            try:
                common.validate_install_dir(options.install_dir)
            except ArgumentError:
                parser.print_help()
                parser.error(f"{options.install_dir} is not a valid cassandra directory. You must define a cassandra dir or version.")

            common.assert_jdk_valid_for_cassandra_version(common.get_version_from_build(options.install_dir))
        if common.is_win() and os.path.exists(r'c:\windows\system32\java.exe'):
            print(r"""WARN: c:\windows\system32\java.exe exists.
                This may cause registry issues, and jre7 to be used, despite jdk8 being installed.
                """)

        if options.scylla_core_package_uri:
            os.environ['SCYLLA_CORE_PACKAGE'] = options.scylla_core_package_uri
        if options.scylla_unified_package_uri:
            os.environ['SCYLLA_UNIFIED_PACKAGE'] = options.scylla_unified_package_uri
        if options.scylla_tools_java_package_uri:
            os.environ['SCYLLA_TOOLS_JAVA_PACKAGE'] = options.scylla_tools_java_package_uri
            # TODO: remove this export eventually, it's for backward
            # compatibility with the previous name
            os.environ['SCYLLA_JAVA_TOOLS_PACKAGE'] = options.scylla_tools_java_package_uri
        if options.scylla_jmx_package_uri:
            os.environ['SCYLLA_JMX_PACKAGE'] = options.scylla_jmx_package_uri

    def run(self):
        try:
            if self.options.scylla:
                if self.options.docker_image:
                    cluster = ScyllaDockerCluster(self.path, self.name, docker_image=self.options.docker_image)
                else:
                    if self.options.scylla_manager_package:
                        from ccmlib.scylla_repository import setup_scylla_manager
                        manager_install_dir = setup_scylla_manager(self.options.scylla_manager_package)
                    else:
                        manager_install_dir = self.options.scyllamanager
                    cluster = ScyllaCluster(self.path, self.name, install_dir=self.options.install_dir,
                                            version=self.options.version, verbose=True, manager=manager_install_dir)
            elif self.options.dse or (not self.options.version and common.isDse(self.options.install_dir)):
                cluster = DseCluster(self.path, self.name, install_dir=self.options.install_dir, version=self.options.version, dse_username=self.options.dse_username, dse_password=self.options.dse_password, opscenter=self.options.opscenter, verbose=True)
            else:
                cluster = Cluster(self.path, self.name, install_dir=self.options.install_dir, version=self.options.version, verbose=True)
        except OSError as e:
            import traceback
            print(f'Cannot create cluster: {str(e)}\n{traceback.format_exc()}', file=sys.stderr)
            sys.exit(1)

        if self.options.partitioner:
            cluster.set_partitioner(self.options.partitioner)

        if self.options.snitch:
            cluster.set_snitch(self.options.snitch)

        if self.options.id:
            cluster.set_id(self.options.id)
            if not self.options.ipprefix:
                self.options.ipprefix = "127.0.%d." % self.options.id

        if cluster.cassandra_version() >= "1.2.5":
            self.options.binary_protocol = True
        if self.options.binary_protocol:
            cluster.set_configuration_options({'start_native_transport': True})

        if cluster.cassandra_version() >= "1.2" and self.options.vnodes:
            cluster.set_configuration_options({'num_tokens': 256})

        if not self.options.no_switch:
            common.switch_cluster(self.path, self.name)
            print(f'Current cluster is now: {self.name}')

        if not (self.options.ipprefix or self.options.ipformat):
            self.options.ipformat = '127.0.0.%d'

        if self.options.ssl_path:
            cluster.enable_ssl(self.options.ssl_path, self.options.require_client_auth)

        if self.options.node_ssl_path:
            cluster.enable_internode_ssl(self.options.node_ssl_path)

        if self.nodes is not None:
            try:
                if self.options.debug_log:
                    cluster.set_log_level("DEBUG")
                if self.options.trace_log:
                    cluster.set_log_level("TRACE")
                cluster.populate(self.nodes, self.options.debug, use_vnodes=self.options.vnodes, ipprefix=self.options.ipprefix, ipformat=self.options.ipformat)
                if self.options.start_nodes:
                    profile_options = None
                    if self.options.profile:
                        profile_options = {}
                        if self.options.profile_options:
                            profile_options['options'] = self.options.profile_options
                    if cluster.start(verbose=self.options.debug_log, wait_for_binary_proto=self.options.binary_protocol, jvm_args=self.options.jvm_args, profile_options=profile_options) is None:
                        details = ""
                        if not self.options.debug_log:
                            details = " (you can use --debug-log for more information)"
                        print(f"Error starting nodes, see above for details{details}", file=sys.stderr)
            except common.ArgumentError as e:
                print(str(e), file=sys.stderr)
                sys.exit(1)


class ClusterAddCmd(Cmd):

    def description(self):
        return "Add a new node to the current cluster"

    def get_parser(self):
        usage = "usage: ccm add [options] node_name"
        parser = self._get_default_parser(usage, self.description())
        parser.add_option('-b', '--auto-bootstrap', action="store_true", dest="bootstrap",
                          help="Set auto bootstrap for the node", default=False)
        parser.add_option('-s', '--seeds', action="store_true", dest="is_seed",
                          help="Configure this node as a seed", default=False)
        parser.add_option('-i', '--itf', type="string", dest="itfs",
                          help="Set host and port for thrift, the binary protocol and storage (format: host[:port])")
        parser.add_option('-t', '--thrift-itf', type="string", dest="thrift_itf",
                          help="Set the thrift host and port for the node (format: host[:port])")
        parser.add_option('-l', '--storage-itf', type="string", dest="storage_itf",
                          help="Set the storage (cassandra internal) host and port for the node (format: host[:port])")
        parser.add_option('--binary-itf', type="string", dest="binary_itf",
                          help="Set the binary protocol host and port for the node (format: host[:port]).")
        parser.add_option('-j', '--jmx-port', type="string", dest="jmx_port",
                          help="JMX port for the node")
        parser.add_option('-r', '--remote-debug-port', type="string", dest="remote_debug_port",
                          help="Remote Debugging Port for the node", default="2000")
        parser.add_option('-n', '--token', type="string", dest="initial_token",
                          help="Initial token for the node", default=None)
        parser.add_option('-d', '--data-center', type="string", dest="data_center",
                          help="Datacenter name this node is part of", default=None)
        parser.add_option('--dse', action="store_true", dest="dse_node",
                          help="Add node to DSE Cluster", default=False)
        parser.add_option('--scylla', action="store_true", dest="scylla_node",
                          help="Add node to Scylla Cluster", default=False)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True, load_node=False)

        if options.itfs is None and (options.thrift_itf is None or options.storage_itf is None or options.binary_itf is None):
            options.itfs = self.cluster.get_node_ip(len(self.cluster.nodelist())+1)

        if options.thrift_itf is None:
            options.thrift_itf = options.itfs
        if options.storage_itf is None:
            options.storage_itf = options.itfs
        if options.binary_itf is None:
            options.binary_itf = options.itfs

        self.thrift = common.parse_interface(options.thrift_itf, 9160)
        self.storage = common.parse_interface(options.storage_itf, 7000)
        self.binary = common.parse_interface(options.binary_itf, 9042)

        if self.binary[0] != self.thrift[0]:
            print('Cannot set a binary address different from the thrift one', file=sys.stderr)
            sys.exit(1)

        used_binary_ips = [node.network_interfaces['binary'][0] for node in self.cluster.nodelist()]
        used_thrift_ips = [node.network_interfaces['thrift'][0] for node in self.cluster.nodelist()]
        used_storage_ips = [node.network_interfaces['storage'][0] for node in self.cluster.nodelist()]

        if self.binary[0] in used_binary_ips or self.thrift[0] in used_thrift_ips or self.storage[0] in used_storage_ips:
            print("One of the ips is already in use choose another.", file=sys.stderr)
            parser.print_help()
            sys.exit(1)

        if options.jmx_port is None:
            options.jmx_port = self.cluster.get_node_jmx_port(len(self.cluster.nodelist())+1)

        used_jmx_ports = [node.jmx_port for node in self.cluster.nodelist()]
        if options.jmx_port in used_jmx_ports:
            print("This JMX port is already in use. Choose another.", file=sys.stderr)
            parser.print_help()
            sys.exit(1)

        self.jmx_port = options.jmx_port
        self.remote_debug_port = options.remote_debug_port
        self.initial_token = options.initial_token

    def run(self):
        try:
            if self.options.scylla_node:
                if self.cluster.is_docker():
                    node_class = ScyllaDockerNode
                else:
                    node_class = ScyllaNode
                node = node_class(self.name, self.cluster, self.options.bootstrap, self.thrift, self.storage, self.jmx_port, self.remote_debug_port, self.initial_token, binary_interface=self.binary)
            elif self.options.dse_node:
                node = DseNode(self.name, self.cluster, self.options.bootstrap, self.thrift, self.storage, self.jmx_port, self.remote_debug_port, self.initial_token, binary_interface=self.binary)
            else:
                node = Node(self.name, self.cluster, self.options.bootstrap, self.thrift, self.storage, self.jmx_port, self.remote_debug_port, self.initial_token, binary_interface=self.binary)
            self.cluster.add(node, self.options.is_seed, self.options.data_center)
        except common.ArgumentError as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)


class ClusterPopulateCmd(Cmd):

    def description(self):
        return "Add a group of new nodes with default options"

    def get_parser(self):
        usage = "usage: ccm populate -n <node count> {-d}"
        parser = self._get_default_parser(usage, self.description())
        parser.add_option('-n', '--nodes', type="string", dest="nodes",
                          help="Number of nodes to populate with (a single int or a colon-separate list of ints for multi-dc setups)")
        parser.add_option('-d', '--debug', action="store_true", dest="debug",
                          help="Enable remote debugging options", default=False)
        parser.add_option('--vnodes', action="store_true", dest="vnodes",
                          help="Populate using vnodes", default=False)
        parser.add_option('-i', '--ipprefix', type="string", dest="ipprefix",
                          help="Ipprefix to use to create the ip of a node")
        parser.add_option('-I', '--ip-format', type="string", dest="ipformat",
                          help="Format to use when creating the ip of a node (supports enumerating ipv6-type addresses like fe80::%d%lo0)")
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        if options.ipprefix and options.ipformat:
            parser.print_help()
            parser.error(f"{parser.get_option('-i')} and {parser.get_option('-I')} may not be used together")

        self.nodes = parse_populate_count(options.nodes)
        if self.nodes is None:
            parser.print_help()
            parser.error("Not a valid number of nodes. Did you use -n?")
            sys.exit(1)

    def run(self):
        try:
            if self.cluster.cassandra_version() >= "1.2" and self.options.vnodes:
                self.cluster.set_configuration_options({'num_tokens': 256})

            if not (self.options.ipprefix or self.options.ipformat):
                self.options.ipformat = '127.0.0.%d'

            self.cluster.populate(self.nodes, self.options.debug, use_vnodes=self.options.vnodes, ipprefix=self.options.ipprefix, ipformat=self.options.ipformat)
        except common.ArgumentError as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)


class ClusterListCmd(Cmd):

    def description(self):
        return "List existing clusters"

    def get_parser(self):
        usage = "usage: ccm list [options]"
        return self._get_default_parser(usage, self.description())

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args)

    def run(self):
        try:
            current = common.current_cluster_name(self.path)
        except Exception:
            current = ''

        for dir in os.listdir(self.path):
            if os.path.exists(os.path.join(self.path, dir, 'cluster.conf')):
                print(f" {'*' if current == dir else ' '}{dir}")


class ClusterSwitchCmd(Cmd):

    def description(self):
        return "Switch of current (active) cluster"

    def get_parser(self):
        usage = "usage: ccm switch [options] cluster_name"
        return self._get_default_parser(usage, self.description())

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, cluster_name=True)
        if not os.path.exists(os.path.join(self.path, self.name, 'cluster.conf')):
            print(f"{self.name} does not appear to be a valid cluster (use ccm list to view valid clusters)", file=sys.stderr)
            sys.exit(1)

    def run(self):
        common.switch_cluster(self.path, self.name)


class ClusterStatusCmd(Cmd):

    def description(self):
        return "Display status on the current cluster"

    def get_parser(self):
        usage = "usage: ccm status [options]"
        parser = self._get_default_parser(usage, self.description())
        parser.add_option('-v', '--verbose', action="store_true", dest="verbose",
                          help="Print full information on all nodes", default=False)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        self.cluster.show(self.options.verbose)


class ClusterRemoveCmd(Cmd):

    def description(self):
        return "Remove the current or specified cluster (delete all data)"

    def get_parser(self):
        usage = "usage: ccm remove [options] [cluster_name]"
        parser = self._get_default_parser(usage, self.description())
        return parser

    def validate(self, parser, options, args):
        self.other_cluster = None
        if len(args) > 0:
            # Setup to remove the specified cluster:
            Cmd.validate(self, parser, options, args)
            self.other_cluster = args[0]
            if not os.path.exists(os.path.join(
                    self.path, self.other_cluster, 'cluster.conf')):
                print("%s does not appear to be a valid cluster"
                       " (use ccm list to view valid clusters)"
                       % self.other_cluster, file=sys.stderr)
                sys.exit(1)
        else:
            # Setup to remove the current cluster:
            Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        if self.other_cluster:
            # Remove the specified cluster:
            cluster = ClusterFactory.load(self.path, self.other_cluster)
            cluster.remove()
            # Remove CURRENT flag if the specified cluster is the current cluster:
            if self.other_cluster == common.current_cluster_name(self.path):
                os.remove(os.path.join(self.path, 'CURRENT'))
        else:
            # Remove the current cluster:
            self.cluster.remove()
            os.remove(os.path.join(self.path, 'CURRENT'))


class ClusterClearCmd(Cmd):

    def description(self):
        return "Clear the current cluster data (and stop all nodes)"

    def get_parser(self):
        usage = "usage: ccm clear [options]"
        parser = self._get_default_parser(usage, self.description())
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        self.cluster.clear()


class ClusterLivesetCmd(Cmd):

    def description(self):
        return "Print a comma-separated list of addresses of running nodes (helpful in scripts)"

    def get_parser(self):
        usage = "usage: ccm liveset [options]"
        parser = self._get_default_parser(usage, self.description())
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        storage_interfaces = [node.network_interfaces['storage'][0] for node in list(self.cluster.nodes.values()) if node.is_live()]
        print(",".join(storage_interfaces))


class ClusterSetdirCmd(Cmd):

    def description(self):
        return "Set the install directory (cassandra or dse) to use"

    def get_parser(self):
        usage = "usage: ccm setdir [options]"
        parser = self._get_default_parser(usage, self.description())
        parser.add_option('-v', "--version", type="string", dest="version",
                          help="Download and use provided cassandra or dse version. If version is of the form 'git:<branch name>', then the specified cassandra branch will be downloaded from the git repo and compiled. (takes precedence over --install-dir)", default=None)
        parser.add_option("--install-dir", type="string", dest="install_dir",
                          help="Path to the cassandra or dse directory to use [default %default]", default="./")
        parser.add_option('-n', '--node', type="string", dest="node",
                          help="Set directory only for the specified node")
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        try:
            target = self.cluster
            if self.options.node:
                target = self.cluster.nodes.get(self.options.node)
                if not target:
                    print(f"Node not found: {self.options.node}")
                    return
            target.set_install_dir(install_dir=self.options.install_dir, version=self.options.version, verbose=True)
        except common.ArgumentError as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)


class ClusterClearrepoCmd(Cmd):

    def description(self):
        return "Cleanup downloaded cassandra sources"

    def get_parser(self):
        usage = "usage: ccm clearrepo [options]"
        parser = self._get_default_parser(usage, self.description())
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args)

    def run(self):
        repository.clean_all()


class ClusterStartCmd(Cmd):

    def description(self):
        return "Start all the non started nodes of the current cluster"

    def get_parser(self):
        usage = "usage: ccm cluster start [options]"
        parser = self._get_default_parser(usage, self.description())
        parser.add_option('-v', '--verbose', action="store_true", dest="verbose",
                          help="Print standard output of cassandra process", default=False)
        parser.add_option('--no-wait', action="store_true", dest="no_wait",
                          help="Do not wait for cassandra node to be ready", default=False)
        parser.add_option('--wait-other-notice', action="store_true", dest="wait_other_notice",
                          help="Wait until all other live nodes of the cluster have marked this node UP", default=False)
        parser.add_option('--wait-for-binary-proto', action="store_true", dest="wait_for_binary_proto",
                          help="Wait for the binary protocol to start", default=False)
        parser.add_option('--jvm_arg', action="append", dest="jvm_args",
                          help="Specify a JVM argument", default=[])
        parser.add_option('--profile', action="store_true", dest="profile",
                          help="Start the nodes with yourkit agent (only valid with -s)", default=False)
        parser.add_option('--profile-opts', type="string", action="store", dest="profile_options",
                          help="Yourkit options when profiling", default=None)
        parser.add_option('--quiet-windows', action="store_true", dest="quiet_start", help="Pass -q on Windows 2.2.4+ and 3.0+ startup. Ignored on linux.", default=False)

        parser.add_option('--sni-proxy', action="store_true", dest="sni_proxy", help="Start sniproxy infront of the cluster", default=False)
        parser.add_option('--sni-port', action="store", type="int", dest="sni_port",
                          help="the port to use for the sniproxy", default=443)

        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        try:
            profile_options = None
            if self.options.profile:
                profile_options = {}
                if self.options.profile_options:
                    profile_options['options'] = self.options.profile_options

            if len(self.cluster.nodes) == 0:
                print("No node in this cluster yet. Use the populate command before starting.")
                sys.exit(1)

            ssl_port = self.cluster._config_options.get('native_transport_port_ssl', 9142)
            encryption_options =  self.cluster._config_options.get('client_encryption_options', {})

            if not getattr(self.cluster, 'sni_generate_ssl_automatic', False):
                if 'keyfile' not in encryption_options and \
                    'certificate' not in encryption_options and \
                    'truststore' not in encryption_options:
                    self.cluster.sni_generate_ssl_automatic = True

            if self.options.sni_proxy and getattr(self.cluster, 'sni_generate_ssl_automatic', False):
                generate_ssl_stores(self.cluster.get_path())

                self.cluster.set_configuration_options(dict(
                        client_encryption_options=
                            dict(require_client_auth=True,
                                 truststore=os.path.join(self.cluster.get_path(), 'ccm_node.cer'),
                                 certificate=os.path.join(self.cluster.get_path(), 'ccm_node.pem'),
                                 keyfile=os.path.join(self.cluster.get_path(), 'ccm_node.key'),
                                 enabled=True),
                        native_transport_port_ssl=ssl_port))

                self.cluster._update_config()

            if self.options.sni_proxy:
                self.options.wait_for_binary_proto = True

            if self.cluster.start(no_wait=self.options.no_wait,
                                  wait_other_notice=self.options.wait_other_notice,
                                  wait_for_binary_proto=self.options.wait_for_binary_proto,
                                  verbose=self.options.verbose,
                                  jvm_args=self.options.jvm_args,
                                  profile_options=profile_options,
                                  quiet_start=self.options.quiet_start) is None:
                details = ""
                if not self.options.verbose:
                    details = " (you can use --verbose for more information)"
                print(f"Error starting nodes, see above for details{details}", file=sys.stderr)
                sys.exit(1)
            if self.options.sni_proxy:
                nodes_info = get_cluster_info(self.cluster, port=ssl_port)
                if getattr(self.cluster, 'sni_generate_ssl_automatic', False):
                    refresh_certs(self.cluster, nodes_info)

                sni_proxy_docker_id = getattr(self.cluster, 'sni_proxy_docker_id', None)
                if sni_proxy_docker_id:
                    listen_port = getattr(self.cluster, 'sni_proxy_listen_port', None)
                    configure_sni_proxy(self.cluster.get_path(), nodes_info, listen_port=listen_port)
                    reload_sni_proxy(self.cluster.sni_proxy_docker_id)
                else:
                    docker_id, listen_address, listen_port = \
                        start_sni_proxy(self.cluster.get_path(), nodes_info=nodes_info, listen_port=self.options.sni_port)
                    create_cloud_config(self.cluster.get_path(), port=listen_port, address=listen_address, nodes_info=nodes_info)

                    print(f'sni_proxy listening on: {listen_address}:{listen_port}')
                    self.cluster.sni_proxy_docker_id = docker_id
                    self.cluster.sni_proxy_listen_port = listen_port
                    self.cluster._update_config()

        except NodeError as e:
            print(str(e), file=sys.stderr)
            if e.process is not None:
                if e.process.stderr is not None:
                    print("Standard error output is:", file=sys.stderr)
                    for line in e.process.stderr:
                        print(line.rstrip('\n'), file=sys.stderr)
            sys.exit(1)


class ClusterStopCmd(Cmd):

    def description(self):
        return "Stop all the nodes of the cluster"

    def get_parser(self):
        usage = "usage: ccm cluster stop [options] name"
        parser = self._get_default_parser(usage, self.description())
        parser.add_option('-v', '--verbose', action="store_true", dest="verbose",
                          help="Print nodes that were not running", default=False)
        parser.add_option('--no-wait', action="store_true", dest="no_wait",
                          help="Do not wait for the node to be stopped", default=False)
        parser.add_option('-g', '--gently', action="store_true", dest="gently",
                          help="Shut down gently (default)", default=True)
        parser.add_option('--not-gently', action="store_false", dest="gently",
                          help="Shut down immediately (kill -9)", default=True)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        try:
            not_running = self.cluster.stop(not self.options.no_wait, gently=self.options.gently)
            if self.options.verbose and len(not_running) > 0:
                sys.stdout.write("The following nodes were not running: ")
                for node in not_running:
                    sys.stdout.write(node.name + " ")
                print("")
        except NodeError as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)


class _ClusterNodetoolCmd(Cmd):

    def get_parser(self):
        parser = self._get_default_parser(self.usage, self.description())
        return parser

    def description(self):
        return self.descr_text

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        self.cluster.nodetool(self.nodetool_cmd)


class ClusterFlushCmd(_ClusterNodetoolCmd):
    usage = "usage: ccm cluster flush [options] name"
    nodetool_cmd = 'flush'
    descr_text = "Flush all (running) nodes of the cluster"


class ClusterCompactCmd(_ClusterNodetoolCmd):
    usage = "usage: ccm cluster compact [options] name"
    nodetool_cmd = 'compact'
    descr_text = "Compact all (running) node of the cluster"


class ClusterDrainCmd(_ClusterNodetoolCmd):
    usage = "usage: ccm cluster drain [options] name"
    nodetool_cmd = 'drain'
    descr_text = "Drain all (running) node of the cluster"


class ClusterStressCmd(Cmd):

    def description(self):
        return "Run stress using all live nodes"

    def get_parser(self):
        usage = "usage: ccm stress [options] [stress_options]"
        parser = self._get_default_parser(usage, self.description(), ignore_unknown_options=True)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        self.stress_options = args + parser.get_ignored()

    def run(self):
        try:
            self.cluster.stress(self.stress_options)
        except Exception as e:
            print(e, file=sys.stderr)


class ClusterUpdateconfCmd(Cmd):

    def description(self):
        return "Update the cassandra config files for all nodes"

    def get_parser(self):
        usage = "usage: ccm updateconf [options] [ new_setting | ...  ], where new_setting should be a string of the form 'compaction_throughput_mb_per_sec: 32'; nested options can be separated with a period like 'client_encryption_options.enabled: false'"
        parser = self._get_default_parser(usage, self.description())
        parser.add_option('--no-hh', '--no-hinted-handoff', action="store_false",
                          dest="hinted_handoff", default=True, help="Disable hinted handoff")
        parser.add_option('--batch-cl', '--batch-commit-log', action="store_true",
                          dest="cl_batch", default=False, help="Set commit log to batch mode")
        parser.add_option('--rt', '--rpc-timeout', action="store", type='int',
                          dest="rpc_timeout", help="Set rpc timeout")
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        try:
            self.setting = common.parse_settings(args)
        except common.ArgumentError as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)

    def run(self):
        self.setting['hinted_handoff_enabled'] = self.options.hinted_handoff

        if self.options.rpc_timeout is not None:
            if self.cluster.cassandra_version() < "1.2":
                self.setting['rpc_timeout_in_ms'] = self.options.rpc_timeout
            else:
                self.setting['read_request_timeout_in_ms'] = self.options.rpc_timeout
                self.setting['range_request_timeout_in_ms'] = self.options.rpc_timeout
                self.setting['write_request_timeout_in_ms'] = self.options.rpc_timeout
                self.setting['truncate_request_timeout_in_ms'] = self.options.rpc_timeout
                self.setting['request_timeout_in_ms'] = self.options.rpc_timeout

        self.cluster.set_configuration_options(values=self.setting, batch_commitlog=self.options.cl_batch)


class ClusterUpdatedseconfCmd(Cmd):

    def description(self):
        return "Update the dse config files for all nodes"

    def get_parser(self):
        usage = "usage: ccm updatedseconf [options] [ new_setting | ...  ], where new_setting should be a string of the form 'max_solr_concurrency_per_core: 2'; nested options can be separated with a period like 'cql_slow_log_options.enabled: true'"
        parser = self._get_default_parser(usage, self.description())
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        try:
            self.setting = common.parse_settings(args)
        except common.ArgumentError as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)

    def run(self):
        self.cluster.set_dse_configuration_options(values=self.setting)

#
# Class implements the functionality of updating log4j-server.properties
# on ALL nodes by copying the given config into
# ~/.ccm/name-of-cluster/nodeX/conf/log4j-server.properties
#


class ClusterUpdatelog4jCmd(Cmd):

    def description(self):
        return "Update the Cassandra log4j-server.properties configuration file on all nodes"

    def get_parser(self):
        usage = "usage: ccm updatelog4j -p <log4j config>"
        parser = self._get_default_parser(usage, self.description(), ignore_unknown_options=True)
        parser.add_option('-p', '--path', type="string", dest="log4jpath",
                          help="Path to new Cassandra log4j configuration file")
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        try:
            self.log4jpath = options.log4jpath
            if self.log4jpath is None:
                raise KeyError("[Errno] -p or --path <path of new log4j congiguration file> is not provided")
        except common.ArgumentError as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)
        except KeyError as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)

    def run(self):
        try:
            self.cluster.update_log4j(self.log4jpath)
        except common.ArgumentError as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)


class ClusterCliCmd(Cmd):

    def description(self):
        return "Launch cassandra cli connected to some live node (if any)"

    def get_parser(self):
        usage = "usage: ccm cli [options] [cli_options]"
        parser = self._get_default_parser(usage, self.description(), ignore_unknown_options=True)
        parser.add_option('-x', '--exec', type="string", dest="cmds", default=None,
                          help="Execute the specified commands and exit")
        parser.add_option('-v', '--verbose', action="store_true", dest="verbose",
                          help="With --exec, show cli output after completion", default=False)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        self.cli_options = parser.get_ignored() + args[1:]

    def run(self):
        self.cluster.run_cli(self.options.cmds, self.options.verbose, self.cli_options)


class ClusterBulkloadCmd(Cmd):

    def description(self):
        return "Bulkload files into the cluster by connecting to some live node (if any)"

    def get_parser(self):
        usage = "usage: ccm bulkload [options] [sstable_dir]"
        parser = self._get_default_parser(usage, self.description(), ignore_unknown_options=True)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        self.loader_options = parser.get_ignored() + args

    def run(self):
        self.cluster.bulkload(self.loader_options)


class ClusterScrubCmd(Cmd):

    def description(self):
        return "Scrub files"

    def get_parser(self):
        usage = "usage: ccm scrub [options] <keyspace> <cf>"
        parser = self._get_default_parser(usage, self.description(), ignore_unknown_options=True)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        self.scrub_options = parser.get_ignored() + args

    def run(self):
        self.cluster.scrub(self.scrub_options)


class ClusterVerifyCmd(Cmd):

    def description(self):
        return "Verify files"

    def get_parser(self):
        usage = "usage: ccm verify [options] <keyspace> <cf>"
        parser = self._get_default_parser(usage, self.description(), ignore_unknown_options=True)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        self.verify_options = parser.get_ignored() + args

    def run(self):
        self.cluster.verify(self.verify_options)


class ClusterSetlogCmd(Cmd):

    def description(self):
        return "Set log level (INFO, DEBUG, ...) with/without Java class for all node of the cluster - require a node restart"

    def get_parser(self):
        usage = "usage: ccm setlog [options] level"
        parser = self._get_default_parser(usage, self.description())
        parser.add_option('-c', '--class', type="string", dest="class_name", default=None,
                          help="Optional java class/package. Logging will be set for only this class/package if set")
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        if len(args) == 0:
            print('Missing log level', file=sys.stderr)
            parser.print_help()
            sys.exit(1)
        self.level = args[0]

    def run(self):
        try:
            self.cluster.set_log_level(self.level, self.options.class_name)
        except common.ArgumentError as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)


class ClusterInvalidatecacheCmd(Cmd):

    def description(self):
        return "Destroys ccm's local git cache."

    def get_parser(self):
        usage = "usage: ccm invalidatecache"
        parser = self._get_default_parser(usage, self.description())
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args)

    def run(self):
        try:
            common.invalidate_cache()
        except Exception as e:
            print(str(e), file=sys.stderr)
            print("Error while deleting cache. Please attempt manually.")
            sys.exit(1)


class ClusterChecklogerrorCmd(Cmd):

    def description(self):
        return "Check for errors in log file of each node."

    def get_parser(self):
        usage = "usage: ccm checklogerror"
        parser = self._get_default_parser(usage, self.description())
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        for node in self.cluster.nodelist():
            errors = node.grep_log_for_errors()
            for mylist in errors:
                for line in mylist:
                    print(line)


class ClusterShowlastlogCmd(Cmd):

    def description(self):
        return "Show the last.log for the most recent build through your $PAGER"

    def get_parser(self):
        usage = "usage: ccm showlastlog"
        return self._get_default_parser(usage, self.description())

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        log = repository.lastlogfilename()
        pager = os.environ.get('PAGER', common.platform_pager())
        os.execvp(pager, (pager, log))


class ClusterJconsoleCmd(Cmd):

    def description(self):
        return "Opens jconsole client and connects to all running nodes"

    def get_parser(self):
        usage = "usage: ccm jconsole"
        return self._get_default_parser(usage, self.description())

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        cmds = ["jconsole"] + [f"localhost:{node.jmx_port}" for node in list(self.cluster.nodes.values())]
        try:
            subprocess.call(cmds, stderr=sys.stderr)
        except OSError:
            print("Could not start jconsole. Please make sure jconsole can be found in your $PATH.")
            sys.exit(1)

class ClusterSctoolCmd(Cmd):
    usage = "usage: ccm node_name nodetool [options]"
    descr_text = "Run nodetool (connecting to node name)"


    def description(self):
        return "Run scylla sctool"

    def get_parser(self):
        usage = "usage: ccm sctool [options]"
        parser = self._get_default_parser(usage, self.description(), ignore_unknown_options=True)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        self.sctool_options = args +  parser.get_ignored()

    def run(self):
        stdout, stderr = self.cluster.sctool(self.sctool_options)
        print(stderr)
        print(stdout)
