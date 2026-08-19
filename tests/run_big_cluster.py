"""Start a large podman cluster with configurable topology and concurrency.

Usage:
  python tests/run_big_cluster.py [--concurrency N] [--nodes DC:RACK:N ...]
"""

import argparse
import concurrent.futures
import logging
import os
import sys
import tempfile
import time
import traceback

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
LOG = logging.getLogger("run_big_cluster")

for name in ("ccmlib.scylla_podman_cluster", "ccmlib.node", "ccmlib.cluster"):
    logging.getLogger(name).setLevel(logging.WARNING)

_DEFAULT_TOPOLOGY = {"dc1": {"rack1": 5, "rack2": 5, "rack3": 5},
                     "dc2": {"rack1": 5, "rack2": 5, "rack3": 5},
                     "dc3": {"rack1": 5, "rack2": 5, "rack3": 5}}


def _parse_topology_arg(node_specs):
    """Parse repeated ``--nodes DC:RACK:N`` values into a topology dict."""
    topology = {}
    for spec in node_specs:
        parts = spec.split(":")
        if len(parts) != 3:
            raise argparse.ArgumentTypeError(
                f"Invalid --nodes value {spec!r}; expected DC:RACK:N"
            )
        dc, rack, count_str = parts
        try:
            count = int(count_str)
        except ValueError:
            raise argparse.ArgumentTypeError(
                f"Invalid --nodes value {spec!r}: node count must be an integer"
            )
        if count <= 0:
            raise argparse.ArgumentTypeError(
                f"Invalid --nodes value {spec!r}: node count must be positive"
            )
        topology.setdefault(dc, {})[rack] = count
    return topology


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Start a large podman cluster with configurable topology and concurrency.",
    )
    parser.add_argument(
        "--concurrency", type=int, default=None,
        help="Number of non-seed nodes to start concurrently "
             "(default: all at once, after the seed node is up)",
    )
    parser.add_argument(
        "--nodes", action="append", default=None, metavar="DC:RACK:N",
        help="Add N nodes to DC/RACK; repeatable. "
             "Default: 3 DCs x 3 racks x 5 nodes each.",
    )
    parser.add_argument(
        "--inter-dc-delay-ms", type=int, default=0,
        help="Simulated inter-DC latency in ms (default: 0 = no shaping)",
    )
    parser.add_argument(
        "--inter-rack-delay-ms", type=int, default=0,
        help="Simulated inter-rack latency in ms (default: 0 = no shaping)",
    )
    parser.add_argument(
        "--smp", type=int, default=None,
        help="Shards per node (default: node default, 1)",
    )
    parser.add_argument(
        "--dir", default=None, metavar="DIR",
        help="Directory to hold the cluster data (default: tempfile under /tmp, tmpfs)",
    )
    return parser.parse_args(argv)


def main():
    import shutil

    args = parse_args()

    if not shutil.which("podman"):
        print("podman not found", file=sys.stderr)
        return 1

    image = os.environ.get(
        "SCYLLA_PODMAN_IMAGE",
        os.environ.get("SCYLLA_DOCKER_IMAGE", "docker.io/scylladb/scylla-nightly:latest"),
    )

    topology = _parse_topology_arg(args.nodes) if args.nodes else _DEFAULT_TOPOLOGY

    node_count = sum(sum(racks.values()) for racks in topology.values())
    concurrency = args.concurrency

    print(f"Image: {image}")
    print(f"Topology: {node_count} nodes across {len(topology)} DCs")
    print(f"Concurrency: {concurrency or 'all at once (after seed)'}")
    print()

    from ccmlib.scylla_podman_cluster import ScyllaPodmanCluster

    test_dir = tempfile.mkdtemp(prefix="ccm-bigcluster-", dir=args.dir)
    cluster = None
    failures = 0
    try:
        cluster = ScyllaPodmanCluster(
            str(test_dir),
            name="bigcluster",
            podman_image=image,
            inter_dc_delay_ms=args.inter_dc_delay_ms,
            inter_rack_delay_ms=args.inter_rack_delay_ms,
        )

        cluster.populate(topology)

        total_nodes = len(cluster.nodelist())
        print(f"Populated {total_nodes} nodes")

        cluster.set_configuration_options(values={
            "read_request_timeout_in_ms": 10000,
            "range_request_timeout_in_ms": 10000,
            "write_request_timeout_in_ms": 10000,
            "truncate_request_timeout_in_ms": 10000,
            "request_timeout_in_ms": 10000,
        })

        nodes = cluster.nodelist()
        if args.smp:
            for node in nodes:
                node.set_smp(args.smp)
        t0 = time.time()

        def start_node(node):
            LOG.info("Starting %s ...", node.name)
            node.start(wait_for_binary_proto=True, wait_other_notice=False)
            LOG.info("Started %s", node.name)
            return node

        # Start the seed alone, then the rest concurrently (all at once by default).
        seed, rest = nodes[0], nodes[1:]
        start_node(seed)
        LOG.info("[1/%d] %s (seed) UP in %.1f s", total_nodes, seed.name, time.time() - t0)

        max_workers = concurrency or max(len(rest), 1)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(start_node, n): n for n in rest}
            done = 1
            for future in concurrent.futures.as_completed(futures):
                n = futures[future]
                done += 1
                try:
                    future.result()
                    elapsed = time.time() - t0
                    LOG.info(
                        "[%d/%d] %s UP in %.1f s",
                        done, total_nodes, n.name, elapsed,
                    )
                except Exception as exc:
                    failures += 1
                    LOG.error("%s FAILED: %s", n.name, exc)
                    traceback.print_exc()

        # node.start() bypasses cluster.start_nodes(); apply tc/netem rules here
        t_shape = time.time()
        cluster.apply_network_shaping()
        LOG.info("tc/netem shaping took %.1f s", time.time() - t_shape)

        total = time.time() - t0
        print(f"\nTotal time: {total:.1f} s ({total/60:.1f} min)")

        running = sum(1 for n in nodes if n.is_running())
        print(f"Nodes UP: {running}/{total_nodes}")
    finally:
        if cluster is not None:
            print("\nCleaning up...")
            cluster.remove()
            print("Done.")

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
